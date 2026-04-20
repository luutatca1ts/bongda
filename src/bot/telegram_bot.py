"""Telegram bot for football analytics alerts and commands."""

import logging
import math
from pathlib import Path
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

# Diagnostic: force INFO-level logs to bot.log (via stderr redirect in start_bot.bat).
# Needed so [CORNER-DEBUG-*] and [RAW-CORNER*] logs in odds_api.py are visible.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    force=True,
)

from src.config import TELEGRAM_BOT_TOKEN
from src.db.models import get_session, Bookmaker, Prediction, Match, DailyReport
from src.pipeline import _match_teams, _match_event, _is_ev_suspicious
from src.bot.formatters import (
    format_bookmaker_list,
    format_stats,
    format_daily_report,
)


# Vietnamese labels → token "corner" để _is_ev_suspicious (pipeline) match được rule 3.
_CORNER_LABELS_VN = {"Phạt góc", "Góc Châu Á", "Góc hiệp 1", "Góc H1 Châu Á"}


def _filter_suspicious_picks(picks: list[dict], label: str = "picks") -> tuple[list[dict], int]:
    """Lọc EV ảo cho picks format /phantich (dùng key 'bk' + Vietnamese market).

    Return (filtered_picks, skipped_count). Mỗi pick bị skip sẽ được log warning.
    """
    kept: list[dict] = []
    skipped = 0
    for p in picks:
        mkt_vn = p.get("market", "") or ""
        # Token hóa corner cho rule 3 của _is_ev_suspicious
        mkt_for_check = "corner" if mkt_vn in _CORNER_LABELS_VN else mkt_vn
        vb = {
            "ev": p.get("ev", 0),
            "bookmaker": p.get("bk", "") or "",
            "market": mkt_for_check,
            "outcome": p.get("outcome", ""),
        }
        susp, reason = _is_ev_suspicious(vb)
        if susp:
            skipped += 1
            logger.warning(
                f"[{label}] FILTERED suspicious VB — "
                f"{p.get('home', '?')} vs {p.get('away', '?')} | "
                f"{mkt_vn}:{p.get('outcome', '?')} @ {p.get('odds', 0)} "
                f"(EV {p.get('ev', 0)*100:+.1f}%, bk={p.get('bk', 'N/A')}) — {reason}"
            )
            continue
        kept.append(p)
    return kept, skipped

logger = logging.getLogger(__name__)


def _is_home_team(spread_name: str, home_team: str) -> bool:
    """Check if a spread outcome name corresponds to the home team."""
    from src.pipeline import _normalize
    return _normalize(spread_name) == _normalize(home_team) or \
           any(w in _normalize(spread_name) for w in _normalize(home_team).split() if len(w) >= 4)


def _get_pair_probs(pair: dict, ah: dict, match_home_team: str) -> dict:
    """
    Get correct model probabilities for a spread pair.
    The pair's 'home_name' may be the match's away team (API order varies).
    The model's asian_handicap keys are always from MATCH home team perspective.
    Returns: {pair_home_prob, pair_away_prob, push_prob, model_key}
    """
    pair_home_is_match_home = _is_home_team(pair["home_name"], match_home_team)

    if pair_home_is_match_home:
        # pair home = match home → use pair's home_point as model key
        model_key = f"{pair['home_point']:+g}" if pair['home_point'] != 0 else "0"
        ah_line = ah.get(model_key, {})
        return {
            "pair_home_prob": ah_line.get("home", 0),
            "pair_away_prob": ah_line.get("away", 0),
            "push": ah_line.get("push", 0),
            "model_key": model_key,
        }
    else:
        # pair home = match AWAY → use pair's away_point as model key (that's match home's handicap)
        model_key = f"{pair['away_point']:+g}" if pair['away_point'] != 0 else "0"
        ah_line = ah.get(model_key, {})
        return {
            "pair_home_prob": ah_line.get("away", 0),  # pair home is match away
            "pair_away_prob": ah_line.get("home", 0),  # pair away is match home
            "push": ah_line.get("push", 0),
            "model_key": model_key,
        }


async def _safe_reply(update, text: str, max_len: int = 3900):
    """Send text, auto-splitting at line boundaries if too long."""
    if len(text) <= max_len:
        await update.message.reply_text(text)
        return
    chunks = []
    current = ""
    for line in text.split("\n"):
        if len(current) + len(line) + 1 > max_len:
            if current:
                chunks.append(current)
            current = line + "\n"
        else:
            current += line + "\n"
    if current.strip():
        chunks.append(current)
    for chunk in chunks:
        await update.message.reply_text(chunk)


# Per-chat selection state: {chat_id: {"command": str, "selected": set, "live_data": dict}}
_picker_state = {}


def _get_live_data():
    """Fetch live fixtures and organize by league code."""
    from src.config import API_FOOTBALL_LEAGUES
    live_matches = {}
    try:
        from src.collectors.api_football import get_live_fixtures
        fixtures = get_live_fixtures()
        for fix in fixtures:
            lid = fix.get("league_id")
            for code, fid in API_FOOTBALL_LEAGUES.items():
                if fid == lid:
                    if code not in live_matches:
                        live_matches[code] = []
                    live_matches[code].append(fix)
                    break
    except Exception:
        pass
    return live_matches


def _build_picker_msg(command: str, selected: set, live_data: dict) -> str:
    """Build the text message for league picker."""
    from src.config import LEAGUES
    total_live = sum(len(v) for v in live_data.values())
    msg = f"\U0001f3c6 CH\u1eccN GI\u1ea2I \u0110\u1ea4U\n"

    if command == "live" and total_live > 0:
        msg += f"\U0001f534 {total_live} tr\u1eadn \u0111ang tr\u1ef1c ti\u1ebfp:\n"
        msg += "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        for code in sorted(live_data.keys()):
            league_name = LEAGUES.get(code, code)
            matches = live_data[code]
            msg += f"\n\U0001f3c6 {league_name} ({len(matches)} tr\u1eadn)\n"
            for m in matches:
                minute = m.get("minute", 0) or 0
                hs = m.get("home_score", 0)
                aws = m.get("away_score", 0)
                home = m.get("home", "?")
                away = m.get("away", "?")
                msg += f"  \u26bd {home} {hs}-{aws} {away} ({minute}')\n"
        msg += "\n"
    elif command == "live":
        msg += "\u26bd Kh\u00f4ng c\u00f3 tr\u1eadn live hi\u1ec7n t\u1ea1i.\n\n"

    sel_count = len(selected)
    action = "ph\u00e2n t\u00edch" if command == "phantich" else "xem live"
    if sel_count > 0:
        msg += f"\u2705 \u0110\u00e3 ch\u1ecdn {sel_count} gi\u1ea3i. B\u1ea5m \u2705 X\u00c1C NH\u1eacN \u0111\u1ec3 {action}."
    else:
        msg += f"\U0001f447 B\u1ea5m gi\u1ea3i \u0111\u1ec3 ch\u1ecdn, sau \u0111\u00f3 b\u1ea5m \u2705 X\u00c1C NH\u1eacN."
    return msg


_PICKER_REGIONS_PER_PAGE = 6  # ~10-15 leagues per page depending on region sizes


def _picker_pages() -> list[list[tuple[str, list[str]]]]:
    """Split LEAGUE_REGIONS into pages of ~10 leagues each.

    Keeping regions intact (don't split a region across pages). Returns
    list[page], page = list[(region_name, [codes])].
    Auto-includes any discovered codes missing from LEAGUE_REGIONS into
    a synthetic 'KHÁC' bucket, so newly discovered leagues still appear.
    """
    from src.config import LEAGUE_REGIONS, LEAGUES
    regions = list(LEAGUE_REGIONS.items())

    # Collect codes not listed in any region → synthetic bucket
    covered = {c for _, codes in regions for c in codes}
    extras = [c for c in LEAGUES if c not in covered]
    if extras:
        regions = regions + [("\U0001f310 KH\u00c1C", extras)]

    pages: list[list[tuple[str, list[str]]]] = []
    current: list[tuple[str, list[str]]] = []
    count = 0
    for name, codes in regions:
        if current and count + len(codes) > 10:
            pages.append(current)
            current = []
            count = 0
        current.append((name, codes))
        count += len(codes)
    if current:
        pages.append(current)
    return pages


def _build_picker_keyboard(command: str, selected: set, live_data: dict, page: int = 0) -> InlineKeyboardMarkup:
    """Build inline keyboard with toggle checkboxes (paginated)."""
    from src.config import LEAGUES, LEAGUES_SHORT
    keyboard = []

    # Shortcut: run across ALL leagues (bypasses selection).
    _shortcut_label = (
        "\U0001f310 XEM T\u1ea4T C\u1ea2 LIVE"
        if command == "live"
        else "\U0001f310 PH\u00c2N T\u00cdCH T\u1ea4T C\u1ea2 GI\u1ea2I"
    )
    keyboard.append([InlineKeyboardButton(
        _shortcut_label,
        callback_data=f"allleagues:{command}",
    )])

    pages = _picker_pages()
    total_pages = max(1, len(pages))
    page = max(0, min(page, total_pages - 1))

    for region, codes in pages[page]:
        keyboard.append([InlineKeyboardButton(
            f"\u2500\u2500 {region} \u2500\u2500",
            callback_data=f"region:{command}:{','.join(codes)}"
        )])
        row = []
        for code in codes:
            is_selected = code in selected
            match_count = len(live_data.get(code, []))
            name = LEAGUES_SHORT.get(code, LEAGUES.get(code, code))
            check = "\u2705" if is_selected else "\u2b1c"
            if match_count > 0:
                label = f"{check} \U0001f534 {name} ({match_count})"
            else:
                label = f"{check} {name}"
            row.append(InlineKeyboardButton(label, callback_data=f"tog:{command}:{code}"))
            if len(row) == 2:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)

    # Pagination row (only shown if >1 page)
    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("\u2b05 Tr\u01b0\u1edbc",
                                            callback_data=f"pickp:{command}:{page - 1}"))
        nav.append(InlineKeyboardButton(f"Trang {page + 1}/{total_pages}",
                                        callback_data="noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton("Sau \u27a1",
                                            callback_data=f"pickp:{command}:{page + 1}"))
        keyboard.append(nav)

    # Bottom action buttons
    keyboard.append([
        InlineKeyboardButton("\u2705 X\u00c1C NH\u1eacN", callback_data=f"run:{command}"),
    ])
    keyboard.append([
        InlineKeyboardButton("\U0001f534 CH\u1eccN T\u1ea4T C\u1ea2 LIVE", callback_data=f"alllive:{command}"),
        InlineKeyboardButton("\U0001f310 CH\u1eccN T\u1ea4T C\u1ea2", callback_data=f"selectall:{command}"),
    ])
    keyboard.append([
        InlineKeyboardButton("\u274c B\u1ecf ch\u1ecdn t\u1ea5t c\u1ea3", callback_data=f"clear:{command}"),
    ])

    return InlineKeyboardMarkup(keyboard)


async def _show_league_picker(update, command: str):
    """Show multi-select league picker with inline keyboard."""
    chat_id = update.effective_chat.id
    live_data = _get_live_data() if command == "live" else {}

    # Auto-select leagues that have live matches
    auto_selected = set(live_data.keys()) if command == "live" else set()

    _picker_state[chat_id] = {
        "command": command,
        "selected": auto_selected,
        "live_data": live_data,
        "page": 0,
    }

    msg = _build_picker_msg(command, auto_selected, live_data)
    kb = _build_picker_keyboard(command, auto_selected, live_data, page=0)
    await update.message.reply_text(msg, reply_markup=kb)


# Store chat IDs that have subscribed to alerts
_subscribers: set[int] = set()

# Persistent auth file
_AUTH_FILE = Path(__file__).resolve().parent.parent.parent / ".authenticated_chats"


def _load_authenticated() -> set[int]:
    """Load authenticated chat IDs from file."""
    try:
        if _AUTH_FILE.exists():
            return {int(line.strip()) for line in _AUTH_FILE.read_text().splitlines() if line.strip()}
    except Exception:
        pass
    return set()


def _save_authenticated():
    """Save authenticated chat IDs to file."""
    try:
        _AUTH_FILE.write_text("\n".join(str(cid) for cid in _authenticated))
    except Exception:
        pass


# Authenticated chat IDs (loaded from file on startup)
_authenticated: set[int] = _load_authenticated()


def _is_authenticated(chat_id: int) -> bool:
    """Check if a chat is authenticated."""
    from src.config import BOT_PASSWORD
    if not BOT_PASSWORD:
        return True
    return chat_id in _authenticated


async def _require_auth(update: Update) -> bool:
    """Check auth and send login prompt if not authenticated. Returns True if OK."""
    chat_id = update.effective_chat.id
    if _is_authenticated(chat_id):
        return True
    await update.message.reply_text(
        "\U0001f512 B\u1ea1n ch\u01b0a \u0111\u0103ng nh\u1eadp!\n\n"
        "D\u00f9ng l\u1ec7nh: /login <m\u1eadt kh\u1ea9u>\n"
        "VD: /login 123456"
    )
    return False


async def cmd_login(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Authenticate with password. Deletes the message to hide password."""
    from src.config import BOT_PASSWORD
    chat_id = update.effective_chat.id
    args = context.args or []

    # Always try to delete the user's message (contains password)
    try:
        await update.message.delete()
    except Exception:
        pass  # May fail if bot lacks delete permission

    if not BOT_PASSWORD:
        await context.bot.send_message(chat_id, "\u2705 Bot kh\u00f4ng y\u00eau c\u1ea7u m\u1eadt kh\u1ea9u.")
        return

    if _is_authenticated(chat_id):
        await context.bot.send_message(chat_id, "\u2705 B\u1ea1n \u0111\u00e3 \u0111\u0103ng nh\u1eadp r\u1ed3i!")
        return

    if not args:
        await context.bot.send_message(
            chat_id,
            "\U0001f512 Nh\u1eadp m\u1eadt kh\u1ea9u:\n/login <m\u1eadt kh\u1ea9u>"
        )
        return

    password = args[0]
    if password == BOT_PASSWORD:
        _authenticated.add(chat_id)
        _subscribers.add(chat_id)
        _save_authenticated()
        await context.bot.send_message(
            chat_id,
            "\u2705 \u0110\u0103ng nh\u1eadp th\u00e0nh c\u00f4ng!\n\n"
            "D\u00f9ng /start \u0111\u1ec3 xem danh s\u00e1ch l\u1ec7nh."
        )
    else:
        await context.bot.send_message(chat_id, "\u274c Sai m\u1eadt kh\u1ea9u! Th\u1eed l\u1ea1i.")


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not await _require_auth(update):
        return
    _subscribers.add(chat_id)
    await update.message.reply_text(
        "\u26bd Football Analytics Bot\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "B\u1ea1n \u0111\u00e3 \u0111\u0103ng k\u00fd nh\u1eadn th\u00f4ng b\u00e1o!\n\n"
        "Commands:\n"
        "/tatca \u2014 T\u1ea5t c\u1ea3 tr\u1eadn s\u1eafp di\u1ec5n ra\n"
        "/tatca PL \u2014 L\u1ecdc theo gi\u1ea3i\n"
        "/phantich \u2014 Ph\u00e2n t\u00edch tr\u1eadn trong 24h\n"
        "/live \u2014 C\u00e1 c\u01b0\u1ee3c tr\u1ef1c ti\u1ebfp (in-play)\n"
        "/today \u2014 Ph\u00e2n t\u00edch to\u00e0n b\u1ed9 h\u00f4m nay\n"
        "\U0001f3af /ancan \u2014 K\u00e8o d\u1ec5 th\u1eafng (Prob \u2265 60%, \u0111\u00e3 l\u1ecdc \u1ea3o)\n"
        "/keoxien \u2014 K\u00e8o xi\u00ean 2\u201310\n"
        "/stats \u2014 Th\u1ed1ng k\u00ea hi\u1ec7u su\u1ea5t\n"
        "/history \u2014 L\u1ecbch s\u1eed d\u1ef1 \u0111o\u00e1n\n"
        "/xoa \u2014 Xo\u00e1 l\u1ecbch s\u1eed\n"
        "/quanly \u2014 Qu\u1ea3n l\u00fd nh\u00e0 c\u00e1i\n"
        "/leagues \u2014 Danh s\u00e1ch gi\u1ea3i \u0111\u1ea5u\n"
        "/giahan \u2014 Ki\u1ec3m tra quota API\n"
        "/help \u2014 Tr\u1ee3 gi\u00fap"
    )


async def cmd_ancan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """K\u00e8o d\u1ec5 th\u1eafng \u2014 prob \u2265 58%, \u0111\u00e3 l\u1ecdc \u1ea3o.

    Query Prediction trong 24h g\u1ea7n nh\u1ea5t, match ch\u01b0a kickoff; \u00e1p
    `_is_ev_suspicious` + 3 rule b\u1ed5 sung (Draw prob>40%, odds<1.25, corner
    prob>75%); sort prob desc; hi\u1ec3n th\u1ecb top 30.
    """
    if not await _require_auth(update):
        return
    from datetime import datetime, timedelta
    from src.config import LEAGUES

    CORNER_MARKETS = {
        "corners_totals", "corners_spreads",
        "corners_h1_totals", "corners_h1_spreads",
    }
    MKT_NAMES = {
        "h2h": "1X2",
        "totals": "T\u00e0i/X\u1ec9u",
        "spreads": "Ch\u00e2u \u00c1",
        "corners_totals": "G\u00f3c T/X",
        "corners_spreads": "G\u00f3c Ch\u00e2u \u00c1",
        "corners_h1_totals": "G\u00f3c H1 T/X",
        "corners_h1_spreads": "G\u00f3c H1 Ch\u00e2u \u00c1",
    }

    session = get_session()
    try:
        now = datetime.utcnow()
        window_end = now + timedelta(hours=24)

        # Filter by Match.utc_date (kickoff) instead of Prediction.created_at
        # so we only surface picks whose matches actually kick off within 24h.
        rows = (
            session.query(Prediction, Match)
            .join(Match, Prediction.match_id == Match.match_id)
            .filter(
                Prediction.is_value_bet == True,
                Prediction.model_probability >= 0.58,
                Match.utc_date >= now,
                Match.utc_date <= window_end,
            )
            .order_by(Prediction.model_probability.desc())
            .all()
        )

        if not rows:
            await update.message.reply_text(
                "\u26d4 Kh\u00f4ng c\u00f3 k\u00e8o prob \u2265 58% trong 24h t\u1edbi.\n"
                "Th\u1eed /phantich \u0111\u1ec3 ch\u1ea1y ph\u00e2n t\u00edch tr\u01b0\u1edbc."
            )
            return

        kept: list[tuple] = []
        filtered = 0
        for p, m in rows:
            mkt_for_check = "corner" if p.market in CORNER_MARKETS else p.market
            vb = {
                "ev": p.expected_value or 0,
                "bookmaker": p.best_bookmaker or "",
                "market": mkt_for_check,
                "outcome": p.outcome or "",
            }
            susp, _ = _is_ev_suspicious(vb)
            if susp:
                filtered += 1
                continue
            if p.market == "h2h" and p.outcome == "Draw" and (p.model_probability or 0) > 0.40:
                filtered += 1
                continue
            if (p.best_odds or 0) < 1.25:
                filtered += 1
                continue
            if p.market in CORNER_MARKETS and (p.model_probability or 0) > 0.75:
                filtered += 1
                continue
            kept.append((p, m))

        top = kept[:30]
        if not top:
            await update.message.reply_text(
                f"\u26d4 Kh\u00f4ng c\u00f2n k\u00e8o n\u00e0o sau khi l\u1ecdc "
                f"(\u0111\u00e3 lo\u1ea1i {filtered} k\u00e8o \u1ea3o)."
            )
            return

        header = (
            f"\U0001f3af K\u00c8O D\u1ec4 TH\u1eaeNG (Prob \u2265 58%)\n"
            f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
            f"\u2705 Top {len(top)} k\u00e8o prob cao trong 24h t\u1edbi\n"
            f"\U0001f6ab \u0110\u00e3 lo\u1ea1i: {filtered} k\u00e8o \u1ea3o\n"
        )

        body = ""
        for i, (p, m) in enumerate(top, 1):
            league = LEAGUES.get(
                m.competition_code or "",
                m.competition_code or m.competition or "?",
            )
            when = m.utc_date.strftime("%d/%m %H:%M") if m.utc_date else "?"
            mkt = MKT_NAMES.get(p.market, p.market)
            prob = (p.model_probability or 0) * 100
            ev = (p.expected_value or 0) * 100
            odds = p.best_odds or 0
            body += (
                f"\n#{i} {m.home_team} vs {m.away_team}\n"
                f"\u23f0 {when} | \U0001f3c6 {league}\n"
                f"\u279c {p.outcome} ({mkt}) @ {odds:.2f}\n"
                f"\u2705 X\u00e1c su\u1ea5t th\u1eafng: {prob:.0f}%\n"
                f"\U0001f4b0 Odds: {odds:.2f} | EV: {ev:+.1f}%\n"
                f"\U0001f4ca {p.best_bookmaker or '?'}\n"
            )

        logger.info(
            f"[ancan] returned {len(top)} picks trong next 24h, "
            f"filtered {filtered} suspicious"
        )
        await _send_chunked(update, header + body)
    finally:
        session.close()


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show today's predictions from the database (no re-analysis)."""
    if not await _require_auth(update):
        return
    from datetime import datetime, date
    from src.config import LEAGUES

    session = get_session()
    try:
        today_start = datetime.combine(date.today(), datetime.min.time())
        preds = (
            session.query(Prediction)
            .filter(Prediction.is_value_bet == True, Prediction.created_at >= today_start)
            .order_by(Prediction.created_at.asc())
            .all()
        )
        if not preds:
            await update.message.reply_text(
                "📭 Chưa có dữ liệu phân tích hôm nay.\n"
                "Dùng /phantich để chạy phân tích trước."
            )
            return

        # Group predictions by match_id
        match_ids = list(dict.fromkeys(p.match_id for p in preds))
        matches = {
            m.match_id: m
            for m in session.query(Match).filter(Match.match_id.in_(match_ids)).all()
        }

        # Group by competition
        by_comp: dict[str, list[int]] = {}
        for mid in match_ids:
            m = matches.get(mid)
            comp = (m.competition_code if m else None) or "?"
            by_comp.setdefault(comp, []).append(mid)

        # Market display names
        MKT_NAMES = {
            "h2h": "1X2",
            "totals": "Tài/Xỉu",
            "spreads": "Châu Á",
            "corners_totals": "Góc T/X",
            "corners_spreads": "Góc Châu Á",
            "corners_h1_totals": "Góc H1 T/X",
            "corners_h1_spreads": "Góc H1 Châu Á",
        }

        # Confidence icons
        CONF_ICON = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}

        preds_by_match: dict[int, list] = {}
        for p in preds:
            preds_by_match.setdefault(p.match_id, []).append(p)

        total_picks = len(preds)
        high_count = sum(1 for p in preds if p.confidence == "HIGH")

        header = (
            f"📋 TỔNG HỢP HÔM NAY ({date.today().strftime('%d/%m/%Y')})\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"📊 {total_picks} kèo value | 🔴 {high_count} HIGH\n\n"
        )

        msgs = [header]
        for comp_code, mids in by_comp.items():
            league_name = LEAGUES.get(comp_code, comp_code)
            comp_msg = f"🏆 {league_name}\n{'─' * 20}\n"

            for mid in mids:
                m = matches.get(mid)
                if not m:
                    continue
                match_preds = preds_by_match.get(mid, [])
                if not match_preds:
                    continue

                comp_msg += f"\n⚽ {m.home_team} vs {m.away_team}\n"
                if m.utc_date:
                    comp_msg += f"🕐 {m.utc_date.strftime('%H:%M %d/%m')}\n"

                # Sort: HIGH first, then by EV desc
                match_preds.sort(
                    key=lambda p: (
                        0 if p.confidence == "HIGH" else 1 if p.confidence == "MEDIUM" else 2,
                        -(p.expected_value or 0),
                    )
                )

                for p in match_preds:
                    icon = CONF_ICON.get(p.confidence, "⚪")
                    mkt = MKT_NAMES.get(p.market, p.market)
                    ev_pct = (p.expected_value or 0) * 100
                    comp_msg += (
                        f"  {icon} [{mkt}] {p.outcome} @ {p.best_odds:.2f}"
                        f" (EV {ev_pct:+.1f}% | {p.confidence})\n"
                    )

            comp_msg += "\n"
            msgs.append(comp_msg)

        await _safe_reply(update, "".join(msgs))
    finally:
        session.close()


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _require_auth(update): return
    session = get_session()
    try:
        all_preds = session.query(Prediction).filter(Prediction.is_value_bet == True).all()
        resolved = [p for p in all_preds if p.result in ("WIN", "LOSE")]
        wins = [p for p in resolved if p.result == "WIN"]

        by_market = {}
        for p in resolved:
            if p.market not in by_market:
                by_market[p.market] = {"wins": 0, "total": 0}
            by_market[p.market]["total"] += 1
            if p.result == "WIN":
                by_market[p.market]["wins"] += 1
        for mkt in by_market.values():
            mkt["rate"] = (mkt["wins"] / mkt["total"] * 100) if mkt["total"] > 0 else 0

        high_preds = [p for p in resolved if p.confidence == "HIGH"]
        med_preds = [p for p in resolved if p.confidence == "MEDIUM"]
        low_preds = [p for p in resolved if p.confidence == "LOW"]

        stats = {
            "total": len(all_preds),
            "resolved": len(resolved),
            "win_rate": (len(wins) / len(resolved) * 100) if resolved else 0,
            "high_wins": len([p for p in high_preds if p.result == "WIN"]),
            "high_total": len(high_preds),
            "med_wins": len([p for p in med_preds if p.result == "WIN"]),
            "med_total": len(med_preds),
            "low_wins": len([p for p in low_preds if p.result == "WIN"]),
            "low_total": len(low_preds),
            "by_market": by_market,
        }
        await update.message.reply_text(format_stats(stats))
    finally:
        session.close()


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /history        — thống kê hôm nay
    /history N      — thống kê N ngày gần nhất (max 7)
    /history YYYY-MM-DD — thống kê ngày cụ thể
    """
    if not await _require_auth(update):
        return
    from datetime import date, datetime, timedelta
    from sqlalchemy import func

    args = (context.args or [])
    today = date.today()

    # Parse argument
    if not args:
        target_dates = [today]
    elif args[0].isdigit():
        n_days = min(int(args[0]), 7)
        target_dates = [today - timedelta(days=i) for i in range(n_days)]
    else:
        try:
            target_dates = [datetime.strptime(args[0], "%Y-%m-%d").date()]
        except ValueError:
            await update.message.reply_text("⚠️ Dùng: /history hoặc /history 3 hoặc /history 2026-04-11")
            return

    session = get_session()
    try:
        all_messages = []
        grand_total = 0
        grand_value = 0
        grand_win = 0
        grand_lose = 0
        grand_pending = 0

        for target_date in target_dates:
            day_start = datetime(target_date.year, target_date.month, target_date.day)
            day_end = day_start + timedelta(days=1)

            # All predictions for this day
            preds = (
                session.query(Prediction)
                .filter(Prediction.created_at >= day_start, Prediction.created_at < day_end)
                .order_by(Prediction.created_at.asc())
                .all()
            )
            if not preds:
                all_messages.append(f"📅 {target_date.strftime('%d/%m/%Y')} — Không có phân tích\n")
                continue

            # Group by match
            match_map = {}
            for p in preds:
                if p.match_id not in match_map:
                    match_map[p.match_id] = []
                match_map[p.match_id].append(p)

            # Stats
            total_preds = len(preds)
            value_bets = [p for p in preds if p.is_value_bet]
            n_value = len(value_bets)
            n_win = sum(1 for p in value_bets if p.result == "WIN")
            n_lose = sum(1 for p in value_bets if p.result == "LOSE")
            n_push = sum(1 for p in value_bets if p.result == "PUSH")
            n_pending = sum(1 for p in value_bets if p.result is None)
            n_high = sum(1 for p in value_bets if p.confidence == "HIGH")
            n_med = sum(1 for p in value_bets if p.confidence == "MEDIUM")
            n_low = sum(1 for p in value_bets if p.confidence == "LOW")
            n_high_win = sum(1 for p in value_bets if p.confidence == "HIGH" and p.result == "WIN")
            n_med_win = sum(1 for p in value_bets if p.confidence == "MEDIUM" and p.result == "WIN")
            n_low_win = sum(1 for p in value_bets if p.confidence == "LOW" and p.result == "WIN")
            win_rate = n_win / (n_win + n_lose) * 100 if (n_win + n_lose) > 0 else 0

            # ROI calculation
            total_stake = n_win + n_lose + n_push  # each bet = 1 unit
            total_return = sum(p.best_odds for p in value_bets if p.result == "WIN")
            roi = (total_return - total_stake) / total_stake * 100 if total_stake > 0 else 0

            grand_total += total_preds
            grand_value += n_value
            grand_win += n_win
            grand_lose += n_lose
            grand_pending += n_pending

            date_str = target_date.strftime('%d/%m/%Y')
            is_today = target_date == today
            day_label = f"{date_str} (HÔM NAY)" if is_today else date_str

            msg = f"📅 {day_label}\n"
            msg += f"━━━━━━━━━━━━━━━━━\n"
            msg += f"📊 Tổng quan:\n"
            msg += f"  Trận phân tích: {len(match_map)}\n"
            msg += f"  Tổng dự đoán: {total_preds}\n"
            msg += f"  Value bets: {n_value}\n"
            if n_win + n_lose > 0:
                msg += f"  Tỉ lệ thắng: {win_rate:.1f}% ({n_win}W / {n_lose}L"
                if n_push:
                    msg += f" / {n_push}P"
                msg += ")\n"
                msg += f"  ROI: {roi:+.1f}%\n"
            if n_pending > 0:
                msg += f"  Chờ kết quả: {n_pending}\n"
            msg += f"\n"

            # Confidence breakdown
            msg += f"🎯 Theo độ tin cậy:\n"
            if n_high:
                h_wr = f" ({n_high_win}W)" if n_high_win else ""
                msg += f"  🔴 HIGH: {n_high} picks{h_wr}\n"
            if n_med:
                m_wr = f" ({n_med_win}W)" if n_med_win else ""
                msg += f"  🟡 MEDIUM: {n_med} picks{m_wr}\n"
            if n_low:
                l_wr = f" ({n_low_win}W)" if n_low_win else ""
                msg += f"  🟢 LOW: {n_low} picks{l_wr}\n"
            msg += f"\n"

            # Market breakdown
            market_stats = {}
            for p in value_bets:
                mk = p.market
                if mk not in market_stats:
                    market_stats[mk] = {"total": 0, "win": 0, "lose": 0, "pending": 0}
                market_stats[mk]["total"] += 1
                if p.result == "WIN":
                    market_stats[mk]["win"] += 1
                elif p.result == "LOSE":
                    market_stats[mk]["lose"] += 1
                elif p.result is None:
                    market_stats[mk]["pending"] += 1

            MARKET_NAMES = {
                "h2h": "1X2", "totals": "Tài/Xỉu", "asian_handicap": "Châu Á",
                "corners_totals": "Góc T/X", "corners_spreads": "Góc CÁ",
                "h1_corners_totals": "Góc H1 T/X", "h1_corners_spreads": "Góc H1 CÁ",
            }
            if market_stats:
                msg += f"📈 Theo thị trường:\n"
                for mk, st in sorted(market_stats.items(), key=lambda x: x[1]["total"], reverse=True):
                    mk_name = MARKET_NAMES.get(mk, mk)
                    wr = ""
                    if st["win"] + st["lose"] > 0:
                        r = st["win"] / (st["win"] + st["lose"]) * 100
                        wr = f" | {r:.0f}%"
                    msg += f"  {mk_name}: {st['total']} picks ({st['win']}W {st['lose']}L {st['pending']}⏳){wr}\n"
                msg += f"\n"

            # Match details — value bets only
            msg += f"⚽ Chi tiết trận:\n"
            for mid, match_preds in match_map.items():
                match = session.query(Match).filter(Match.match_id == mid).first()
                if match:
                    match_name = f"{match.home_team} vs {match.away_team}"
                    score = ""
                    if match.home_goals is not None:
                        score = f" ({match.home_goals}-{match.away_goals})"
                    league = match.competition_code or ""
                else:
                    match_name = f"#{mid}"
                    score = ""
                    league = ""

                vb = [p for p in match_preds if p.is_value_bet]
                if not vb:
                    continue

                league_str = f" [{league}]" if league else ""
                msg += f"  {match_name}{score}{league_str}\n"
                for p in sorted(vb, key=lambda x: x.expected_value, reverse=True):
                    if p.result == "WIN":
                        icon = "✅"
                    elif p.result == "LOSE":
                        icon = "❌"
                    elif p.result == "PUSH":
                        icon = "↩️"
                    else:
                        icon = "⏳"
                    conf_tag = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}.get(p.confidence, "⚪")
                    mk_short = MARKET_NAMES.get(p.market, p.market)
                    msg += f"    {icon}{conf_tag} {p.outcome} @{p.best_odds:.2f} EV:{p.expected_value*100:+.1f}% ({mk_short}) [{p.best_bookmaker}]\n"

            all_messages.append(msg)

        # Grand summary if multiple days
        if len(target_dates) > 1 and grand_value > 0:
            g_wr = grand_win / (grand_win + grand_lose) * 100 if (grand_win + grand_lose) > 0 else 0
            summary = f"\n📊 TỔNG KẾT {len(target_dates)} NGÀY:\n"
            summary += f"━━━━━━━━━━━━━━━━━\n"
            summary += f"  Value bets: {grand_value} | Thắng: {g_wr:.1f}% ({grand_win}W/{grand_lose}L)\n"
            if grand_pending:
                summary += f"  Chờ kết quả: {grand_pending}\n"
            all_messages.append(summary)

        await _safe_reply(update, "\n".join(all_messages))
    finally:
        session.close()


async def cmd_xoa(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xoá toàn bộ lịch sử phân tích (predictions, matches, daily reports)."""
    if not await _require_auth(update):
        return
    session = get_session()
    try:
        n_pred = session.query(Prediction).count()
        n_match = session.query(Match).count()
        n_report = session.query(DailyReport).count()
        if n_pred == 0 and n_match == 0 and n_report == 0:
            await update.message.reply_text("📭 Không có dữ liệu nào để xoá.")
            return
        session.query(Prediction).delete()
        session.query(Match).delete()
        session.query(DailyReport).delete()
        session.commit()
        await update.message.reply_text(
            "🗑 Đã xoá toàn bộ lịch sử:\n"
            f"• {n_pred} dự đoán\n"
            f"• {n_match} trận đấu\n"
            f"• {n_report} báo cáo ngày"
        )
    finally:
        session.close()


async def cmd_bookie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _require_auth(update): return
    args = context.args or []
    session = get_session()
    try:
        if not args or args[0] == "list":
            bookies = session.query(Bookmaker).order_by(Bookmaker.is_default.desc()).all()
            data = [{"name": b.name, "url": b.url, "is_default": b.is_default} for b in bookies]
            await update.message.reply_text(format_bookmaker_list(data))

        elif args[0] == "add" and len(args) >= 3:
            name = args[1]
            url = args[2]
            existing = session.query(Bookmaker).filter(Bookmaker.key == name.lower()).first()
            if existing:
                await update.message.reply_text(f"\u26a0\ufe0f Nh\u00e0 c\u00e1i '{name}' \u0111\u00e3 t\u1ed3n t\u1ea1i.")
                return
            bk = Bookmaker(key=name.lower(), name=name, url=url)
            session.add(bk)
            session.commit()
            await update.message.reply_text(f"\u2705 \u0110\u00e3 th\u00eam: {name} \u2014 {url}")

        elif args[0] == "remove" and len(args) >= 2:
            name = args[1]
            bk = session.query(Bookmaker).filter(Bookmaker.key == name.lower()).first()
            if bk:
                session.delete(bk)
                session.commit()
                await update.message.reply_text(f"\U0001f5d1\ufe0f \u0110\u00e3 x\u00f3a: {name}")
            else:
                await update.message.reply_text(f"\u274c Kh\u00f4ng t\u00ecm th\u1ea5y: {name}")

        elif args[0] == "default" and len(args) >= 2:
            name = args[1]
            # Reset all defaults
            session.query(Bookmaker).update({"is_default": False})
            bk = session.query(Bookmaker).filter(Bookmaker.key == name.lower()).first()
            if bk:
                bk.is_default = True
                session.commit()
                await update.message.reply_text(f"\u2b50 Nh\u00e0 c\u00e1i m\u1eb7c \u0111\u1ecbnh: {bk.name}")
            else:
                session.commit()
                await update.message.reply_text(f"\u274c Kh\u00f4ng t\u00ecm th\u1ea5y: {name}")

        else:
            await update.message.reply_text(
                "S\u1eed d\u1ee5ng:\n"
                "/quanly list\n"
                "/quanly add <t\u00ean> <url>\n"
                "/quanly remove <t\u00ean>\n"
                "/quanly default <t\u00ean>"
            )
    finally:
        session.close()


async def cmd_matches(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show all upcoming matches, optionally filtered by league code."""
    if not await _require_auth(update): return
    args = context.args or []
    league_filter = args[0].upper() if args else None

    session = get_session()
    try:
        from datetime import datetime
        query = (
            session.query(Match)
            .filter(Match.status == "SCHEDULED", Match.utc_date >= datetime.utcnow())
            .order_by(Match.utc_date)
        )
        if league_filter:
            query = query.filter(Match.competition_code == league_filter)

        matches = query.all()

        if not matches:
            hint = f" cho {league_filter}" if league_filter else ""
            await update.message.reply_text(f"\U0001f4ad Kh\u00f4ng c\u00f3 tr\u1eadn s\u1eafp di\u1ec5n ra{hint}.")
            return

        # Group by competition
        by_comp = {}
        for m in matches:
            comp = m.competition or "Unknown"
            if comp not in by_comp:
                by_comp[comp] = []
            by_comp[comp].append(m)

        # Telegram max message 4096 chars — split if needed
        messages = []
        current_msg = f"\u26bd TR\u1eacN S\u1eaeP DI\u1ec4N RA ({len(matches)} tr\u1eadn)\n\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"

        for comp, comp_matches in by_comp.items():
            section = f"\n\U0001f3c6 {comp}\n"
            for m in comp_matches:
                try:
                    time_str = m.utc_date.strftime("%d/%m %H:%M")
                except Exception:
                    time_str = "?"
                line = f"  \U0001f552 {time_str} | {m.home_team} vs {m.away_team}\n"
                section += line

            # Check message length
            if len(current_msg) + len(section) > 3900:
                messages.append(current_msg)
                current_msg = f"\u26bd TI\u1ebeP THEO...\n\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
            current_msg += section

        current_msg += f"\n\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\U0001f4a1 L\u1ecdc theo gi\u1ea3i: /tatca PL, /tatca BL1..."
        messages.append(current_msg)

        for msg in messages:
            await _safe_reply(update, msg)
    finally:
        session.close()


async def cmd_analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Analyze matches in the next 24h with Poisson model + odds."""
    if not await _require_auth(update): return
    args = context.args or []
    league_filter = args[0].upper() if args else None

    if not league_filter:
        await _show_league_picker(update, "phantich")
        return

    from src.config import LEAGUES
    if league_filter not in LEAGUES:
        await update.message.reply_text(f"\u274c M\u00e3 gi\u1ea3i '{league_filter}' kh\u00f4ng h\u1ee3p l\u1ec7. D\u00f9ng /leagues \u0111\u1ec3 xem danh s\u00e1ch.")
        return

    await _run_full_analysis(update, league_codes=[league_filter])


async def _run_full_analysis(update, league_codes: list[str] | None = None, collect_only: bool = False):
    """
    Shared analysis engine used by /today, /phantich, and /keoxien.
    league_codes=None → all leagues with scheduled matches in 24h.
    collect_only=True → skip sending messages, return list of all value picks.
    """
    session = get_session()
    try:
        from datetime import datetime, timedelta
        from src.models.poisson import PoissonModel, calculate_expected_value, get_confidence_tier
        from src.models.dixon_coles import DixonColesModel
        from src.collectors.football_data import get_recent_results
        from src.collectors.odds_api import get_odds, get_best_odds, get_spread_pairs, get_corner_odds
        from src.config import LEAGUES, ODDS_SPORTS, USE_DIXON_COLES
        import asyncio

        _ModelCls = DixonColesModel if USE_DIXON_COLES else PoissonModel

        now = datetime.utcnow()
        next_24h = now + timedelta(hours=24)

        query = (
            session.query(Match)
            .filter(Match.status == "SCHEDULED", Match.utc_date >= now, Match.utc_date <= next_24h)
            .order_by(Match.utc_date)
        )
        if league_codes:
            if len(league_codes) == 1:
                query = query.filter(Match.competition_code == league_codes[0])
            else:
                query = query.filter(Match.competition_code.in_(league_codes))
        matches = query.all()

        if not matches:
            if collect_only:
                return []
            hint = f" cho {', '.join(league_codes)}" if league_codes else ""
            await update.message.reply_text(f"\U0001f4ad Kh\u00f4ng c\u00f3 tr\u1eadn n\u00e0o trong 24h t\u1edbi{hint}.")
            return

        # Group matches by league
        by_league = {}
        for m in matches:
            code = m.competition_code or "?"
            if code not in by_league:
                by_league[code] = []
            by_league[code].append(m)

        total_analyzed = sum(len(v) for v in by_league.values())
        if not collect_only:
            await update.message.reply_text(
                f"\u23f3 \u0110ang ph\u00e2n t\u00edch {total_analyzed} tr\u1eadn "
                f"trong 24h t\u1edbi ({len(by_league)} gi\u1ea3i)..."
            )

        # The Odds API: all leagues + corners in parallel (with per-task timeout)
        async def _get_odds_api(lc):
            if lc in ODDS_SPORTS:
                try:
                    return lc, await asyncio.wait_for(asyncio.to_thread(get_odds, lc), timeout=30)
                except Exception as e:
                    logger.warning(f"[Analyze] Odds fetch failed for {lc}: {e}")
            return lc, []

        async def _get_corners_api(lc):
            if lc in ODDS_SPORTS:
                try:
                    return lc, await asyncio.wait_for(asyncio.to_thread(get_corner_odds, lc), timeout=45)
                except Exception as e:
                    logger.warning(f"[Analyze] Corner fetch failed for {lc}: {e}")
            return lc, {}

        odds_tasks = [_get_odds_api(lc) for lc in by_league.keys()]
        corner_tasks = [_get_corners_api(lc) for lc in by_league.keys()]

        odds_results, corner_results = await asyncio.gather(
            asyncio.gather(*odds_tasks),
            asyncio.gather(*corner_tasks),
        )

        # Belt-and-suspenders 24h filter on odds events. DB matches are
        # already 24h-filtered, but odds API returns fixtures days out —
        # trimming here avoids wasted fuzzy matching and makes the skip
        # count visible in logs.
        def _within_24h(ev_dict: dict) -> bool:
            ct = ev_dict.get("commence_time")
            if not ct:
                return True
            try:
                ev_dt = datetime.fromisoformat(ct.replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                return True
            return now <= ev_dt <= next_24h

        all_odds = {}
        corners_map = {lc: data for lc, data in corner_results}
        skipped_total = 0
        for lc, odds_ev in odds_results:
            before = len(odds_ev)
            kept = [ev for ev in odds_ev if _within_24h(ev)]
            skipped = before - len(kept)
            if skipped:
                skipped_total += skipped
                logger.info(
                    f"[phantich] {lc}: skipped {skipped} fixtures outside 24h window "
                    f"({before} -> {len(kept)})"
                )
            all_odds[lc] = (kept, corners_map.get(lc, {}))
        if skipped_total:
            logger.info(f"[phantich] Skipped {skipped_total} fixtures outside 24h window total")

        # Fit model & analyze
        messages = []
        top_picks = []
        avoid_picks = []
        parlay_picks = []  # ALL positive EV picks for parlay generation
        safe_picks = []  # Prob >= 70% picks regardless of confidence tier
        total_hist = 0

        for league_code, league_matches in by_league.items():
            # Fit Poisson model from DB historical data
            hist = (
                session.query(Match)
                .filter(Match.competition_code == league_code, Match.status == "FINISHED")
                .all()
            )
            results_data = [
                {"home_team": h.home_team, "away_team": h.away_team,
                 "home_goals": h.home_goals, "away_goals": h.away_goals,
                 "utc_date": h.utc_date.isoformat() if h.utc_date else None}
                for h in hist if h.home_goals is not None
            ]
            total_hist += len(results_data)

            model = _ModelCls()
            model.fit(results_data)

            # Use pre-fetched odds
            odds_events, league_corners = all_odds.get(league_code, ([], {}))

            league_name = LEAGUES.get(league_code, league_code)
            current_msg = f"\n\U0001f3c6 {league_name}\n\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n"

            for m in league_matches:
                # Find odds first — require team name AND kickoff time match
                odds_event = None
                for ev in odds_events:
                    if _match_event(m.home_team, m.away_team, m.utc_date, ev):
                        odds_event = ev
                        break

                if not odds_event:
                    logger.info(f"[Analyze] Skip {m.home_team} vs {m.away_team} — no bookmaker odds")
                    continue

                pred = model.predict(m.home_team, m.away_team)

                try:
                    time_str = m.utc_date.strftime("%H:%M %d/%m")
                except Exception:
                    time_str = "?"

                current_msg += f"\n\u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510\n"
                current_msg += f"  \u26bd {m.home_team} vs {m.away_team}\n"
                current_msg += f"  \U0001f552 {time_str}\n"
                current_msg += f"  \u26a1 xG: {m.home_team[:3]} {pred['home_xg']} - {pred['away_xg']} {m.away_team[:3]}\n"

                h = pred["h2h"]
                t = pred["totals"]
                b = pred["btts"]
                all_values = []
                ah = pred.get("asian_handicap", {})

                best_h2h = get_best_odds(odds_event, "h2h")
                best_totals = get_best_odds(odds_event, "totals")
                spread_pairs = get_spread_pairs(odds_event)

                # === 1X2 ===
                h_odds = best_h2h.get("Home", {})
                d_odds = best_h2h.get("Draw", {})
                a_odds = best_h2h.get("Away", {})
                h_price = f" @{h_odds['price']:.2f}" if isinstance(h_odds, dict) and "price" in h_odds else ""
                d_price = f" @{d_odds['price']:.2f}" if isinstance(d_odds, dict) and "price" in d_odds else ""
                a_price = f" @{a_odds['price']:.2f}" if isinstance(a_odds, dict) and "price" in a_odds else ""
                current_msg += f"  \U0001f1ea\U0001f1fa K\u00e8o Ch\u00e2u \u00c2u (1X2):\n"
                current_msg += f"    H: {h['Home']*100:.0f}%{h_price} | D: {h['Draw']*100:.0f}%{d_price} | A: {h['Away']*100:.0f}%{a_price}\n"

                # === ASIAN HANDICAP ===
                current_msg += f"  \U0001f30f K\u00e8o Ch\u00e2u \u00c1:\n"
                if spread_pairs:
                    pair = spread_pairs[0]
                    hp_str = f"{pair['home_point']:+g}" if pair['home_point'] != 0 else "0"
                    ap_str = f"{pair['away_point']:+g}" if pair['away_point'] != 0 else "0"

                    probs = _get_pair_probs(pair, ah, m.home_team)
                    h_prob = probs["pair_home_prob"]
                    a_prob = probs["pair_away_prob"]
                    push_prob = probs["push"]

                    current_msg += f"    {pair['home_name']} {hp_str}: {h_prob*100:.0f}% @{pair['home_price']:.2f}\n"
                    current_msg += f"    {pair['away_name']} {ap_str}: {a_prob*100:.0f}% @{pair['away_price']:.2f}\n"
                    if push_prob > 0.01:
                        current_msg += f"    Ho\u00e0 k\u00e8o: {push_prob*100:.0f}%\n"
                    current_msg += f"    ({pair['bookmaker']})\n"
                else:
                    current_msg += f"    Chưa có odds Châu Á\n"

                # === O/U ===
                ou_over = best_totals.get("Over", {})
                ou_under = best_totals.get("Under", {})
                o_price = f" @{ou_over['price']:.2f}" if isinstance(ou_over, dict) and "price" in ou_over else ""
                u_price = f" @{ou_under['price']:.2f}" if isinstance(ou_under, dict) and "price" in ou_under else ""
                o_point = ou_over.get("point", 2.5) if isinstance(ou_over, dict) else 2.5
                over_key = f"Over {o_point}"
                under_key = f"Under {o_point}"
                over_prob = t.get(over_key, t.get("Over 2.5", 0))
                under_prob = t.get(under_key, t.get("Under 2.5", 0))
                current_msg += f"  \u2b06 T\u00e0i/X\u1ec9u {o_point}:\n"
                current_msg += f"    T\u00e0i: {over_prob*100:.0f}%{o_price} | X\u1ec9u: {under_prob*100:.0f}%{u_price}\n"

                # === BTTS ===
                current_msg += f"  \U0001f945 BTTS: Yes {b['Yes']*100:.0f}% | No {b['No']*100:.0f}%\n"

                # === CORNERS ===
                corners_pred = pred.get("corners", {})
                corner_lines = corners_pred.get("lines", {})
                corner_ah_pred = corners_pred.get("asian_handicap", {})
                corner_key = f"{m.home_team}__{m.away_team}"
                corner_data = league_corners.get(corner_key, {})
                if not corner_data:
                    for ck, cv in league_corners.items():
                        parts = ck.split("__")
                        if len(parts) == 2 and _match_teams(m.home_team, m.away_team, parts[0], parts[1]):
                            corner_data = cv
                            break
                corner_totals_odds = corner_data.get("totals", {})
                corner_spreads = corner_data.get("spreads", [])
                corner_xg = corners_pred.get("xg", 10.5)
                home_xc = corners_pred.get("home_xc", 5.5)
                away_xc = corners_pred.get("away_xc", 5.0)

                # === CORNER DEBUG: log handler-level view for /phantich ===
                try:
                    logger.warning(
                        f"[CORNER-DEBUG-PHANTICH] {league_code} {m.home_team} vs {m.away_team} | "
                        f"corner_key={corner_key!r} | "
                        f"league_corners_keys={list(league_corners.keys())} | "
                        f"corner_data_keys={list((corner_data or {}).keys())} | "
                        f"totals_raw={corner_totals_odds} | "
                        f"spreads_count={len(corner_spreads)}"
                    )
                except Exception as _dbg_exc:
                    logger.warning(f"[CORNER-DEBUG-PHANTICH] dump failed: {_dbg_exc}")
                # === END CORNER DEBUG ===

                if corner_totals_odds or corner_lines or corner_spreads:
                    current_msg += f"  \u2691 Ph\u1ea1t g\u00f3c (xC: {corner_xg} | {m.home_team[:3]} {home_xc} - {away_xc} {m.away_team[:3]}):\n"

                    shown_corner = False
                    # Only the MAIN line bookmaker is offering — pick line with odds closest to model xC
                    odds_lines = [l for l in corner_totals_odds.keys() if corner_totals_odds[l].get("over_price")]
                    if odds_lines:
                        line = min(odds_lines, key=lambda x: abs(x - corner_xg))
                        cl = corner_lines.get(line, {})
                        co = corner_totals_odds.get(line, {})
                        o_prob = cl.get("over", 0)
                        u_prob = cl.get("under", 0)
                        o_price = f" @{co['over_price']:.2f}" if co.get("over_price") else ""
                        u_price = f" @{co['under_price']:.2f}" if co.get("under_price") else ""
                        current_msg += f"    T\u00e0i/X\u1ec9u {line}: T\u00e0i {o_prob*100:.0f}%{o_price} | X\u1ec9u {u_prob*100:.0f}%{u_price}\n"
                        shown_corner = True

                        if co.get("over_price") and o_prob > 0:
                            ev_co = o_prob * co["over_price"] - 1
                            all_values.append({"outcome": f"Góc Tài {line}", "market": "Phạt góc", "odds": co["over_price"], "ev": ev_co, "bk": co["over_bk"], "prob": o_prob, "line": line})
                        if co.get("under_price") and u_prob > 0:
                            ev_cu = u_prob * co["under_price"] - 1
                            all_values.append({"outcome": f"Góc Xỉu {line}", "market": "Phạt góc", "odds": co["under_price"], "ev": ev_cu, "bk": co["under_bk"], "prob": u_prob, "line": line})

                    if corner_spreads:
                        cs = corner_spreads[0]
                        hp = cs["home_point"]
                        ap = cs["away_point"]
                        hp_str = f"{hp:+g}"
                        ap_str = f"{ap:+g}"

                        pair_home_is_match_home = _is_home_team(cs["home_name"], m.home_team)
                        if pair_home_is_match_home:
                            model_key = f"{hp:+g}" if hp != 0 else "0"
                            ah_pred = corner_ah_pred.get(model_key, {})
                            h_prob = ah_pred.get("home", 0)
                            a_prob = ah_pred.get("away", 0)
                        else:
                            model_key = f"{ap:+g}" if ap != 0 else "0"
                            ah_pred = corner_ah_pred.get(model_key, {})
                            h_prob = ah_pred.get("away", 0)
                            a_prob = ah_pred.get("home", 0)

                        current_msg += (
                            f"    Châu Á: {cs['home_name'][:10]} {hp_str} "
                            f"{h_prob*100:.0f}% @{cs['home_price']:.2f} | "
                            f"{cs['away_name'][:10]} {ap_str} "
                            f"{a_prob*100:.0f}% @{cs['away_price']:.2f} ({cs['bk']})\n"
                        )
                        if h_prob > 0:
                            ev_ch = h_prob * cs["home_price"] - 1
                            all_values.append({"outcome": f"Góc {cs['home_name'][:10]} {hp_str}", "market": "Góc Châu Á", "odds": cs["home_price"], "ev": ev_ch, "bk": cs["bk"], "prob": h_prob})
                        if a_prob > 0:
                            ev_ca = a_prob * cs["away_price"] - 1
                            all_values.append({"outcome": f"Góc {cs['away_name'][:10]} {ap_str}", "market": "Góc Châu Á", "odds": cs["away_price"], "ev": ev_ca, "bk": cs["bk"], "prob": a_prob})
                    if not shown_corner and not corner_spreads:
                        current_msg += f"    Chưa có dữ liệu\n"

                # === FIRST HALF CORNERS ===
                h1c_pred = pred.get("corners_h1", {})
                h1c_lines = h1c_pred.get("lines", {})
                h1c_ah_pred = h1c_pred.get("asian_handicap", {})
                h1c_totals_odds = corner_data.get("h1_totals", {})
                h1c_spreads = corner_data.get("h1_spreads", [])
                h1c_xg = h1c_pred.get("xg", 4.7)

                if h1c_totals_odds or h1c_lines or h1c_spreads:
                    current_msg += f"  \u2691 G\u00f3c hi\u1ec7p 1 (xC: {h1c_xg}):\n"

                    h1c_shown = False
                    h1_odds_lines = [l for l in h1c_totals_odds.keys() if h1c_totals_odds[l].get("over_price")]
                    if h1_odds_lines:
                        line = min(h1_odds_lines, key=lambda x: abs(x - h1c_xg))
                        cl = h1c_lines.get(line, {})
                        co = h1c_totals_odds.get(line, {})
                        o_prob = cl.get("over", 0)
                        u_prob = cl.get("under", 0)
                        o_price = f" @{co['over_price']:.2f}" if co.get("over_price") else ""
                        u_price = f" @{co['under_price']:.2f}" if co.get("under_price") else ""
                        current_msg += f"    T\u00e0i/X\u1ec9u {line}: T\u00e0i {o_prob*100:.0f}%{o_price} | X\u1ec9u {u_prob*100:.0f}%{u_price}\n"
                        h1c_shown = True

                        if co.get("over_price") and o_prob > 0:
                            ev_co = o_prob * co["over_price"] - 1
                            all_values.append({"outcome": f"Góc H1 Tài {line}", "market": "Góc hiệp 1", "odds": co["over_price"], "ev": ev_co, "bk": co["over_bk"], "prob": o_prob})
                        if co.get("under_price") and u_prob > 0:
                            ev_cu = u_prob * co["under_price"] - 1
                            all_values.append({"outcome": f"Góc H1 Xỉu {line}", "market": "Góc hiệp 1", "odds": co["under_price"], "ev": ev_cu, "bk": co["under_bk"], "prob": u_prob})

                    # H1 corner AH (main line only)
                    if h1c_spreads:
                        cs = h1c_spreads[0]
                        hp = cs["home_point"]
                        ap = cs["away_point"]
                        hp_str = f"{hp:+g}"
                        ap_str = f"{ap:+g}"
                        pair_home_is_match_home = _is_home_team(cs["home_name"], m.home_team)
                        if pair_home_is_match_home:
                            model_key = f"{hp:+g}" if hp != 0 else "0"
                            ah_p = h1c_ah_pred.get(model_key, {})
                            h_prob = ah_p.get("home", 0)
                            a_prob = ah_p.get("away", 0)
                        else:
                            model_key = f"{ap:+g}" if ap != 0 else "0"
                            ah_p = h1c_ah_pred.get(model_key, {})
                            h_prob = ah_p.get("away", 0)
                            a_prob = ah_p.get("home", 0)
                        current_msg += (
                            f"    Ch\u00e2u \u00c1: {cs['home_name'][:10]} {hp_str} "
                            f"{h_prob*100:.0f}% @{cs['home_price']:.2f} | "
                            f"{cs['away_name'][:10]} {ap_str} "
                            f"{a_prob*100:.0f}% @{cs['away_price']:.2f} ({cs['bk']})\n"
                        )
                        if h_prob > 0:
                            ev_ch = h_prob * cs["home_price"] - 1
                            all_values.append({"outcome": f"Góc H1 {cs['home_name'][:10]} {hp_str}", "market": "Góc H1 Châu Á", "odds": cs["home_price"], "ev": ev_ch, "bk": cs["bk"], "prob": h_prob})
                        if a_prob > 0:
                            ev_ca = a_prob * cs["away_price"] - 1
                            all_values.append({"outcome": f"Góc H1 {cs['away_name'][:10]} {ap_str}", "market": "Góc H1 Châu Á", "odds": cs["away_price"], "ev": ev_ca, "bk": cs["bk"], "prob": a_prob})
                    if not h1c_shown and not h1c_spreads:
                        current_msg += f"    Chưa có dữ liệu\n"

                # === VALUE BETS ===
                for outcome, prob in h.items():
                    info = best_h2h.get(outcome)
                    if isinstance(info, dict) and "price" in info:
                        ev_val = prob * info["price"] - 1
                        if ev_val > 0:
                            all_values.append({"outcome": outcome, "market": "1X2", "odds": info["price"], "ev": ev_val, "bk": info["bookmaker"], "prob": prob})

                for outcome_key, vn_label in [("Over", "T\u00e0i"), ("Under", "X\u1ec9u")]:
                    info = best_totals.get(outcome_key)
                    if isinstance(info, dict) and "price" in info:
                        actual_point = info.get("point", 2.5)
                        pred_key = f"{outcome_key} {actual_point}"
                        prob = t.get(pred_key, t.get(f"{outcome_key} 2.5", 0))
                        ev_val = prob * info["price"] - 1
                        if ev_val > 0:
                            all_values.append({"outcome": f"{vn_label} {actual_point}", "market": "T\u00e0i/X\u1ec9u", "odds": info["price"], "ev": ev_val, "bk": info.get("bookmaker", "?"), "prob": prob})

                if spread_pairs:
                    pair = spread_pairs[0]
                    probs = _get_pair_probs(pair, ah, m.home_team)
                    h_prob = probs["pair_home_prob"]
                    a_prob = probs["pair_away_prob"]

                    ev_h = h_prob * pair["home_price"] - 1
                    if ev_h > 0:
                        hp_str = f"{pair['home_point']:+g}" if pair['home_point'] != 0 else "0"
                        all_values.append({"outcome": f"{pair['home_name']} {hp_str}", "market": "Ch\u00e2u \u00c1", "odds": pair["home_price"], "ev": ev_h, "bk": pair["bookmaker"], "prob": h_prob})

                    ev_a = a_prob * pair["away_price"] - 1
                    if ev_a > 0:
                        ap_str = f"{pair['away_point']:+g}" if pair["away_point"] != 0 else "0"
                        all_values.append({"outcome": f"{pair['away_name']} {ap_str}", "market": "Ch\u00e2u \u00c1", "odds": pair["away_price"], "ev": ev_a, "bk": pair["bookmaker"], "prob": a_prob})

                # === TRAP BETS ===
                for outcome, prob in h.items():
                    info = best_h2h.get(outcome)
                    if isinstance(info, dict) and "price" in info:
                        ev_val = prob * info["price"] - 1
                        if ev_val < -0.10 and info["price"] <= 2.5:
                            avoid_picks.append({
                                "outcome": outcome, "market": "1X2",
                                "odds": info["price"], "ev": ev_val,
                                "prob": prob, "bk": info["bookmaker"],
                                "home": m.home_team, "away": m.away_team,
                                "time": time_str, "league": league_name,
                            })

                for outcome_key, vn_label in [("Over", "T\u00e0i"), ("Under", "X\u1ec9u")]:
                    info = best_totals.get(outcome_key)
                    if isinstance(info, dict) and "price" in info:
                        actual_point = info.get("point", 2.5)
                        pred_key = f"{outcome_key} {actual_point}"
                        prob = t.get(pred_key, t.get(f"{outcome_key} 2.5", 0))
                        ev_val = prob * info["price"] - 1
                        if ev_val < -0.10:
                            avoid_picks.append({
                                "outcome": f"{vn_label} {actual_point}", "market": "T\u00e0i/X\u1ec9u",
                                "odds": info["price"], "ev": ev_val,
                                "prob": prob, "bk": info.get("bookmaker", "?"),
                                "home": m.home_team, "away": m.away_team,
                                "time": time_str, "league": league_name,
                            })

                for pair in spread_pairs[:1]:
                    trap_probs = _get_pair_probs(pair, ah, m.home_team)
                    for side, sp, price_key, point_key, name_key in [
                        ("home", trap_probs["pair_home_prob"], "home_price", "home_point", "home_name"),
                        ("away", trap_probs["pair_away_prob"], "away_price", "away_point", "away_name"),
                    ]:
                        ev_val = sp * pair[price_key] - 1
                        if ev_val < -0.10:
                            pt = pair[point_key]
                            pt_str = f"{pt:+g}" if pt != 0 else "0"
                            avoid_picks.append({
                                "outcome": f"{pair[name_key]} {pt_str}", "market": "Ch\u00e2u \u00c1",
                                "odds": pair[price_key], "ev": ev_val,
                                "prob": sp, "bk": pair["bookmaker"],
                                "home": m.home_team, "away": m.away_team,
                                "time": time_str, "league": league_name,
                            })

                all_values.sort(key=lambda x: x["ev"], reverse=True)

                # Collect best pick per market for parlay generation
                match_key = f"{m.home_team}__{m.away_team}"
                _parlay_seen_markets = set()
                for v in all_values:
                    mk = v["market"]
                    if mk not in _parlay_seen_markets:
                        _parlay_seen_markets.add(mk)
                        parlay_picks.append({
                            **v,
                            "home": m.home_team, "away": m.away_team,
                            "match_key": match_key,
                            "time": time_str, "league": league_name,
                        })

                if all_values:
                    market_groups = {}
                    for v in all_values:
                        cat = v["market"]
                        if cat not in market_groups:
                            market_groups[cat] = v

                    shown_best = list(market_groups.values())
                    for i, best in enumerate(shown_best):
                        conf = get_confidence_tier(best["ev"], best["prob"])
                        conf_emoji = {"HIGH": "\U0001f534", "MEDIUM": "\U0001f7e1", "LOW": "\U0001f7e2"}.get(conf, "\u26aa")
                        label = "BEST" if i == 0 else f"#{i+1}"
                        current_msg += (
                            f"  \U0001f4b0 {label}: {best['outcome']} ({best['market']}) "
                            f"@ {best['odds']:.2f} ({best['bk']})\n"
                            f"     EV: {best['ev']*100:+.1f}% | Prob: {best['prob']*100:.0f}% {conf_emoji}\n"
                        )

                    CORNER_MARKETS = {"Phạt góc", "Góc Châu Á", "Góc hiệp 1", "Góc H1 Châu Á"}
                    for best in shown_best:
                        conf = get_confidence_tier(best["ev"], best["prob"])
                        is_corner = best["market"] in CORNER_MARKETS
                        if conf in ("HIGH", "MEDIUM") or is_corner:
                            top_picks.append({
                                **best,
                                "home": m.home_team,
                                "away": m.away_team,
                                "time": time_str,
                                "confidence": conf if conf != "SKIP" else "LOW",
                                "league": league_name,
                            })

                    # SAFE PICKS — best pick per major market type with Prob >= 70%
                    # Order: Châu Á, Châu Âu (1X2), Tài/Xỉu, Góc Châu Á, Phạt góc OU
                    # Only main line (no alternates): for "Phạt góc" pick line closest to xC
                    SAFE_MARKETS = ["Châu Á", "1X2", "Tài/Xỉu", "Góc Châu Á", "Phạt góc"]
                    best_per_market = {}
                    for v in all_values:
                        if v.get("prob", 0) < 0.70 or v.get("ev", 0) <= 0:
                            continue
                        mk = v.get("market")
                        if mk not in SAFE_MARKETS:
                            continue
                        # For corner OU, only consider lines close to model xC (main line)
                        if mk == "Phạt góc":
                            line_val = v.get("line")
                            if line_val is None or abs(line_val - corner_xg) > 1.0:
                                continue
                        cur = best_per_market.get(mk)
                        if cur is None or (v["prob"], v["ev"]) > (cur["prob"], cur["ev"]):
                            best_per_market[mk] = v
                    for mk in SAFE_MARKETS:
                        if mk in best_per_market:
                            v = best_per_market[mk]
                            safe_picks.append({
                                **v,
                                "home": m.home_team,
                                "away": m.away_team,
                                "time": time_str,
                                "league": league_name,
                            })
                elif not odds_event:
                    current_msg += f"  \u2753 Ch\u01b0a c\u00f3 odds\n"
                else:
                    current_msg += f"  \u274c Kh\u00f4ng t\u00ecm th\u1ea5y value bet\n"
                current_msg += f"\u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518\n"

            if messages and len(messages[-1]) + len(current_msg) < 3900:
                messages[-1] += current_msg
            else:
                messages.append(current_msg)

        # If collect_only, return picks without sending messages
        if collect_only:
            parlay_picks, _n_par_skip = _filter_suspicious_picks(parlay_picks, "phantich/parlay")
            if _n_par_skip:
                logger.info(f"[phantich/parlay] Filter đã loại {_n_par_skip} kèo EV ảo.")
            return parlay_picks

        # Send header
        title = "PH\u00c2N T\u00cdCH 24H T\u1edaI" if not league_codes else "PH\u00c2N T\u00cdCH 24H"
        header = (
            f"\U0001f4ca {title} \u2014 {total_analyzed} tr\u1eadn ({len(by_league)} gi\u1ea3i)\n"
            f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
            f"Model: Poisson | Data: {total_hist} tr\u1eadn l\u1ecbch s\u1eed\n"
        )

        all_text = header
        for msg in messages:
            all_text += msg

        await _safe_reply(update, all_text)

        # Safety filter: loại kèo EV ảo trước khi format TOP / SAFE / PARLAY.
        top_picks, _n_top_skip = _filter_suspicious_picks(top_picks, "phantich/top")
        safe_picks, _n_safe_skip = _filter_suspicious_picks(safe_picks, "phantich/safe")
        parlay_picks, _n_par_skip = _filter_suspicious_picks(parlay_picks, "phantich/parlay")
        _filtered_total = _n_top_skip + _n_safe_skip + _n_par_skip
        if _filtered_total:
            logger.info(
                f"[phantich] Filter đã loại {_filtered_total} kèo EV ảo "
                f"(top={_n_top_skip}, safe={_n_safe_skip}, parlay={_n_par_skip})."
            )

        # TOP PICKS
        if top_picks:
            tier_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
            top_picks.sort(key=lambda x: (tier_order.get(x["confidence"], 9), -x["ev"]))

            match_picks = {}
            for pick in top_picks:
                match_key = f"{pick['home']}__{pick['away']}"
                if match_key not in match_picks:
                    match_picks[match_key] = []
                match_picks[match_key].append(pick)

            summary = (
                f"\n\U0001f3c6 TOP PICKS \u2014 K\u00c8O GI\u00c1 TR\u1eca CAO\n"
                f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
            )

            conf_emojis = {"HIGH": "\U0001f534", "MEDIUM": "\U0001f7e1", "LOW": "\U0001f7e2"}
            conf_labels = {"HIGH": "CAO", "MEDIUM": "TB", "LOW": "TH\u1ea4P"}

            for match_key, picks in match_picks.items():
                best_conf = picks[0]["confidence"]
                emoji = conf_emojis.get(best_conf, "\u26aa")
                label = conf_labels.get(best_conf, "?")
                p0 = picks[0]
                summary += (
                    f"\n{emoji} [{label}] {p0['home']} vs {p0['away']}\n"
                    f"  \U0001f552 {p0['time']} | {p0['league']}\n"
                )
                for pick in picks:
                    summary += (
                        f"  ➤ {pick['outcome']} ({pick['market']}) @ {pick['odds']:.2f}\n"
                        f"    Prob: {pick['prob']*100:.0f}% | EV: {pick['ev']*100:+.1f}% | {pick['bk']}\n"
                    )

            summary += (
                f"\n\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
                f"\U0001f534 CAO: EV>8%, Prob>80%\n"
                f"\U0001f7e1 TB: EV>4%, Prob>65%\n"
                f"\u2691 Góc: Luôn hiển thị (cả trận + H1)\n"
            )
            if _filtered_total:
                summary += f"\U0001f6ab Đã lọc {_filtered_total} kèo EV ảo\n"

            await _safe_reply(update, summary)
        else:
            msg = "\u26a0\ufe0f Kh\u00f4ng t\u00ecm th\u1ea5y k\u00e8o N\u00caN \u0110\u00c1NH trong 24h t\u1edbi."
            if _filtered_total:
                msg += f"\n\U0001f6ab Đã lọc {_filtered_total} kèo EV ảo"
            await update.message.reply_text(msg)

        # AVOID
        if avoid_picks:
            avoid_picks.sort(key=lambda x: x["ev"])
            avoid_msg = (
                f"\n\u26d4 C\u1ea2NH B\u00c1O \u2014 K\u00c8O N\u00caN TR\u00c1NH\n"
                f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
                f"Odds h\u1ea5p d\u1eabn nh\u01b0ng model cho x\u00e1c su\u1ea5t th\u1ea5p\n"
            )
            seen_matches = set()
            for pick in avoid_picks:
                match_key = f"{pick['home']}_{pick['away']}_{pick['market']}"
                if match_key in seen_matches:
                    continue
                seen_matches.add(match_key)
                avoid_msg += (
                    f"\n\U0001f6ab {pick['home']} vs {pick['away']}\n"
                    f"  \U0001f552 {pick['time']} | {pick['league']}\n"
                    f"  \u2717 {pick['outcome']} ({pick['market']}) @ {pick['odds']:.2f}\n"
                    f"  \u26a0\ufe0f Prob ch\u1ec9 {pick['prob']*100:.0f}% | EV: {pick['ev']*100:.1f}% | {pick['bk']}\n"
                )
            avoid_msg += f"\n\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
            avoid_msg += f"\u26a0\ufe0f EV \u00e2m > 10% = nh\u00e0 c\u00e1i l\u1eddi, b\u1ea1n l\u1ed7\n"
            await _safe_reply(update, avoid_msg)

        # SAFE PICKS — Prob >= 70% (high-confidence shortlist)
        if safe_picks:
            # Group by match preserving insertion order; picks within a match
            # already follow SAFE_MARKETS order from collection
            safe_by_match = {}
            for p in safe_picks:
                mk = f"{p['home']}__{p['away']}"
                if mk not in safe_by_match:
                    safe_by_match[mk] = []
                safe_by_match[mk].append(p)

            # Sort matches by their best pick (highest prob, then EV)
            sorted_matches = sorted(
                safe_by_match.items(),
                key=lambda kv: (-max(p["prob"] for p in kv[1]), -max(p["ev"] for p in kv[1])),
            )

            safe_msg = (
                f"\n\U0001f31f K\u00c8O \u0102N CH\u1eaeC \u2014 PROB \u2265 70%\n"
                f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
                f"M\u1ed7i market ch\u1ec9 l\u1ea5y k\u00e8o ch\u00ednh t\u1ed1t nh\u1ea5t\n"
            )
            for mk, picks in sorted_matches:
                p0 = picks[0]
                safe_msg += (
                    f"\n\U0001f31f {p0['home']} vs {p0['away']}\n"
                    f"  \U0001f552 {p0['time']} | {p0['league']}\n"
                )
                for pick in picks:
                    safe_msg += (
                        f"  \u2794 {pick['outcome']} ({pick['market']}) @ {pick['odds']:.2f}\n"
                        f"    Prob: {pick['prob']*100:.0f}% | EV: {pick['ev']*100:+.1f}% | {pick['bk']}\n"
                    )
            safe_msg += (
                f"\n\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
                f"\U0001f4a1 Ch\u1ec9 li\u1ec7t k\u00ea k\u00e8o c\u00f3 model prob \u2265 70% v\u00e0 EV > 0\n"
            )
            await _safe_reply(update, safe_msg)

    except Exception as e:
        logger.error(f"[Analyze] Error: {e}", exc_info=True)
        if collect_only:
            return []
        await update.message.reply_text(f"\u274c L\u1ed7i ph\u00e2n t\u00edch: {e}")
    finally:
        session.close()


async def cmd_keoxien(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Generate parlay (accumulator) bets from all value picks in 24h."""
    if not await _require_auth(update): return
    from itertools import combinations
    from src.models.poisson import get_confidence_tier

    await update.message.reply_text("\u23f3 \u0110ang ph\u00e2n t\u00edch v\u00e0 gh\u00e9p k\u00e8o xi\u00ean...")

    # Collect all value picks from full analysis
    all_picks = await _run_full_analysis(update, league_codes=None, collect_only=True)

    if not all_picks:
        await update.message.reply_text("\U0001f4ad Kh\u00f4ng c\u00f3 k\u00e8o gi\u00e1 tr\u1ecb n\u00e0o \u0111\u1ec3 gh\u00e9p xi\u00ean. Th\u1eed l\u1ea1i khi c\u00f3 tr\u1eadn \u0111\u1ea5u.")
        return

    # Keep only best pick per match (highest EV), then sort
    best_per_match = {}
    for p in all_picks:
        mk = p["match_key"]
        if mk not in best_per_match or p["ev"] > best_per_match[mk]["ev"]:
            best_per_match[mk] = p
    picks = sorted(best_per_match.values(), key=lambda x: x["ev"], reverse=True)

    # Cap at top 20 picks for combinatorial sanity
    picks = picks[:20]

    if len(picks) < 2:
        await update.message.reply_text("\u26a0\ufe0f C\u1ea7n \u00edt nh\u1ea5t 2 tr\u1eadn kh\u00e1c nhau \u0111\u1ec3 gh\u00e9p xi\u00ean.")
        return

    # Generate parlays for sizes 2..min(10, len(picks))
    max_size = min(10, len(picks))
    conf_emojis = {"HIGH": "\U0001f534", "MEDIUM": "\U0001f7e1", "LOW": "\U0001f7e2"}

    all_msg = (
        f"\U0001f3af K\u00c8O XI\u00caN \u2014 PARLAY\n"
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        f"D\u1ef1a tr\u00ean {len(picks)} k\u00e8o gi\u00e1 tr\u1ecb t\u1eeb {len(best_per_match)} tr\u1eadn\n\n"
    )

    for size in range(2, max_size + 1):
        combos = list(combinations(range(len(picks)), size))

        # Score each combo: combined_prob * combined_odds - 1
        scored = []
        for combo in combos:
            combo_picks = [picks[i] for i in combo]
            # Ensure all matches are different (already guaranteed by best_per_match)
            combined_odds = 1.0
            combined_prob = 1.0
            for cp in combo_picks:
                combined_odds *= cp["odds"]
                combined_prob *= cp["prob"]
            combined_ev = combined_prob * combined_odds - 1
            if combined_ev > 0:
                scored.append({
                    "picks": combo_picks,
                    "odds": combined_odds,
                    "prob": combined_prob,
                    "ev": combined_ev,
                })

        if not scored:
            continue

        # Sort by EV desc, show top 3
        scored.sort(key=lambda x: x["ev"], reverse=True)
        top_n = scored[:3]

        all_msg += f"\U0001f4cc XI\u00caN {size} (Top {len(top_n)}/{len(scored)} c\u00f3 EV+)\n"
        all_msg += f"\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n"

        for idx, parlay in enumerate(top_n, 1):
            conf = get_confidence_tier(parlay["ev"], parlay["prob"])
            emoji = conf_emojis.get(conf, "\u26aa")
            all_msg += (
                f"\n{emoji} Xi\u00ean #{idx} \u2014 Odds: {parlay['odds']:.2f} | "
                f"Prob: {parlay['prob']*100:.1f}% | EV: {parlay['ev']*100:+.1f}%\n"
            )
            for cp in parlay["picks"]:
                all_msg += (
                    f"  \u26bd {cp['home']} vs {cp['away']}\n"
                    f"    \u27a4 {cp['outcome']} ({cp['market']}) @{cp['odds']:.2f} "
                    f"| Prob: {cp['prob']*100:.0f}% | {cp['league']}\n"
                )
        all_msg += "\n"

    if all_msg.count("\U0001f4cc") == 0:
        await update.message.reply_text("\u26a0\ufe0f Kh\u00f4ng t\u00ecm th\u1ea5y k\u00e8o xi\u00ean c\u00f3 EV d\u01b0\u01a1ng.")
        return

    # Footer
    all_msg += (
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        f"\U0001f534 Odds = t\u00edch c\u00e1c odds \u0111\u01a1n\n"
        f"\U0001f7e1 Prob = x\u00e1c su\u1ea5t k\u1ebft h\u1ee3p (model)\n"
        f"\U0001f7e2 EV = Prob \u00d7 Odds \u2212 1\n"
        f"\u26a0\ufe0f Xi\u00ean c\u00e0ng d\u00e0i, r\u1ee7i ro c\u00e0ng cao!\n"
    )

    await _safe_reply(update, all_msg)


async def cmd_leagues(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _require_auth(update): return
    from src.config import LEAGUES, LEAGUE_REGIONS
    msg = "\U0001f3c6 GI\u1ea2I \u0110\u1ea4U H\u1ed6 TR\u1ee2\n\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
    msg += "D\u00f9ng m\u00e3 gi\u1ea3i v\u1edbi /phantich, /tatca, /live\n"
    msg += "VD: /phantich PL, /live BL1\n\n"
    for region, codes in LEAGUE_REGIONS.items():
        msg += f"{region}\n"
        for code in codes:
            name = LEAGUES.get(code, code)
            msg += f"  {code} \u2014 {name}\n"
        msg += "\n"
    await _safe_reply(update, msg)


async def cmd_quota(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show current API quota status for all APIs."""
    if not await _require_auth(update): return
    from src.collectors.odds_api import get_quota
    from src.collectors.api_football import get_af_quota

    # --- The Odds API ---
    q = get_quota()
    remaining = q.get("remaining")
    used = q.get("used")

    msg = ""
    if remaining is not None:
        total = (remaining + used) if used is not None else "?"
        pct = (remaining / (remaining + used) * 100) if used is not None and (remaining + used) > 0 else 0

        if remaining <= 0:
            status = "\U0001f534 H\u1ebeT QUOTA!"
        elif remaining <= 50:
            status = "\U0001f7e1 S\u1eaeP H\u1ebeT!"
        else:
            status = "\U0001f7e2 OK"

        msg += (
            f"\U0001f4ca THE ODDS API\n"
            f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
            f"Tr\u1ea1ng th\u00e1i: {status}\n"
            f"C\u00f2n l\u1ea1i: {remaining} / {total} requests\n"
            f"\u0110\u00e3 d\u00f9ng: {used or '?'} requests\n"
            f"T\u1ec9 l\u1ec7 c\u00f2n: {pct:.1f}%\n"
            f"\U0001f4a1 Gia h\u1ea1n: https://the-odds-api.com\n"
        )
    else:
        msg += "\U0001f4ca THE ODDS API\n\u23f3 Ch\u01b0a c\u00f3 d\u1eef li\u1ec7u. Ch\u1ea1y /phantich ho\u1eb7c /live tr\u01b0\u1edbc.\n"

    # --- API-Football ---
    af = get_af_quota()
    af_remaining = af.get("current")
    af_limit = af.get("limit")

    msg += "\n"
    if af_remaining is not None:
        af_used = (af_limit - af_remaining) if af_limit else "?"
        af_pct = (af_remaining / af_limit * 100) if af_limit and af_limit > 0 else 0

        if af_remaining <= 0:
            af_status = "\U0001f534 H\u1ebeT QUOTA!"
        elif af_remaining <= 20:
            af_status = "\U0001f7e1 S\u1eaeP H\u1ebeT!"
        else:
            af_status = "\U0001f7e2 OK"

        msg += (
            f"\u26bd API-FOOTBALL\n"
            f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
            f"Tr\u1ea1ng th\u00e1i: {af_status}\n"
            f"C\u00f2n l\u1ea1i: {af_remaining} / {af_limit} requests/ng\u00e0y\n"
            f"\u0110\u00e3 d\u00f9ng: {af_used} requests\n"
            f"T\u1ec9 l\u1ec7 c\u00f2n: {af_pct:.1f}%\n"
            f"\U0001f4a1 Gia h\u1ea1n: https://dashboard.api-football.com\n"
        )
    else:
        msg += "\u26bd API-FOOTBALL\n\u23f3 Ch\u01b0a c\u00f3 d\u1eef li\u1ec7u. Ch\u1ea1y /live tr\u01b0\u1edbc.\n"

    await _safe_reply(update, msg)


def _analyze_live(hs: dict, as_: dict, minute: int, home_score: int, away_score: int, pred: dict, parsed_events: dict = None, events: list = None) -> dict:
    """
    Full live analysis with 7 systems:
    1. Red cards  2. Substitutions  3. Half-time  4. Game state
    5. Momentum  6. Corners clustering  7. Base stats
    Returns: {
        momentum: "home"/"away"/"balanced",
        pressure: float (0-100, how dominant one side is),
        insights: [str],
        adj_h2h: {Home, Draw, Away},  # adjusted probs
        goals_trend: "high"/"low"/"normal",
        corners_trend: "high"/"low"/"normal",
        corners_pace: float (projected total corners for 90min),
    }
    """
    remaining = max(90 - minute, 1)
    played_ratio = minute / 90.0 if minute > 0 else 0.01

    # === MOMENTUM from stats ===
    h_poss = int(str(hs.get("possession", "50")).replace("%", "") or 50)
    a_poss = int(str(as_.get("possession", "50")).replace("%", "") or 50)
    h_shots = hs.get("shots", 0)
    a_shots = as_.get("shots", 0)
    h_shots_on = hs.get("shots_on", 0)
    a_shots_on = as_.get("shots_on", 0)
    h_inside = hs.get("shots_insidebox", 0)
    a_inside = as_.get("shots_insidebox", 0)
    h_corners = hs.get("corners", 0)
    a_corners = as_.get("corners", 0)
    h_saves = hs.get("saves", 0)
    a_saves = as_.get("saves", 0)

    # xG live
    try:
        h_xg = float(hs.get("expected_goals", 0) or 0)
        a_xg = float(as_.get("expected_goals", 0) or 0)
    except (TypeError, ValueError):
        h_xg, a_xg = 0.0, 0.0

    # Pressure score (weighted composite)
    total_shots = h_shots + a_shots or 1
    total_inside = h_inside + a_inside or 1
    h_pressure = (
        (h_poss / 100) * 25 +
        (h_shots / total_shots) * 25 +
        (h_shots_on / max(h_shots_on + a_shots_on, 1)) * 25 +
        (h_inside / total_inside) * 25
    )

    if h_pressure > 62:
        momentum = "home"
    elif h_pressure < 38:
        momentum = "away"
    else:
        momentum = "balanced"

    # === ADJUSTED H2H from live stats ===
    base_h = pred["h2h"]
    adj = dict(base_h)

    if minute >= 10:
        # Shift based on momentum
        shift = (h_pressure - 50) / 100 * 0.3  # max ±15% shift
        adj["Home"] = min(0.95, max(0.02, base_h["Home"] + shift))
        adj["Away"] = min(0.95, max(0.02, base_h["Away"] - shift))
        adj["Draw"] = max(0.02, 1 - adj["Home"] - adj["Away"])

        # Score impact: leading team gets boost, trailing gets reduced
        goal_diff = home_score - away_score
        if goal_diff > 0 and minute > 30:
            time_factor = min(remaining / 60, 1)
            adj["Home"] += 0.05 * goal_diff * (1 - time_factor)
            adj["Away"] = max(0.02, adj["Away"] - 0.05 * goal_diff * (1 - time_factor))
        elif goal_diff < 0 and minute > 30:
            time_factor = min(remaining / 60, 1)
            adj["Away"] += 0.05 * abs(goal_diff) * (1 - time_factor)
            adj["Home"] = max(0.02, adj["Home"] - 0.05 * abs(goal_diff) * (1 - time_factor))

        # Normalize
        total_p = adj["Home"] + adj["Draw"] + adj["Away"]
        adj = {k: round(v / total_p, 4) for k, v in adj.items()}

    # === GOALS TREND ===
    total_goals = home_score + away_score
    total_xg = h_xg + a_xg
    goals_per_min = total_goals / max(minute, 1)
    xg_per_min = total_xg / max(minute, 1)
    proj_goals = goals_per_min * 90
    proj_xg = xg_per_min * 90

    if proj_xg > 3.5 or (total_xg > 2.0 and minute < 60):
        goals_trend = "high"
    elif proj_xg < 1.5 and minute > 30:
        goals_trend = "low"
    else:
        goals_trend = "normal"

    # === CORNERS TREND ===
    total_corners = h_corners + a_corners
    corners_per_min = total_corners / max(minute, 1)
    corners_pace = round(corners_per_min * 90, 1)

    if corners_pace > 12 or (total_corners > 6 and minute < 45):
        corners_trend = "high"
    elif corners_pace < 8 and minute > 30:
        corners_trend = "low"
    else:
        corners_trend = "normal"

    # === 1. RED CARD DETECTION ===
    pe = parsed_events or {}
    home_reds = pe.get("home_reds", 0)
    away_reds = pe.get("away_reds", 0)
    has_red = home_reds > 0 or away_reds > 0

    if has_red:
        # Red card shifts xG ~25-30%, invalidates pre-match projections
        red_shift = 0.25
        if home_reds > away_reds:
            # Home team has more reds → shift toward away
            adj["Home"] = max(0.02, adj["Home"] * (1 - red_shift * home_reds))
            adj["Away"] = min(0.95, adj["Away"] * (1 + red_shift * home_reds * 0.8))
        elif away_reds > home_reds:
            adj["Away"] = max(0.02, adj["Away"] * (1 - red_shift * away_reds))
            adj["Home"] = min(0.95, adj["Home"] * (1 + red_shift * away_reds * 0.8))
        adj["Draw"] = max(0.02, 1 - adj["Home"] - adj["Away"])
        # Re-normalize
        total_p = adj["Home"] + adj["Draw"] + adj["Away"]
        adj = {k: round(v / total_p, 4) for k, v in adj.items()}

    # === 2. SUBSTITUTION INTENT ===
    sub_intent = pe.get("sub_intent", "neutral")
    home_subs = pe.get("home_subs", 0)
    away_subs = pe.get("away_subs", 0)
    last_sub_min = pe.get("last_sub_minute", 0)

    # === 3. HALF-TIME ANALYSIS ===
    is_ht_window = 43 <= minute <= 52
    ht_insight = None
    if is_ht_window and (h_xg > 0 or a_xg > 0):
        # Compare xG vs actual score at HT
        xg_diff_home = h_xg - home_score
        xg_diff_away = a_xg - away_score
        if xg_diff_home > 0.7:
            ht_insight = f"Ch\u1ee7 nh\u00e0 xG {h_xg:.1f} vs {home_score} b\u00e0n \u2192 \u0111ang ch\u01a1i t\u1ed1t h\u01a1n t\u1ec9 s\u1ed1"
        elif xg_diff_away > 0.7:
            ht_insight = f"Kh\u00e1ch xG {a_xg:.1f} vs {away_score} b\u00e0n \u2192 \u0111ang ch\u01a1i t\u1ed1t h\u01a1n t\u1ec9 s\u1ed1"
        elif total_xg < 0.5 and total_goals == 0:
            ht_insight = "HT xG th\u1ea5p, tr\u1eadn \u0111\u1ea5u b\u1ebf t\u1eafc \u2192 X\u1ec8U m\u1ea1nh"

    # === 4. GAME STATE SEGMENTATION ===
    goal_diff = home_score - away_score
    if goal_diff > 0:
        game_state = "home_leading"
    elif goal_diff < 0:
        game_state = "away_leading"
    else:
        game_state = "drawing"

    if minute <= 30:
        time_phase = "early"
    elif minute <= 65:
        time_phase = "mid"
    else:
        time_phase = "late"

    # === 5. MOMENTUM ROLLING 10-MIN WINDOW ===
    recent_momentum = None
    evts = events or []
    if evts and minute > 15:
        window_start = max(0, minute - 10)
        recent_events = [e for e in evts if e.get("minute", 0) >= window_start]
        home_events = sum(1 for e in recent_events
                         if e.get("type") in ("Goal", "subst") and e.get("team_id") == hs.get("team_id", -1))
        away_events = sum(1 for e in recent_events
                         if e.get("type") in ("Goal", "subst") and e.get("team_id") == as_.get("team_id", -1))
        home_goals_recent = sum(1 for e in recent_events
                                if e.get("type") == "Goal" and e.get("team_id") == hs.get("team_id", -1))
        away_goals_recent = sum(1 for e in recent_events
                                if e.get("type") == "Goal" and e.get("team_id") == as_.get("team_id", -1))
        if home_goals_recent > away_goals_recent:
            recent_momentum = "home_surge"
        elif away_goals_recent > home_goals_recent:
            recent_momentum = "away_surge"

    # === 6. CORNERS CLUSTERING ===
    corner_cluster = None
    corner_events = [e for e in evts if "corner" in e.get("detail", "").lower() or "corner" in e.get("type", "").lower()]
    # Also detect from stats if no corner events in timeline
    if not corner_events and total_corners > 0 and minute > 20:
        # Use pace-based detection instead
        last_10_pace = total_corners / max(minute, 1) * 10
        if last_10_pace > 3:
            corner_cluster = "high_frequency"
    elif len(corner_events) >= 3:
        # Check if 3+ corners in a 10-minute window
        for i in range(len(corner_events) - 2):
            span = corner_events[i + 2].get("minute", 0) - corner_events[i].get("minute", 0)
            if 0 < span <= 10:
                corner_cluster = "burst"
                break

    # === 7. INSIGHTS (combining all systems) ===
    insights = []

    # --- Red card alerts (highest priority) ---
    if has_red:
        for rc in pe.get("red_cards", []):
            side = "Ch\u1ee7 nh\u00e0" if rc.get("is_home") else "Kh\u00e1ch"
            insights.append(f"\U0001f7e5 TH\u1eba \u0110\u1ece: {rc['player']} ({side}) ph\u00fat {rc['minute']}' \u2192 thay \u0111\u1ed5i c\u1ee5c di\u1ec7n!")
        if home_reds > 0 and minute < 70:
            insights.append(f"\u26a0 Ch\u1ee7 nh\u00e0 {10 - home_reds} ng\u01b0\u1eddi \u2192 kh\u00e1ch c\u00f3 l\u1ee3i th\u1ebf, xem x\u00e9t T\u00c0I")
        elif away_reds > 0 and minute < 70:
            insights.append(f"\u26a0 Kh\u00e1ch {10 - away_reds} ng\u01b0\u1eddi \u2192 ch\u1ee7 nh\u00e0 c\u00f3 l\u1ee3i th\u1ebf, xem x\u00e9t T\u00c0I")

    # --- Substitution intent ---
    if sub_intent == "attacking" and last_sub_min > 0:
        insights.append(f"\U0001f504 Thay ng\u01b0\u1eddi t\u1ea5n c\u00f4ng (ph\u00fat {last_sub_min}') \u2192 \u0111\u1ed9i h\u00ecnh d\u1ed3n l\u1ef1c, T\u00c0I c\u00f3 th\u1ec3")
    if home_subs >= 4 or away_subs >= 4:
        side = "Ch\u1ee7 nh\u00e0" if home_subs >= 4 else "Kh\u00e1ch"
        insights.append(f"\U0001f504 {side} \u0111\u00e3 d\u00f9ng {max(home_subs, away_subs)}/5 l\u01b0\u1ee3t thay \u2192 gi\u1ea3m l\u1ef1c cu\u1ed1i tr\u1eadn")

    # --- Half-time analysis ---
    if ht_insight:
        insights.append(f"\u23f1 HT: {ht_insight}")

    # --- Game state + time phase insights ---
    if game_state == "drawing" and time_phase == "late" and total_goals == 0:
        insights.append(f"\u23f0 Ph\u00fat {minute}, 0-0 \u2192 X\u1ec8U r\u1ea5t m\u1ea1nh")
    elif game_state == "drawing" and time_phase == "late" and total_goals >= 2:
        insights.append(f"\u23f0 H\u00f2a {home_score}-{away_score} cu\u1ed1i tr\u1eadn \u2192 c\u1ea3 2 \u0111\u1ed9i d\u1ed3n l\u1ef1c, T\u00c0I c\u00f3 th\u1ec3")
    elif game_state == "home_leading" and time_phase == "late" and abs(goal_diff) == 1:
        if h_pressure < 45:
            insights.append(f"\u23f0 Kh\u00e1ch \u00e1p \u0111\u1ea3o d\u00f9 thua {away_score}-{home_score} \u2192 c\u00f3 th\u1ec3 g\u1ee1")
    elif game_state == "away_leading" and time_phase == "late" and abs(goal_diff) == 1:
        if h_pressure > 55:
            insights.append(f"\u23f0 Ch\u1ee7 nh\u00e0 \u00e1p \u0111\u1ea3o d\u00f9 thua {home_score}-{away_score} \u2192 c\u00f3 th\u1ec3 g\u1ee1")

    if game_state != "drawing" and time_phase == "early" and abs(goal_diff) >= 2:
        leading = "Ch\u1ee7 nh\u00e0" if goal_diff > 0 else "Kh\u00e1ch"
        insights.append(f"\u26a1 {leading} d\u1eabn {abs(goal_diff)} b\u00e0n s\u1edbm \u2192 T\u00c0I r\u1ea5t m\u1ea1nh (c\u00f2n nhi\u1ec1u th\u1eddi gian)")

    # --- Rolling momentum surge ---
    if recent_momentum == "home_surge":
        insights.append(f"\U0001f4c8 Ch\u1ee7 nh\u00e0 b\u00f9ng n\u1ed5 10' g\u1ea7n \u0111\u00e2y \u2192 momentum m\u1ea1nh")
    elif recent_momentum == "away_surge":
        insights.append(f"\U0001f4c8 Kh\u00e1ch b\u00f9ng n\u1ed5 10' g\u1ea7n \u0111\u00e2y \u2192 momentum m\u1ea1nh")

    # --- Dominance ---
    if h_pressure > 65:
        insights.append(f"\U0001f525 CH\u1ee6 NH\u00c0 \u00e1p \u0111\u1ea3o ({h_pressure:.0f}% pressure)")
    elif h_pressure < 35:
        insights.append(f"\U0001f525 KH\u00c1CH \u00e1p \u0111\u1ea3o ({100-h_pressure:.0f}% pressure)")

    # --- xG vs Score mismatch ---
    if h_xg > 0 and a_xg > 0:
        if h_xg > a_xg + 0.8 and home_score <= away_score:
            insights.append(f"\u26a0 Ch\u1ee7 nh\u00e0 xG {h_xg:.1f} nh\u01b0ng \u0111ang thua/h\u00f2a \u2192 c\u00f3 th\u1ec3 ghi b\u00e0n")
        elif a_xg > h_xg + 0.8 and away_score <= home_score:
            insights.append(f"\u26a0 Kh\u00e1ch xG {a_xg:.1f} nh\u01b0ng \u0111ang thua/h\u00f2a \u2192 c\u00f3 th\u1ec3 ghi b\u00e0n")

    # --- Shots on target pressure ---
    if h_shots_on >= 5 and a_saves >= 4 and home_score == 0:
        insights.append(f"\U0001f6a8 Ch\u1ee7 nh\u00e0 {h_shots_on} s\u00fat tr\u00fang, th\u1ee7 m\u00f4n kh\u00e1ch c\u1ee9u {a_saves} \u2192 b\u00e0n th\u1eafng s\u1eafp \u0111\u1ebfn")
    elif a_shots_on >= 5 and h_saves >= 4 and away_score == 0:
        insights.append(f"\U0001f6a8 Kh\u00e1ch {a_shots_on} s\u00fat tr\u00fang, th\u1ee7 m\u00f4n ch\u1ee7 c\u1ee9u {h_saves} \u2192 b\u00e0n th\u1eafng s\u1eafp \u0111\u1ebfn")

    # --- Goals trend ---
    if goals_trend == "high":
        insights.append(f"\u2b06 Nh\u1ecbp tr\u1eadn cao, xG pace {proj_xg:.1f}/90' \u2192 T\u00c0I")
    elif goals_trend == "low" and minute > 50 and total_goals <= 1:
        insights.append(f"\u2b07 Tr\u1eadn \u0111\u1ea5u ch\u1eadm, xG {total_xg:.1f} sau {minute}' \u2192 X\u1ec8U")

    # --- Corners trend ---
    if corners_trend == "high":
        insights.append(f"\u2691 Nh\u1ecbp g\u00f3c cao: {total_corners} g\u00f3c/{minute}' (pace {corners_pace}/90') \u2192 T\u00c0I G\u00d3C")
    elif corners_trend == "low" and minute > 40:
        insights.append(f"\u2691 \u00cdt g\u00f3c: {total_corners} g\u00f3c/{minute}' (pace {corners_pace}/90') \u2192 X\u1ec8U G\u00d3C")

    # --- Corners clustering ---
    if corner_cluster == "burst":
        insights.append(f"\u2691\u26a1 C\u1ee5m g\u00f3c: 3+ g\u00f3c trong 10 ph\u00fat \u2192 T\u00c0I G\u00d3C m\u1ea1nh")
    elif corner_cluster == "high_frequency":
        insights.append(f"\u2691 T\u1ea7n su\u1ea5t g\u00f3c cao \u2192 T\u00c0I G\u00d3C")

    # --- Corner handicap insight ---
    corner_diff = h_corners - a_corners
    if abs(corner_diff) >= 3 and minute > 25:
        leader = "Ch\u1ee7 nh\u00e0" if corner_diff > 0 else "Kh\u00e1ch"
        insights.append(f"\u2691 {leader} d\u1eabn {abs(corner_diff)} g\u00f3c \u2192 k\u00e8o g\u00f3c ch\u00e2u \u00e1")

    return {
        "momentum": momentum,
        "pressure": h_pressure,
        "insights": insights,
        "adj_h2h": adj,
        "goals_trend": goals_trend,
        "corners_trend": corners_trend,
        "corners_pace": corners_pace,
        "proj_xg": proj_xg,
        "h_xg": h_xg,
        "a_xg": a_xg,
        "has_red": has_red,
        "home_reds": home_reds,
        "away_reds": away_reds,
        "sub_intent": sub_intent,
        "home_subs": home_subs,
        "away_subs": away_subs,
        "game_state": game_state,
        "time_phase": time_phase,
        "recent_momentum": recent_momentum,
        "corner_cluster": corner_cluster,
    }


_ALL_LIVE_PAGE_SIZE = 10


async def _send_chunked(update: Update, text: str, max_len: int = 3900):
    """Split `text` on line boundaries, reply sequentially with 1s gap."""
    import asyncio
    if len(text) <= max_len:
        await update.message.reply_text(text)
        return
    parts: list[str] = []
    current = ""
    for line in text.splitlines(keepends=True):
        if current and len(current) + len(line) > max_len:
            parts.append(current)
            current = line
        else:
            current += line
    if current:
        parts.append(current)
    for i, part in enumerate(parts):
        await update.message.reply_text(part)
        if i < len(parts) - 1:
            await asyncio.sleep(1)


async def _run_all_leagues_phantich(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Run /phantich across every league in LEAGUES, with quota guard + summary.

    Quota-conscious: refuses if Odds API remaining < 10000. Reuses
    `_run_full_analysis(league_codes=None, collect_only=True)` — engine already
    loops by scheduled matches, caches per-league odds/corners, and applies
    `_is_ev_suspicious`.
    """
    import time
    from datetime import datetime, timedelta
    from src.collectors.odds_api import get_quota
    from src.config import LEAGUES

    q = get_quota()
    remaining = q.get("remaining")
    if remaining is not None and remaining < 10000:
        await update.message.reply_text(
            f"\u26a0\ufe0f Quota th\u1ea5p ({remaining} c\u00f2n). "
            f"Ph\u00e2n t\u00edch t\u1ea5t c\u1ea3 gi\u1ea3i c\u00f3 th\u1ec3 ti\u00eau 200-500 calls.\n"
            f"G\u00f5 /phantich r\u1ed3i ch\u1ecdn gi\u1ea3i c\u1ee5 th\u1ec3."
        )
        return

    user_id = update.effective_user.id if update.effective_user else "?"
    t_start = time.time()
    session_started_at = datetime.utcnow()
    logger.info(
        f"[phantich-ALL] user={user_id} start leagues={len(LEAGUES)} "
        f"remaining_quota={remaining}"
    )

    await update.message.reply_text(
        f"\u23f3 \u0110ang ph\u00e2n t\u00edch tr\u1eadn trong 24h t\u1edbi "
        f"({len(LEAGUES)} gi\u1ea3i), vui l\u00f2ng ch\u1edd 2-5 ph\u00fat..."
    )

    picks = await _run_full_analysis(update, league_codes=None, collect_only=True)
    picks = picks or []

    elapsed = time.time() - t_start
    q2 = get_quota()
    remaining2 = q2.get("remaining")
    used_calls = (
        remaining - remaining2
        if (remaining is not None and remaining2 is not None)
        else None
    )
    calls_str = str(used_calls) if used_calls is not None else "?"

    # Re-query predictions just written in this session — apply identical
    # filtering pipeline as /ancan (prob ≥ 58%, 4 anti-ảo rules, sort prob desc)
    CORNER_MARKETS = {
        "corners_totals", "corners_spreads",
        "corners_h1_totals", "corners_h1_spreads",
    }
    MKT_NAMES = {
        "h2h": "1X2",
        "totals": "T\u00e0i/X\u1ec9u",
        "spreads": "Ch\u00e2u \u00c1",
        "corners_totals": "G\u00f3c T/X",
        "corners_spreads": "G\u00f3c Ch\u00e2u \u00c1",
        "corners_h1_totals": "G\u00f3c H1 T/X",
        "corners_h1_spreads": "G\u00f3c H1 Ch\u00e2u \u00c1",
    }

    _now = datetime.utcnow()
    _win_end = _now + timedelta(hours=24)
    _s = get_session()
    try:
        # Count analyzed matches + leagues in 24h window
        scheduled_rows = (
            _s.query(Match.competition_code)
            .filter(
                Match.status == "SCHEDULED",
                Match.utc_date >= _now,
                Match.utc_date <= _win_end,
            )
            .all()
        )
        n_matches = len(scheduled_rows)
        m_leagues = len({r[0] for r in scheduled_rows if r[0]})

        # Fresh predictions from THIS session only
        rows = (
            _s.query(Prediction, Match)
            .join(Match, Prediction.match_id == Match.match_id)
            .filter(
                Prediction.is_value_bet == True,
                Prediction.model_probability >= 0.58,
                Prediction.created_at >= session_started_at,
                Match.utc_date >= _now,
                Match.utc_date <= _win_end,
            )
            .order_by(Prediction.model_probability.desc())
            .all()
        )

        kept: list[tuple] = []
        filtered = 0
        for p, m in rows:
            mkt_for_check = "corner" if p.market in CORNER_MARKETS else p.market
            vb = {
                "ev": p.expected_value or 0,
                "bookmaker": p.best_bookmaker or "",
                "market": mkt_for_check,
                "outcome": p.outcome or "",
            }
            susp, _ = _is_ev_suspicious(vb)
            if susp:
                filtered += 1
                continue
            if p.market == "h2h" and p.outcome == "Draw" and (p.model_probability or 0) > 0.40:
                filtered += 1
                continue
            if (p.best_odds or 0) < 1.25:
                filtered += 1
                continue
            if p.market in CORNER_MARKETS and (p.model_probability or 0) > 0.75:
                filtered += 1
                continue
            kept.append((p, m))

        top = kept[:30]

        logger.info(
            f"[phantich-ALL] user={user_id} done elapsed={elapsed:.1f}s "
            f"picks_raw={len(picks)} rows_fetched={len(rows)} kept={len(kept)} "
            f"top={len(top)} filtered_ao={filtered} odds_api_calls={used_calls}"
        )

        msg = (
            f"\U0001f310 PH\u00c2N T\u00cdCH 24H T\u1edaI\n"
            f"{'\u2501' * 17}\n"
            f"\u0110\u00e3 ph\u00e2n t\u00edch: {n_matches} tr\u1eadn trong {m_leagues} gi\u1ea3i\n"
            f"T\u00ecm \u0111\u01b0\u1ee3c: {len(kept)} k\u00e8o c\u00f3 Prob \u2265 58%\n"
            f"\u0110\u00e3 lo\u1ea1i: {filtered} k\u00e8o \u1ea3o\n"
            f"\u23f1 {elapsed:.0f}s | \U0001f4ca {calls_str} Odds API calls\n"
        )

        if not top:
            msg += (
                "\n\u26a0\ufe0f Kh\u00f4ng c\u00f3 k\u00e8o Prob \u2265 58% "
                "sau khi l\u1ecdc \u1ea3o."
            )
            await update.message.reply_text(msg)
            return

        msg += (
            f"\n\U0001f3af TOP {len(top)} K\u00c8O PROB CAO "
            f"(Prob \u2265 58%, \u0111\u00e3 l\u1ecdc \u1ea3o)\n"
            f"{'\u2500' * 17}\n"
        )
        for i, (p, m) in enumerate(top, 1):
            league = LEAGUES.get(
                m.competition_code or "",
                m.competition_code or m.competition or "?",
            )
            when = m.utc_date.strftime("%d/%m %H:%M") if m.utc_date else "?"
            mkt = MKT_NAMES.get(p.market, p.market)
            prob = (p.model_probability or 0) * 100
            ev = (p.expected_value or 0) * 100
            odds = p.best_odds or 0
            msg += (
                f"\n#{i} {m.home_team} vs {m.away_team}\n"
                f"\u23f0 {when} | \U0001f3c6 {league}\n"
                f"\u279c {p.outcome} ({mkt}) @ {odds:.2f}\n"
                f"\u2705 X\u00e1c su\u1ea5t th\u1eafng: {prob:.0f}%\n"
                f"\U0001f4b0 Odds: {odds:.2f} | EV: {ev:+.1f}%\n"
                f"\U0001f4ca {p.best_bookmaker or '?'}\n"
            )
    finally:
        _s.close()

    await _send_chunked(update, msg)


async def _run_all_live_summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show simplified summary of ALL live matches across Pinnacle-covered leagues.

    Quota-conscious: uses API-Football /fixtures?live=all (1 call) for scores,
    then fetches Odds API live odds only for leagues that actually have a live
    match. Results are cached per-chat for pagination (60s TTL).
    """
    import asyncio
    from src.config import LEAGUES, ODDS_SPORTS, API_FOOTBALL_LEAGUES
    from src.collectors.api_football import get_live_fixtures
    from src.collectors.odds_api import get_live_odds, get_live_scores, get_best_odds, get_spread_pairs

    chat_id = update.effective_chat.id if update.effective_chat else None
    await update.message.reply_text("\u26a1 \u0110ang t\u1ea3i danh s\u00e1ch tr\u1eadn LIVE...")

    loop = asyncio.get_event_loop()

    # Step 1: single call to api-football for all live fixtures.
    try:
        fixtures = await loop.run_in_executor(None, get_live_fixtures)
    except Exception as e:
        logger.error(f"[AllLive] get_live_fixtures failed: {e}")
        fixtures = []

    # Group fixtures by league code (only codes in ODDS_SPORTS so we can fetch odds)
    af_to_code = {fid: code for code, fid in API_FOOTBALL_LEAGUES.items()}
    by_league: dict[str, list[dict]] = {}
    for fix in fixtures:
        code = af_to_code.get(fix.get("league_id"))
        if code and code in ODDS_SPORTS:
            by_league.setdefault(code, []).append(fix)

    if not by_league:
        await update.message.reply_text(
            "\u26bd Kh\u00f4ng c\u00f3 tr\u1eadn live n\u00e0o "
            "\u0111ang di\u1ec5n ra \u1edf c\u00e1c gi\u1ea3i c\u00f3 Pinnacle cover."
        )
        return

    # Step 2: parallel fetch odds for each league that has live matches.
    async def _fetch_odds_for_league(lc):
        try:
            scores = await loop.run_in_executor(None, get_live_scores, lc)
            eids = [s["event_id"] for s in (scores or []) if s.get("event_id")]
            odds = await loop.run_in_executor(None, get_live_odds, lc, eids) if eids else []
            return lc, scores or [], odds
        except Exception as e:
            logger.warning(f"[AllLive] odds fetch failed for {lc}: {e}")
            return lc, [], []

    odds_results = await asyncio.gather(*[_fetch_odds_for_league(lc) for lc in by_league])
    scores_by_lc = {lc: scores for lc, scores, _ in odds_results}
    odds_by_lc = {lc: odds for lc, _, odds in odds_results}

    # Step 3: build rows per (league, match).
    lines: list[tuple[str, str]] = []  # (league_code, formatted_line)
    total = 0
    for code, fxs in by_league.items():
        for fx in fxs:
            total += 1
            home = fx.get("home", "?")
            away = fx.get("away", "?")
            hs = fx.get("home_score", 0) or 0
            ax = fx.get("away_score", 0) or 0
            minute = fx.get("minute", 0) or 0

            # Find matching odds event
            ev = None
            for o in odds_by_lc.get(code, []):
                if _match_teams(home, away, o.get("home_team", ""), o.get("away_team", "")):
                    ev = o
                    break

            parts = [f"\u26bd {home} {hs}-{ax} {away} ({minute}')"]
            if ev:
                best_h2h = get_best_odds(ev, "h2h")
                best_totals = get_best_odds(ev, "totals")
                spread_pairs = get_spread_pairs(ev)

                h_odds = best_h2h.get("Home", {})
                d_odds = best_h2h.get("Draw", {})
                a_odds = best_h2h.get("Away", {})
                h_p = h_odds.get("price") if isinstance(h_odds, dict) else None
                d_p = d_odds.get("price") if isinstance(d_odds, dict) else None
                a_p = a_odds.get("price") if isinstance(a_odds, dict) else None
                if h_p and d_p and a_p:
                    parts.append(f"1X2: {h_p:.2f}/{d_p:.2f}/{a_p:.2f}")

                if spread_pairs:
                    pair = spread_pairs[0]
                    parts.append(
                        f"AH {pair['home_point']:+g}@{pair['home_price']:.2f}/"
                        f"{pair['away_price']:.2f}"
                    )

                ou_over = best_totals.get("Over", {})
                ou_under = best_totals.get("Under", {})
                if isinstance(ou_over, dict) and "price" in ou_over and isinstance(ou_under, dict) and "price" in ou_under:
                    point = ou_over.get("point", 2.5)
                    parts.append(f"O/U {point}@{ou_over['price']:.2f}/{ou_under['price']:.2f}")

            lines.append((code, " | ".join(parts)))

    # Step 4: cache + paginate.
    if chat_id is not None:
        context.user_data["_all_live_cache"] = {
            "lines": lines,
            "total": total,
            "ts": _now_ts(),
        }

    await _send_all_live_page(update, lines, total, page=0)


def _now_ts() -> float:
    import time
    return time.time()


async def _send_all_live_page(update: Update, lines: list[tuple[str, str]], total: int, page: int):
    """Render one page of all-live lines with prev/next keyboard."""
    from src.config import LEAGUES

    page_size = _ALL_LIVE_PAGE_SIZE
    max_page = max(0, (len(lines) - 1) // page_size)
    page = max(0, min(page, max_page))
    start = page * page_size
    end = start + page_size
    chunk = lines[start:end]

    # Group the chunk lines by league code for readability.
    msg = f"\U0001f534 LIVE \u2014 TR\u1eacN \u0110ANG \u0110\u1ea4U ({total})\n"
    msg += "\u2501" * 17 + "\n"
    current_code = None
    for code, line in chunk:
        if code != current_code:
            msg += f"\n\U0001f4cb {LEAGUES.get(code, code)}\n"
            current_code = code
        msg += f"{line}\n"

    msg += f"\nTrang {page + 1}/{max_page + 1}"

    kb_rows = []
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("\u2b05 Tr\u01b0\u1edbc", callback_data=f"alllivep:{page - 1}"))
    if page < max_page:
        nav_row.append(InlineKeyboardButton("Sau \u27a1", callback_data=f"alllivep:{page + 1}"))
    if nav_row:
        kb_rows.append(nav_row)

    kb = InlineKeyboardMarkup(kb_rows) if kb_rows else None

    if update.callback_query:
        await update.callback_query.edit_message_text(msg, reply_markup=kb)
    else:
        await update.message.reply_text(msg, reply_markup=kb)


async def cmd_live(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show live in-play matches with odds and value analysis."""
    if not await _require_auth(update): return
    import asyncio
    from src.config import LEAGUES, ODDS_SPORTS, API_FOOTBALL_KEY, USE_DIXON_COLES
    from src.collectors.odds_api import get_live_odds, get_live_scores, get_best_odds, get_spread_pairs, get_corner_odds
    from src.collectors.api_football import get_live_stats_batch
    from src.models.poisson import PoissonModel, get_confidence_tier
    from src.models.dixon_coles import DixonColesModel

    _ModelCls = DixonColesModel if USE_DIXON_COLES else PoissonModel

    args = context.args or []
    league_filter = args[0].upper() if args else None

    if not league_filter:
        # No args → show simplified ALL-LIVE summary (all Pinnacle-covered leagues).
        # User can pick a specific league via /live <code> or buttons in picker.
        await _run_all_live_summary(update, context)
        return

    if league_filter not in LEAGUES:
        await update.message.reply_text(f"\u274c M\u00e3 gi\u1ea3i '{league_filter}' kh\u00f4ng h\u1ee3p l\u1ec7. D\u00f9ng /leagues \u0111\u1ec3 xem danh s\u00e1ch.")
        return

    await update.message.reply_text(f"\u26a1 \u0110ang t\u1ea3i {LEAGUES[league_filter]} tr\u1ef1c ti\u1ebfp...")

    session = get_session()
    try:
        # Step 1: Fetch live scores + live stats in parallel
        if league_filter:
            league_codes = [league_filter] if league_filter in ODDS_SPORTS else []
        else:
            league_codes = [lc for lc in LEAGUES if lc in ODDS_SPORTS]
        loop = asyncio.get_event_loop()

        async def fetch_scores(lc):
            return lc, await loop.run_in_executor(None, get_live_scores, lc)

        async def fetch_live_stats():
            if not API_FOOTBALL_KEY:
                return []
            return await loop.run_in_executor(None, get_live_stats_batch)

        # Parallel: all scores + live stats
        score_tasks = [fetch_scores(lc) for lc in league_codes]
        stats_task = fetch_live_stats()
        all_step1 = await asyncio.gather(*score_tasks, stats_task)

        score_results = all_step1[:-1]
        live_stats_list = all_step1[-1]

        # Build stats lookup by team name
        live_stats_map = {}
        for ls in (live_stats_list or []):
            key = f"{ls['home']}__{ls['away']}"
            live_stats_map[key] = ls

        # Step 2: For leagues with live matches, fetch odds + corners
        async def fetch_live_odds(lc, event_ids):
            return lc, await loop.run_in_executor(None, get_live_odds, lc, event_ids)

        async def fetch_corners(lc, eids=None):
            return lc, await loop.run_in_executor(None, get_corner_odds, lc, eids)

        odds_tasks = []
        corner_tasks = []
        scores_by_league = {}
        for lc, scores in score_results:
            if scores:
                scores_by_league[lc] = scores
                event_ids = [s["event_id"] for s in scores if s.get("event_id")]
                if event_ids:
                    odds_tasks.append(fetch_live_odds(lc, event_ids))
                    corner_tasks.append(fetch_corners(lc, event_ids))
                else:
                    corner_tasks.append(fetch_corners(lc))

        odds_by_league = {}
        corners_by_league = {}
        step2_tasks = odds_tasks + corner_tasks
        if step2_tasks:
            step2_results = await asyncio.gather(*step2_tasks)
            for item in step2_results:
                lc, data = item
                if isinstance(data, list):
                    odds_by_league[lc] = data
                elif isinstance(data, dict):
                    corners_by_league[lc] = data

        # === CORNER DEBUG: log what get_corner_odds actually returned per league ===
        try:
            for _lc, _cmap in corners_by_league.items():
                _keys = list(_cmap.keys()) if isinstance(_cmap, dict) else []
                logger.warning(
                    f"[CORNER-DEBUG-FETCH] /live {_lc}: "
                    f"returned {len(_keys)} match(es) keys={_keys}"
                )
                if isinstance(_cmap, dict):
                    for _ck, _cv in _cmap.items():
                        _tot = list((_cv or {}).get("totals", {}).keys())
                        _spr_n = len((_cv or {}).get("spreads", []) or [])
                        _h1t = list((_cv or {}).get("h1_totals", {}).keys())
                        _h1s_n = len((_cv or {}).get("h1_spreads", []) or [])
                        logger.warning(
                            f"[CORNER-DEBUG-FETCH] /live {_lc} {_ck}: "
                            f"totals_lines={_tot} spreads_count={_spr_n} "
                            f"h1_totals_lines={_h1t} h1_spreads_count={_h1s_n}"
                        )
        except Exception as _dbg_exc:
            logger.warning(f"[CORNER-DEBUG-FETCH] /live dump failed: {_dbg_exc}")
        # === END CORNER DEBUG ===

        # Combine
        results = []
        for lc in league_codes:
            scores = scores_by_league.get(lc, [])
            odds = odds_by_league.get(lc, [])
            corners = corners_by_league.get(lc, {})
            if scores:
                logger.info(f"[Live] {lc}: {len(scores)} live, {len(odds)} odds events, {len(corners)} corner events")
                results.append((lc, odds, scores, corners))

        # Build model per league
        from src.db.models import Match as MatchModel
        from datetime import datetime, timedelta

        total_live = 0
        messages = []
        live_values = []

        for league_code, live_odds, live_scores, league_corners in results:
            if not live_odds and not live_scores:
                continue

            league_name = LEAGUES.get(league_code, league_code)

            # Fit model from history
            hist = (
                session.query(MatchModel)
                .filter(MatchModel.competition_code == league_code)
                .filter(MatchModel.home_goals.isnot(None))
                .order_by(MatchModel.utc_date.desc())
                .limit(200)
                .all()
            )
            results_data = [
                {"home_team": h.home_team, "away_team": h.away_team,
                 "home_goals": h.home_goals, "away_goals": h.away_goals,
                 "utc_date": h.utc_date.isoformat() if h.utc_date else None}
                for h in hist if h.home_goals is not None
            ]
            model = _ModelCls()
            model.fit(results_data)

            # Build score lookup
            score_map = {}
            for s in live_scores:
                key = f"{s['home_team']}__{s['away_team']}"
                score_map[key] = s

            # Build odds lookup by team matching
            odds_map = {}
            for ev in live_odds:
                odds_map[f"{ev['home_team']}__{ev['away_team']}"] = ev

            current_msg = f"\n\U0001f3c6 {league_name} \u2014 LIVE [v11-rawlog]\n\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n"

            for sc in live_scores:
                home = sc["home_team"]
                away = sc["away_team"]

                # Find matching odds event — skip if no bookmaker has odds
                ev = None
                for ok, ov in odds_map.items():
                    parts = ok.split("__")
                    if len(parts) == 2 and _match_teams(home, away, parts[0], parts[1]):
                        ev = ov
                        break

                if not ev:
                    logger.info(f"[Live] Skip {home} vs {away} — no bookmaker odds")
                    continue

                total_live += 1
                score_str = f" {sc['home_score']}-{sc['away_score']}"

                pred = model.predict(home, away)
                h = pred["h2h"]
                t = pred["totals"]
                ah = pred.get("asian_handicap", {})

                best_h2h = get_best_odds(ev, "h2h")
                best_totals = get_best_odds(ev, "totals")
                spread_pairs = get_spread_pairs(ev)

                current_msg += f"\n\u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510\n"

                # Find live stats
                match_stats = None
                for sk, sv in live_stats_map.items():
                    parts = sk.split("__")
                    if len(parts) == 2 and _match_teams(home, away, parts[0], parts[1]):
                        match_stats = sv
                        break

                minute_str = f" ({match_stats['minute']}')" if match_stats and match_stats.get("minute") else ""
                current_msg += f"  \U0001f534 LIVE{minute_str}: {home} vs {away}{score_str}\n"

                # Live stats + analysis
                live_analysis = None
                if match_stats and match_stats.get("stats"):
                    hs = match_stats["stats"].get("home", {})
                    as_ = match_stats["stats"].get("away", {})
                    if hs and as_:
                        m_minute = match_stats.get("minute", 0) or 0
                        m_parsed = match_stats.get("parsed_events")
                        m_events = match_stats.get("events", [])
                        live_analysis = _analyze_live(
                            hs, as_, m_minute,
                            sc.get("home_score", 0), sc.get("away_score", 0),
                            pred, parsed_events=m_parsed, events=m_events
                        )

                        # Stats display — all API-Football v3 stats
                        h_xg_str = f"{live_analysis['h_xg']:.2f}" if live_analysis.get("h_xg") else "?"
                        a_xg_str = f"{live_analysis['a_xg']:.2f}" if live_analysis.get("a_xg") else "?"
                        current_msg += (
                            f"  \U0001f4ca Thống kê trực tiếp:\n"
                            f"    Kiểm soát: {hs.get('possession', '?')} - {as_.get('possession', '?')}\n"
                            f"    Sút (trúng/tổng): {hs.get('shots_on', 0)}/{hs.get('shots', 0)} - {as_.get('shots_on', 0)}/{as_.get('shots', 0)}\n"
                            f"    Sút ngoài khung: {hs.get('shots_off', 0)} - {as_.get('shots_off', 0)}\n"
                            f"    Sút bị chặn: {hs.get('blocked', 0)} - {as_.get('blocked', 0)}\n"
                            f"    Trong vòng cấm: {hs.get('shots_insidebox', 0)} - {as_.get('shots_insidebox', 0)}\n"
                            f"    Ngoài vòng cấm: {hs.get('shots_outsidebox', 0)} - {as_.get('shots_outsidebox', 0)}\n"
                            f"    Chuyền (ch.xác): {hs.get('passes', 0)}({hs.get('pass_accuracy', 0)}) {hs.get('pass_pct', '?')} - {as_.get('passes', 0)}({as_.get('pass_accuracy', 0)}) {as_.get('pass_pct', '?')}\n"
                            f"    Phạt góc: {hs.get('corners', 0)} - {as_.get('corners', 0)} (pace {live_analysis['corners_pace']}/90')\n"
                            f"    Phạm lỗi: {hs.get('fouls', 0)} - {as_.get('fouls', 0)}\n"
                            f"    Việt vị: {hs.get('offsides', 0)} - {as_.get('offsides', 0)}\n"
                            f"    Cứu thua: {hs.get('saves', 0)} - {as_.get('saves', 0)}\n"
                            f"    Thẻ vàng: {hs.get('yellow', 0)} - {as_.get('yellow', 0)}\n"
                            f"    xG: {h_xg_str} - {a_xg_str}\n"
                        )
                        # Red cards display
                        if live_analysis.get("has_red"):
                            current_msg += f"    \U0001f7e5 Th\u1ebb \u0111\u1ecf: Ch\u1ee7 {live_analysis['home_reds']} - Kh\u00e1ch {live_analysis['away_reds']}\n"

                        # Substitutions + game state
                        h_subs = live_analysis.get("home_subs", 0)
                        a_subs = live_analysis.get("away_subs", 0)
                        if h_subs or a_subs:
                            intent_label = {"attacking": "\u2694 T\u1ea5n c\u00f4ng", "defensive": "\U0001f6e1 Ph\u00f2ng ng\u1ef1", "neutral": ""}.get(live_analysis.get("sub_intent", ""), "")
                            intent_str = f" ({intent_label})" if intent_label else ""
                            current_msg += f"    \U0001f504 Thay ng\u01b0\u1eddi: Ch\u1ee7 {h_subs}/5 - Kh\u00e1ch {a_subs}/5{intent_str}\n"

                        # Game state label
                        gs = live_analysis.get("game_state", "")
                        tp = live_analysis.get("time_phase", "")
                        gs_labels = {"home_leading": "Ch\u1ee7 d\u1eabn", "away_leading": "Kh\u00e1ch d\u1eabn", "drawing": "H\u00f2a"}
                        tp_labels = {"early": "S\u1edbm", "mid": "Gi\u1eefa", "late": "Cu\u1ed1i"}
                        if gs and tp:
                            current_msg += f"    \U0001f3af Tr\u1ea1ng th\u00e1i: {gs_labels.get(gs, gs)} | Giai \u0111o\u1ea1n: {tp_labels.get(tp, tp)}\n"

                        # Insights
                        if live_analysis["insights"]:
                            current_msg += f"  \U0001f9e0 Ph\u00e2n t\u00edch:\n"
                            for insight in live_analysis["insights"]:
                                current_msg += f"    {insight}\n"

                # Use adjusted h2h if available
                if live_analysis:
                    h = live_analysis["adj_h2h"]

                # 1X2
                h_odds = best_h2h.get("Home", {})
                d_odds = best_h2h.get("Draw", {})
                a_odds = best_h2h.get("Away", {})
                h_price = f" @{h_odds['price']:.2f}" if isinstance(h_odds, dict) and "price" in h_odds else ""
                d_price = f" @{d_odds['price']:.2f}" if isinstance(d_odds, dict) and "price" in d_odds else ""
                a_price = f" @{a_odds['price']:.2f}" if isinstance(a_odds, dict) and "price" in a_odds else ""
                h2h_bk = h_odds.get("bookmaker", "") if isinstance(h_odds, dict) else ""
                bk_str = f" ({h2h_bk})" if h2h_bk else ""
                current_msg += f"  \U0001f1ea\U0001f1fa 1X2: H {h['Home']*100:.0f}%{h_price} | D {h['Draw']*100:.0f}%{d_price} | A {h['Away']*100:.0f}%{a_price}{bk_str}\n"

                # O/U
                ou_over = best_totals.get("Over", {})
                ou_under = best_totals.get("Under", {})
                if isinstance(ou_over, dict) and "price" in ou_over:
                    actual_point = ou_over.get("point", 2.5)
                    over_key = f"Over {actual_point}"
                    under_key = f"Under {actual_point}"
                    over_prob = t.get(over_key, t.get("Over 2.5", 0))
                    under_prob = t.get(under_key, t.get("Under 2.5", 0))
                    ou_bk = ou_over.get("bookmaker", "")
                    u_price = f" @{ou_under['price']:.2f}" if isinstance(ou_under, dict) and "price" in ou_under else ""
                    bk_str = f" ({ou_bk})" if ou_bk else ""
                    current_msg += (
                        f"  \u2b06 T\u00e0i/X\u1ec9u {actual_point}: "
                        f"T\u00e0i {over_prob*100:.0f}% @{ou_over['price']:.2f} | "
                        f"X\u1ec9u {under_prob*100:.0f}%{u_price}{bk_str}\n"
                    )

                # Châu Á
                if spread_pairs:
                    pair = spread_pairs[0]
                    probs = _get_pair_probs(pair, ah, home)
                    hp = pair["home_point"]
                    ap = pair["away_point"]
                    current_msg += (
                        f"  \U0001f30f Ch\u00e2u \u00c1: {pair['home_name'][:10]} {hp:+g} "
                        f"{probs['pair_home_prob']*100:.0f}% @{pair['home_price']:.2f} | "
                        f"{pair['away_name'][:10]} {ap:+g} "
                        f"{probs['pair_away_prob']*100:.0f}% @{pair['away_price']:.2f} ({pair['bookmaker']})\n"
                    )

                # === PH\u1ea0T G\u00d3C (Corners) ===
                corners_pred = pred.get("corners", {})
                corner_lines = corners_pred.get("lines", {})
                corner_ah_pred = corners_pred.get("asian_handicap", {})
                corner_key = f"{home}__{away}"
                corner_data = league_corners.get(corner_key, {})
                if not corner_data:
                    for ck, cv in league_corners.items():
                        parts = ck.split("__")
                        if len(parts) == 2 and _match_teams(home, away, parts[0], parts[1]):
                            corner_data = cv
                            break
                corner_totals_odds = corner_data.get("totals", {})
                corner_spreads = corner_data.get("spreads", [])
                corner_xg = corners_pred.get("xg", 10.5)

                # Calculate live-adjusted corner probabilities
                actual_corners = 0
                live_corner_pace = corner_xg  # fallback to model xC
                m_minute = 0
                if match_stats and match_stats.get("stats"):
                    _hs = match_stats["stats"].get("home", {})
                    _as = match_stats["stats"].get("away", {})
                    actual_corners = (_hs.get("corners", 0) or 0) + (_as.get("corners", 0) or 0)
                    m_minute = match_stats.get("minute", 0) or 0
                    if m_minute > 5:
                        live_corner_pace = actual_corners / m_minute * 90

                def _live_corner_prob(line, actual, minute, pace):
                    """Calculate live O/U probability using Poisson on remaining corners."""
                    if actual > line:
                        return 1.0, 0.0  # Already over
                    if minute >= 90:
                        return (1.0, 0.0) if actual > line else (0.0, 1.0)
                    remaining_min = max(90 - minute, 1)
                    expected_remaining = pace / 90 * remaining_min
                    # Need (line - actual) more corners to go over
                    need = line - actual  # e.g. 10.5 - 13 = -2.5 → already over
                    if need <= 0:
                        return 1.0, 0.0
                    # P(X > need) where X ~ Poisson(expected_remaining)
                    # P(X > k) = 1 - P(X <= floor(k))
                    k = int(need)  # floor of 0.5 = 0, floor of 1.5 = 1, etc.
                    cum_prob = 0.0
                    for i in range(k + 1):
                        cum_prob += (expected_remaining ** i) * math.exp(-expected_remaining) / math.factorial(i)
                    over_prob = 1 - cum_prob
                    return over_prob, cum_prob

                # Pinnacle-only corner data
                live_corner_lines = [l for l in corner_totals_odds.keys() if corner_totals_odds[l].get("over_price")]
                has_corner_data = bool(live_corner_lines or corner_spreads)
                # === CORNER DEBUG: log handler-level view ===
                try:
                    logger.warning(
                        f"[CORNER-DEBUG-LIVE] {league_code} {home} vs {away} | "
                        f"corner_key={corner_key!r} | "
                        f"league_corners_keys={list(league_corners.keys())} | "
                        f"corner_data_keys={list((corner_data or {}).keys())} | "
                        f"totals_raw={corner_totals_odds} | "
                        f"spreads_count={len(corner_spreads)} | "
                        f"live_corner_lines={live_corner_lines} | "
                        f"has_corner_data={has_corner_data}"
                    )
                except Exception as _dbg_exc:
                    logger.warning(f"[CORNER-DEBUG-LIVE] dump failed: {_dbg_exc}")
                # === END CORNER DEBUG ===
                if not has_corner_data:
                    current_msg += f"  \u2691 Ph\u1ea1t g\u00f3c: Ch\u01b0a c\u00f3 k\u00e8o\n"
                else:
                    pace_str = f" | live {live_corner_pace:.1f}/90'" if m_minute > 5 else ""
                    actual_str = f" | thực tế: {actual_corners}" if actual_corners > 0 else ""
                    current_msg += f"  \u2691 Phạt góc (xC: {corner_xg}{actual_str}{pace_str}):\n"
                    for line in live_corner_lines:
                        cl = corner_lines.get(line, {})
                        co = corner_totals_odds.get(line, {})
                        if m_minute > 5:
                            o_prob, u_prob = _live_corner_prob(line, actual_corners, m_minute, live_corner_pace)
                        else:
                            o_prob = cl.get("over", 0)
                            u_prob = cl.get("under", 0)
                        o_price = f" @{co['over_price']:.2f}" if co.get("over_price") else ""
                        u_price = f" @{co['under_price']:.2f}" if co.get("under_price") else ""
                        if actual_corners > line:
                            status = " ✅ ĐÃ QUA"
                        elif m_minute >= 85 and actual_corners <= line - 2:
                            status = " ❌ KHÓ"
                        else:
                            status = ""
                        current_msg += f"    Tài/Xỉu {line}: Tài {o_prob*100:.0f}%{o_price} | Xỉu {u_prob*100:.0f}%{u_price}{status}\n"
                    # Corner AH
                    if corner_spreads:
                        cs = corner_spreads[0]
                        hp = cs["home_point"]
                        ap = cs["away_point"]
                        pair_home_is_match_home = _is_home_team(cs["home_name"], home)
                        if pair_home_is_match_home:
                            model_key = f"{hp:+g}" if hp != 0 else "0"
                            ah_p = corner_ah_pred.get(model_key, {})
                            h_prob = ah_p.get("home", 0)
                            a_prob = ah_p.get("away", 0)
                        else:
                            model_key = f"{ap:+g}" if ap != 0 else "0"
                            ah_p = corner_ah_pred.get(model_key, {})
                            h_prob = ah_p.get("away", 0)
                            a_prob = ah_p.get("home", 0)
                        current_msg += (
                            f"    Ch\u00e2u \u00c1: {cs['home_name'][:10]} {hp:+g} "
                            f"{h_prob*100:.0f}% @{cs['home_price']:.2f} | "
                            f"{cs['away_name'][:10]} {ap:+g} "
                            f"{a_prob*100:.0f}% @{cs['away_price']:.2f} ({cs['bk']})\n"
                        )

                # === G\u00d3C HI\u1ec6P 1 (First Half Corners) ===
                h1c_pred = pred.get("corners_h1", {})
                h1c_lines = h1c_pred.get("lines", {})
                h1c_ah_pred = h1c_pred.get("asian_handicap", {})
                h1c_totals_odds = corner_data.get("h1_totals", {})
                h1c_spreads = corner_data.get("h1_spreads", [])
                h1c_xg = h1c_pred.get("xg", 4.7)

                live_h1c_lines = [l for l in h1c_totals_odds.keys() if h1c_totals_odds[l].get("over_price")]
                has_h1c_data = bool(live_h1c_lines or h1c_spreads)
                if not has_h1c_data:
                    current_msg += f"  \u2691 Ch\u01b0a c\u00f3 k\u00e8o hi\u1ec7p 1\n"
                else:
                    current_msg += f"  \u2691 G\u00f3c hi\u1ec7p 1 (xC: {h1c_xg}):\n"
                    for line in live_h1c_lines:
                        cl = h1c_lines.get(line, {})
                        co = h1c_totals_odds.get(line, {})
                        o_prob = cl.get("over", 0)
                        u_prob = cl.get("under", 0)
                        o_price = f" @{co['over_price']:.2f}" if co.get("over_price") else ""
                        u_price = f" @{co['under_price']:.2f}" if co.get("under_price") else ""
                        current_msg += f"    T\u00e0i/X\u1ec9u {line}: T\u00e0i {o_prob*100:.0f}%{o_price} | X\u1ec9u {u_prob*100:.0f}%{u_price}\n"
                    if h1c_spreads:
                        cs = h1c_spreads[0]
                        hp = cs["home_point"]
                        ap = cs["away_point"]
                        pair_home_is_match_home = _is_home_team(cs["home_name"], home)
                        if pair_home_is_match_home:
                            model_key = f"{hp:+g}" if hp != 0 else "0"
                            ah_p = h1c_ah_pred.get(model_key, {})
                            h_prob = ah_p.get("home", 0)
                            a_prob = ah_p.get("away", 0)
                        else:
                            model_key = f"{ap:+g}" if ap != 0 else "0"
                            ah_p = h1c_ah_pred.get(model_key, {})
                            h_prob = ah_p.get("away", 0)
                            a_prob = ah_p.get("home", 0)
                        current_msg += (
                            f"    Ch\u00e2u \u00c1: {cs['home_name'][:10]} {hp:+g} "
                            f"{h_prob*100:.0f}% @{cs['home_price']:.2f} | "
                            f"{cs['away_name'][:10]} {ap:+g} "
                            f"{a_prob*100:.0f}% @{cs['away_price']:.2f} ({cs['bk']})\n"
                        )

                # Value bets
                match_values = []
                # 1X2 value
                for outcome, prob in h.items():
                    info = best_h2h.get(outcome)
                    if isinstance(info, dict) and "price" in info:
                        ev_val = prob * info["price"] - 1
                        if ev_val > 0:
                            match_values.append({"outcome": outcome, "market": "1X2", "odds": info["price"], "ev": ev_val, "bk": info["bookmaker"], "prob": prob})

                # O/U value
                for outcome_key, vn_label in [("Over", "T\u00e0i"), ("Under", "X\u1ec9u")]:
                    info = best_totals.get(outcome_key)
                    if isinstance(info, dict) and "price" in info:
                        actual_pt = info.get("point", 2.5)
                        pred_key = f"{outcome_key} {actual_pt}"
                        prob = t.get(pred_key, t.get(f"{outcome_key} 2.5", 0))
                        ev_val = prob * info["price"] - 1
                        if ev_val > 0:
                            match_values.append({"outcome": f"{vn_label} {actual_pt}", "market": "T\u00e0i/X\u1ec9u", "odds": info["price"], "ev": ev_val, "bk": info.get("bookmaker", "?"), "prob": prob})

                # AH value — only main line (first pair = Pinnacle or best)
                if spread_pairs:
                    pair = spread_pairs[0]
                    probs = _get_pair_probs(pair, ah, home)
                    for side, sp, price_key, point_key, name_key in [
                        ("home", probs["pair_home_prob"], "home_price", "home_point", "home_name"),
                        ("away", probs["pair_away_prob"], "away_price", "away_point", "away_name"),
                    ]:
                        ev_val = sp * pair[price_key] - 1
                        if ev_val > 0:
                            pt = pair[point_key]
                            pt_str = f"{pt:+g}" if pt != 0 else "0"
                            match_values.append({"outcome": f"{pair[name_key]} {pt_str}", "market": "Ch\u00e2u \u00c1", "odds": pair[price_key], "ev": ev_val, "bk": pair["bookmaker"], "prob": sp})

                # Corner O/U value — only Pinnacle's listed line
                live_cv_lines = [l for l in corner_totals_odds.keys() if corner_totals_odds[l].get("over_price")]
                for line in live_cv_lines:
                    cl = corner_lines.get(line, {})
                    co = corner_totals_odds.get(line, {})
                    if m_minute > 5:
                        o_prob, u_prob = _live_corner_prob(line, actual_corners, m_minute, live_corner_pace)
                    else:
                        o_prob = cl.get("over", 0)
                        u_prob = cl.get("under", 0)
                    # Determine live signal based on pace
                    live_sig = ""
                    if m_minute > 5:
                        if actual_corners > line:
                            live_sig = "✅ ĐÃ QUA"
                        elif live_corner_pace > line + 1:
                            live_sig = "🟢 PACE CAO"
                        elif live_corner_pace < line - 1:
                            live_sig = "🔴 PACE THẤP"
                    if co.get("over_price") and o_prob > 0:
                        ev_co = o_prob * co["over_price"] - 1
                        if ev_co > 0:
                            entry = {"outcome": f"Góc Tài {line}", "market": "Phạt góc", "odds": co["over_price"], "ev": ev_co, "bk": co["over_bk"], "prob": o_prob}
                            if live_sig:
                                entry["live_signal"] = live_sig
                            match_values.append(entry)
                    if co.get("under_price") and u_prob > 0:
                        ev_cu = u_prob * co["under_price"] - 1
                        if ev_cu > 0:
                            u_sig = ""
                            if m_minute > 5:
                                if live_corner_pace < line - 1:
                                    u_sig = "🟢 PACE THẤP"
                                elif live_corner_pace > line + 1:
                                    u_sig = "🔴 PACE CAO"
                            entry = {"outcome": f"Góc Xỉu {line}", "market": "Phạt góc", "odds": co["under_price"], "ev": ev_cu, "bk": co["under_bk"], "prob": u_prob}
                            if u_sig:
                                entry["live_signal"] = u_sig
                            match_values.append(entry)

                # Corner AH value — main line only (matches what bookmaker displays)
                if corner_spreads:
                    cs = corner_spreads[0]
                    pair_home_is_match_home = _is_home_team(cs["home_name"], home)
                    if pair_home_is_match_home:
                        mk = f"{cs['home_point']:+g}" if cs['home_point'] != 0 else "0"
                        ah_p = corner_ah_pred.get(mk, {})
                        ch_prob = ah_p.get("home", 0)
                        ca_prob = ah_p.get("away", 0)
                    else:
                        mk = f"{cs['away_point']:+g}" if cs['away_point'] != 0 else "0"
                        ah_p = corner_ah_pred.get(mk, {})
                        ch_prob = ah_p.get("away", 0)
                        ca_prob = ah_p.get("home", 0)
                    if ch_prob > 0:
                        ev_ch = ch_prob * cs["home_price"] - 1
                        if ev_ch > 0:
                            match_values.append({"outcome": f"G\u00f3c {cs['home_name'][:10]} {cs['home_point']:+g}", "market": "G\u00f3c Ch\u00e2u \u00c1", "odds": cs["home_price"], "ev": ev_ch, "bk": cs["bk"], "prob": ch_prob})
                    if ca_prob > 0:
                        ev_ca = ca_prob * cs["away_price"] - 1
                        if ev_ca > 0:
                            match_values.append({"outcome": f"G\u00f3c {cs['away_name'][:10]} {cs['away_point']:+g}", "market": "G\u00f3c Ch\u00e2u \u00c1", "odds": cs["away_price"], "ev": ev_ca, "bk": cs["bk"], "prob": ca_prob})

                # H1 corner O/U value
                for line in [3.5, 4.5, 5.5, 6.5]:
                    cl = h1c_lines.get(line, {})
                    co = h1c_totals_odds.get(line, {})
                    o_prob = cl.get("over", 0)
                    u_prob = cl.get("under", 0)
                    if co.get("over_price") and o_prob > 0:
                        ev_co = o_prob * co["over_price"] - 1
                        if ev_co > 0:
                            match_values.append({"outcome": f"G\u00f3c H1 T\u00e0i {line}", "market": "G\u00f3c hi\u1ec7p 1", "odds": co["over_price"], "ev": ev_co, "bk": co["over_bk"], "prob": o_prob})
                    if co.get("under_price") and u_prob > 0:
                        ev_cu = u_prob * co["under_price"] - 1
                        if ev_cu > 0:
                            match_values.append({"outcome": f"G\u00f3c H1 X\u1ec9u {line}", "market": "G\u00f3c hi\u1ec7p 1", "odds": co["under_price"], "ev": ev_cu, "bk": co["under_bk"], "prob": u_prob})

                # H1 corner AH value
                if h1c_spreads:
                    cs = h1c_spreads[0]
                    pair_hm = _is_home_team(cs["home_name"], home)
                    if pair_hm:
                        mk = f"{cs['home_point']:+g}" if cs['home_point'] != 0 else "0"
                        ah_p = h1c_ah_pred.get(mk, {})
                        ch_prob = ah_p.get("home", 0)
                        ca_prob = ah_p.get("away", 0)
                    else:
                        mk = f"{cs['away_point']:+g}" if cs['away_point'] != 0 else "0"
                        ah_p = h1c_ah_pred.get(mk, {})
                        ch_prob = ah_p.get("away", 0)
                        ca_prob = ah_p.get("home", 0)
                    if ch_prob > 0:
                        ev_ch = ch_prob * cs["home_price"] - 1
                        if ev_ch > 0:
                            match_values.append({"outcome": f"G\u00f3c H1 {cs['home_name'][:10]} {cs['home_point']:+g}", "market": "G\u00f3c H1 Ch\u00e2u \u00c1", "odds": cs["home_price"], "ev": ev_ch, "bk": cs["bk"], "prob": ch_prob})
                    if ca_prob > 0:
                        ev_ca = ca_prob * cs["away_price"] - 1
                        if ev_ca > 0:
                            match_values.append({"outcome": f"G\u00f3c H1 {cs['away_name'][:10]} {cs['away_point']:+g}", "market": "G\u00f3c H1 Ch\u00e2u \u00c1", "odds": cs["away_price"], "ev": ev_ca, "bk": cs["bk"], "prob": ca_prob})

                # Show best value per market
                CORNER_MARKETS = {"Phạt góc", "Góc Châu Á", "Góc hiệp 1", "Góc H1 Châu Á"}
                market_groups = {}
                for v in sorted(match_values, key=lambda x: x["ev"], reverse=True):
                    if v["market"] not in market_groups:
                        market_groups[v["market"]] = v

                shown_best = list(market_groups.values())
                if shown_best:
                    current_msg += f"  \U0001f3af QUYẾT ĐỊNH:\n"
                    for best in shown_best:
                        conf = get_confidence_tier(best["ev"], best["prob"])
                        is_corner = best["market"] in CORNER_MARKETS
                        conf_emoji = {"HIGH": "\U0001f534", "MEDIUM": "\U0001f7e1", "LOW": "\U0001f7e2"}.get(conf, "\u26aa")

                        # Decision: BET or SKIP
                        live_sig_val = best.get("live_signal", "")
                        if "ĐÃ QUA" in live_sig_val:
                            decision = "✅ ĐÃ THẮNG"
                        elif conf in ("HIGH", "MEDIUM"):
                            decision = "✅ ĐẶT"
                        elif is_corner and best.get("prob", 0) >= 0.7 and best["ev"] > 0:
                            decision = "✅ ĐẶT"  # High live prob corner
                        elif is_corner and best["ev"] > -0.05:
                            decision = "⚡ CÂN NHẮC"
                        else:
                            decision = "⏭ BỎ QUA"

                        live_sig = best.get("live_signal", "")
                        sig_str = f" {live_sig}" if live_sig else ""

                        current_msg += (
                            f"    {conf_emoji} {best['outcome']} ({best['market']}) "
                            f"@{best['odds']:.2f} | EV:{best['ev']*100:+.1f}% "
                            f"→ {decision}{sig_str}\n"
                        )

                        if conf in ("HIGH", "MEDIUM") or (is_corner and best["ev"] > -0.03):
                            m_min = match_stats.get("minute", 0) if match_stats else 0
                            live_values.append({
                                **best, "home": home, "away": away,
                                "score": score_str, "league": league_name,
                                "confidence": conf, "minute": m_min or 0,
                            })

                current_msg += f"\u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518\n"

            messages.append(current_msg)

        if total_live == 0:
            await update.message.reply_text(
                "\u26bd Kh\u00f4ng c\u00f3 tr\u1eadn n\u00e0o \u0111ang di\u1ec5n ra.\n"
                "D\u00f9ng /phantich \u0111\u1ec3 xem tr\u1eadn s\u1eafp t\u1edbi."
            )
            return

        header = (
            f"\u26a1 TR\u1ef0C TI\u1ebeP \u2014 {total_live} tr\u1eadn \u0111ang di\u1ec5n ra\n"
            f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        )
        all_text = header + "".join(messages)
        await _safe_reply(update, all_text)

        # Live value picks summary
        if live_values:
            conf_emojis = {"HIGH": "\U0001f534", "MEDIUM": "\U0001f7e1", "LOW": "\U0001f7e2"}
            conf_labels = {"HIGH": "CAO", "MEDIUM": "TB", "LOW": "THẤP"}
            live_values.sort(key=lambda x: (0 if x["confidence"] == "HIGH" else 1 if x["confidence"] == "MEDIUM" else 2, -x["ev"]))

            summary = (
                f"\n\U0001f525 KÈO LIVE — QUYẾT ĐỊNH\n"
                f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
            )
            seen = set()
            for pick in live_values:
                mk = f"{pick['home']}_{pick['market']}"
                if mk in seen:
                    continue
                seen.add(mk)
                conf = pick["confidence"]
                emoji = conf_emojis.get(conf, "\u26aa")
                label = conf_labels.get(conf, "?")

                if conf in ("HIGH", "MEDIUM"):
                    decision = "✅ ĐẶT"
                else:
                    decision = "⚡ CÂN NHẮC"

                pick_min = pick.get('minute', 0)
                min_str = f" ({pick_min}')" if pick_min else ""
                live_sig = pick.get("live_signal", "")
                sig_str = f"\n  {live_sig}" if live_sig else ""

                summary += (
                    f"\n{emoji} [{label}] {pick['home']} vs {pick['away']}{pick['score']}{min_str}\n"
                    f"  {pick['league']}\n"
                    f"  \u27a4 {pick['outcome']} ({pick['market']}) @{pick['odds']:.2f}\n"
                    f"  Prob: {pick['prob']*100:.0f}% | EV: {pick['ev']*100:+.1f}% | {pick['bk']}\n"
                    f"  → {decision}{sig_str}\n"
                )
            await _safe_reply(update, summary)

    except Exception as e:
        logger.error(f"[Live] Error: {e}", exc_info=True)
        await update.message.reply_text(f"\u274c L\u1ed7i live: {e}")
    finally:
        session.close()


async def cmd_dongtien(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Hiển thị phân tích dòng tiền (line movement) cho 1 trận cụ thể."""
    if not await _require_auth(update):
        return

    args = context.args or []
    if not args:
        await update.message.reply_text(
            "\U0001f4a1 C\u00e1ch d\u00f9ng: /dongtien <match_id>\n"
            "VD: /dongtien 497123\n"
            "\n\u0110\u1ec3 xem match_id: d\u00f9ng /today ho\u1eb7c /phantich"
        )
        return

    try:
        match_id = int(args[0])
    except ValueError:
        await update.message.reply_text("\u274c match_id ph\u1ea3i l\u00e0 s\u1ed1 nguy\u00ean.")
        return

    from src.analytics.line_movement import (
        compute_drift,
        get_all_bookmakers_current,
        get_current_odds,
    )

    session = get_session()
    try:
        match = session.query(Match).filter(Match.match_id == match_id).first()
        if match is None:
            await update.message.reply_text(f"\u274c Kh\u00f4ng t\u00ecm th\u1ea5y tr\u1eadn match_id={match_id}.")
            return

        try:
            kickoff = match.utc_date.strftime("%H:%M %d/%m/%Y UTC") if match.utc_date else "?"
        except Exception:
            kickoff = str(match.utc_date)

        msg = (
            f"\U0001f4b8 PH\u00c2N T\u00cdCH D\u00d2NG TI\u1ec0N\n"
            f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
            f"\U0001f3c6 {match.competition or '?'}\n"
            f"\u26bd {match.home_team} vs {match.away_team}\n"
            f"\U0001f552 {kickoff}\n\n"
        )

        # --- 1X2 Line Movement (Pinnacle) ---
        # OddsHistory lưu outcome là tên đội (vd "Arsenal") + "Draw" theo format Odds API.
        msg += "\U0001f4ca 1X2 — Pinnacle\n"
        any_1x2 = False
        for display, outcome in (
            (match.home_team, match.home_team),
            ("Draw", "Draw"),
            (match.away_team, match.away_team),
        ):
            drift = compute_drift(match_id, "h2h", outcome, "pinnacle")
            if drift is None:
                continue
            any_1x2 = True
            emoji = {"shortening": "\U0001f53b", "drifting": "\U0001f53a", "stable": "\u27a1\ufe0f"}[drift["direction"]]
            msg += (
                f"  {emoji} {display}: {drift['opening_odds']:.2f} \u2192 "
                f"{drift['current_odds']:.2f} ({drift['drift_pct']:+.2f}%)\n"
            )
        if not any_1x2:
            msg += "  (ch\u01b0a c\u00f3 d\u1eef li\u1ec7u)\n"

        # --- Tài/Xỉu (totals) on latest point ---
        msg += "\n\U0001f522 T\u00e0i/X\u1ec9u — Pinnacle\n"
        any_tot = False
        # Find the latest totals point captured for this match from Pinnacle
        totals_point = None
        for outcome in ("Over", "Under"):
            cur = get_current_odds(match_id, "totals", outcome, "pinnacle")
            if cur and cur.get("point") is not None:
                totals_point = cur["point"]
                break
        if totals_point is not None:
            for outcome in ("Over", "Under"):
                drift = compute_drift(match_id, "totals", outcome, "pinnacle", point=totals_point)
                if drift is None:
                    continue
                any_tot = True
                emoji = {"shortening": "\U0001f53b", "drifting": "\U0001f53a", "stable": "\u27a1\ufe0f"}[drift["direction"]]
                msg += (
                    f"  {emoji} {outcome} {totals_point:g}: {drift['opening_odds']:.2f} \u2192 "
                    f"{drift['current_odds']:.2f} ({drift['drift_pct']:+.2f}%)\n"
                )
        if not any_tot:
            msg += "  (ch\u01b0a c\u00f3 d\u1eef li\u1ec7u)\n"

        # --- Bookmaker comparison for Home 1X2 ---
        msg += f"\n\U0001f3e6 So s\u00e1nh nh\u00e0 c\u00e1i — c\u1eeda {match.home_team}\n"
        home_bks = get_all_bookmakers_current(match_id, "h2h", match.home_team)
        if home_bks:
            items = sorted(home_bks.items(), key=lambda x: x[1]["odds"], reverse=True)[:6]
            for _, info in items:
                msg += f"  \u2022 {info['bookmaker_name']}: {info['odds']:.2f}\n"
            odds_vals = [i[1]["odds"] for i in items]
            if odds_vals:
                spread_pct = (max(odds_vals) - min(odds_vals)) / min(odds_vals) * 100
                if spread_pct >= 5.0:
                    msg += f"\n\u26a0\ufe0f Ch\u00ean h l\u1ec7ch odds {spread_pct:.1f}% — arbitrage ti\u1ec1m n\u0103ng!\n"
        else:
            msg += "  (ch\u01b0a c\u00f3 d\u1eef li\u1ec7u)\n"

        msg += (
            "\n\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
            "Ch\u00fa th\u00edch: \U0001f53b shortening (odds gi\u1ea3m, ti\u1ec1n v\u00e0o)  "
            "\U0001f53a drifting (odds t\u0103ng, ti\u1ec1n r\u00fat)  "
            "\u27a1\ufe0f stable"
        )

        if len(msg) > 4000:
            msg = msg[:3980] + "\n... (c\u1eaft)"

        await update.message.reply_text(msg)
    except Exception as e:
        logger.error(f"[DongTien] Error: {e}", exc_info=True)
        await update.message.reply_text(f"\u274c L\u1ed7i /dongtien: {e}")
    finally:
        session.close()


async def cmd_clv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Hiển thị báo cáo Closing Line Value."""
    if not await _require_auth(update):
        return

    args = context.args or []
    days = 30
    if args:
        try:
            days = max(1, min(365, int(args[0])))
        except ValueError:
            await update.message.reply_text("\u274c days ph\u1ea3i l\u00e0 s\u1ed1 nguy\u00ean (1-365).")
            return

    from src.analytics.clv import get_clv_stats, format_clv_report
    try:
        stats = get_clv_stats(days=days)
        await update.message.reply_text(format_clv_report(stats))
    except Exception as e:
        logger.error(f"[CLV] Error: {e}", exc_info=True)
        await update.message.reply_text(f"\u274c L\u1ed7i /clv: {e}")


async def cmd_live_vb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Chạy live pipeline ngay bây giờ — tìm value bet trong trận đang live."""
    if not await _require_auth(update):
        return
    import asyncio
    from src.live_pipeline import run_live_pipeline

    await update.message.reply_text(
        "\U0001f525 \u0110ang qu\u00e9t tr\u1eadn live t\u00ecm value bet..."
    )
    try:
        loop = asyncio.get_event_loop()
        alerts = await loop.run_in_executor(None, run_live_pipeline)
    except Exception as e:
        logger.error(f"[live_vb] Error: {e}", exc_info=True)
        await update.message.reply_text(f"\u274c L\u1ed7i /live_vb: {e}")
        return

    if not alerts:
        await update.message.reply_text(
            "\u2139\ufe0f Kh\u00f4ng c\u00f3 live value bet n\u00e0o trong cycle n\u00e0y."
        )
        return
    for msg in alerts:
        await update.message.reply_text(msg)


async def cmd_theodoi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Hiển thị timeline state của 1 trận đang live: /theodoi <match_id>."""
    if not await _require_auth(update):
        return

    args = context.args or []
    if not args:
        await update.message.reply_text(
            "D\u00f9ng: /theodoi <match_id>\nV\u00ed d\u1ee5: /theodoi 12345"
        )
        return
    try:
        match_id = int(args[0])
    except ValueError:
        await update.message.reply_text("\u274c match_id ph\u1ea3i l\u00e0 s\u1ed1.")
        return

    from src.db.models import LiveMatchState, LivePrediction
    session = get_session()
    try:
        match = session.query(Match).filter(Match.match_id == match_id).first()
        if not match:
            await update.message.reply_text(f"\u274c Kh\u00f4ng t\u00ecm th\u1ea5y match_id {match_id}.")
            return

        states = (
            session.query(LiveMatchState)
            .filter(LiveMatchState.match_id == match_id)
            .order_by(LiveMatchState.captured_at.asc())
            .all()
        )
        if not states:
            await update.message.reply_text(
                f"\u2139\ufe0f Ch\u01b0a c\u00f3 snapshot live cho {match.home_team} vs {match.away_team}."
            )
            return

        preds = (
            session.query(LivePrediction)
            .filter(LivePrediction.match_id == match_id)
            .order_by(LivePrediction.created_at.asc())
            .all()
        )

        msg = (
            f"\U0001f440 LIVE TIMELINE \u2014 match_id {match_id}\n"
            f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
            f"{match.home_team} vs {match.away_team}\n"
            f"{match.competition or ''}\n\n"
            f"\U0001f4cc Snapshots ({len(states)}):\n"
        )
        for s in states[-10:]:
            msg += (
                f"  {s.captured_at.strftime('%H:%M')}  {s.minute}'  "
                f"{s.home_score}-{s.away_score}  "
                f"xG {s.home_xg:.2f}-{s.away_xg:.2f}  "
                f"SoT {s.home_shots_on_target}-{s.away_shots_on_target}"
            )
            if s.home_red_cards or s.away_red_cards:
                msg += f"  reds {s.home_red_cards}-{s.away_red_cards}"
            msg += "\n"

        if preds:
            msg += f"\n\U0001f525 Live value bets ({len(preds)}):\n"
            for p in preds[-10:]:
                msg += (
                    f"  {p.created_at.strftime('%H:%M')}  "
                    f"{p.minute}'  {p.market}:{p.outcome}  "
                    f"@ {p.live_odds:.2f}  EV {p.expected_value*100:+.1f}%  "
                    f"[{p.confidence}]\n"
                )
        msg += "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501"
        await update.message.reply_text(msg)
    finally:
        session.close()


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _require_auth(update): return
    await update.message.reply_text(
        "\U0001f4d6 H\u01b0\u1edbng d\u1eabn s\u1eed d\u1ee5ng:\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "/start \u2014 \u0110\u0103ng k\u00fd nh\u1eadn th\u00f4ng b\u00e1o\n"
        "/tatca \u2014 T\u1ea5t c\u1ea3 tr\u1eadn s\u1eafp di\u1ec5n ra\n"
        "/tatca PL \u2014 L\u1ecdc theo gi\u1ea3i (PL, PD, BL1, SA, FL1, CL)\n"
        "/phantich \u2014 Ph\u00e2n t\u00edch chi ti\u1ebft tr\u1eadn trong 24h\n"
        "/live \u2014 C\u00e1 c\u01b0\u1ee3c tr\u1ef1c ti\u1ebfp (in-play)\n"
        "\U0001f525 /live_vb \u2014 Qu\u00e9t live value bet ngay (LivePoisson)\n"
        "\U0001f440 /theodoi <match_id> \u2014 Timeline state + live VB c\u1ee7a 1 tr\u1eadn\n"
        "/today \u2014 Ph\u00e2n t\u00edch to\u00e0n b\u1ed9 h\u00f4m nay\n"
        "\U0001f3af /ancan \u2014 K\u00e8o d\u1ec5 th\u1eafng (Prob \u2265 60%, \u0111\u00e3 l\u1ecdc \u1ea3o)\n"
        "\U0001f4ca /dongtien <match_id> \u2014 Ph\u00e2n t\u00edch d\u00f2ng ti\u1ec1n (line movement)\n"
        "\U0001f4c8 /clv [days] \u2014 B\u00e1o c\u00e1o Closing Line Value\n"
        "/keoxien \u2014 K\u00e8o xi\u00ean 2\u201310 (parlay)\n"
        "/stats \u2014 Th\u1ed1ng k\u00ea hi\u1ec7u su\u1ea5t model\n"
        "/history \u2014 20 d\u1ef1 \u0111o\u00e1n g\u1ea7n nh\u1ea5t\n"
        "/xoa \u2014 Xo\u00e1 to\u00e0n b\u1ed9 l\u1ecbch s\u1eed ph\u00e2n t\u00edch\n"
        "/quanly list|add|remove|default \u2014 Qu\u1ea3n l\u00fd nh\u00e0 c\u00e1i\n"
        "/leagues \u2014 Danh s\u00e1ch gi\u1ea3i \u0111\u1ea5u\n"
        "/giahan \u2014 Ki\u1ec3m tra quota API\n"
        "/help \u2014 Tin nh\u1eafn n\u00e0y"
    )


async def send_alert(app: Application, message: str):
    """Send alert to all subscribers."""
    for chat_id in _subscribers:
        try:
            await app.bot.send_message(chat_id=chat_id, text=message)
        except Exception as e:
            logger.warning(f"Failed to send to {chat_id}: {e}")


# Track whether we already sent an alert at each threshold (avoid spam)
_quota_alerted = {"50": False, "10": False, "0": False}


async def check_quota_alert(app: Application):
    """Check API quota and send alert if low. Called after each analysis cycle."""
    from src.collectors.odds_api import get_quota
    q = get_quota()
    remaining = q.get("remaining")
    if remaining is None:
        return

    if remaining <= 0 and not _quota_alerted["0"]:
        _quota_alerted["0"] = True
        await send_alert(app,
            "\U0001f6a8 C\u1ea2NH B\u00c1O: THE ODDS API \u0110\u00c3 H\u1ebeT QUOTA!\n"
            "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
            f"C\u00f2n l\u1ea1i: {remaining} requests\n"
            "Bot s\u1ebd KH\u00d4NG th\u1ec3 l\u1ea5y odds cho \u0111\u1ebfn khi gia h\u1ea1n!\n"
            "\n\U0001f449 Gia h\u1ea1n ngay: https://the-odds-api.com"
        )
    elif remaining <= 10 and not _quota_alerted["10"]:
        _quota_alerted["10"] = True
        await send_alert(app,
            "\u26a0\ufe0f C\u1ea2NH B\u00c1O: API s\u1eafp h\u1ebft quota!\n"
            "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
            f"C\u00f2n l\u1ea1i: {remaining} requests\n"
            "Ch\u1ec9 c\u00f2n \u0111\u1ee7 cho ~1-2 l\u1ea7n ph\u00e2n t\u00edch n\u1eefa!\n"
            "\n\U0001f449 Gia h\u1ea1n t\u1ea1i: https://the-odds-api.com"
        )
    elif remaining <= 50 and not _quota_alerted["50"]:
        _quota_alerted["50"] = True
        await send_alert(app,
            "\U0001f7e1 TH\u00d4NG B\u00c1O: Quota API c\u00f2n \u00edt\n"
            "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
            f"C\u00f2n l\u1ea1i: {remaining} requests\n"
            "N\u00ean c\u00e2n nh\u1eafc gia h\u1ea1n s\u1edbm.\n"
            "\n\U0001f4a1 Ki\u1ec3m tra: /giahan"
        )


def get_subscribers() -> set[int]:
    return _subscribers


async def callback_league_picker(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle inline keyboard button clicks for multi-select league picker."""
    query = update.callback_query
    data = query.data

    if data == "noop":
        await query.answer()
        return

    if ":" not in data:
        await query.answer()
        return

    chat_id = update.effective_chat.id
    state = _picker_state.get(chat_id)

    parts = data.split(":", 2)
    action = parts[0]

    # --- Run across ALL leagues (bypass selection) ---
    if action == "allleagues" and len(parts) >= 2:
        command = parts[1]
        user_id = update.effective_user.id if update.effective_user else "?"
        logger.info(f"[picker] user={user_id} ALL-LEAGUES requested for /{command}")

        await query.answer(f"\u26a1 Ch\u1ea1y t\u1ea5t c\u1ea3 gi\u1ea3i...")
        await query.edit_message_text(
            f"\U0001f310 \u0110ang ch\u1ea1y /{command} tr\u00ean T\u1ea4T C\u1ea2 gi\u1ea3i..."
        )
        _picker_state.pop(chat_id, None)

        class _FakeUpdate:
            def __init__(self, real_update):
                self.message = real_update.callback_query.message
                self.effective_chat = real_update.effective_chat
                self.effective_user = real_update.effective_user
                self.callback_query = None

        fake = _FakeUpdate(update)
        try:
            if command == "phantich":
                await _run_all_leagues_phantich(fake, context)
            elif command == "live":
                await _run_all_live_summary(fake, context)
        except Exception as e:
            logger.error(f"[picker] ALL-LEAGUES /{command} failed: {e}", exc_info=True)
            try:
                await fake.message.reply_text(f"\u274c L\u1ed7i: {e}")
            except Exception:
                pass
        return

    # --- All-live pagination ---
    if action == "alllivep" and len(parts) >= 2:
        try:
            page = int(parts[1])
        except ValueError:
            await query.answer("Trang kh\u00f4ng h\u1ee3p l\u1ec7")
            return
        cache = context.user_data.get("_all_live_cache")
        if not cache or (_now_ts() - cache.get("ts", 0)) > 60:
            await query.answer("Cache h\u1ebft h\u1ea1n, ch\u1ea1y l\u1ea1i /live", show_alert=True)
            return
        await query.answer()
        await _send_all_live_page(update, cache["lines"], cache["total"], page=page)
        return

    # --- Picker pagination ---
    if action == "pickp" and len(parts) == 3:
        command, page_str = parts[1], parts[2]
        try:
            page = int(page_str)
        except ValueError:
            await query.answer()
            return
        if not state or state["command"] != command:
            live_data = _get_live_data() if command == "live" else {}
            state = {"command": command, "selected": set(), "live_data": live_data, "page": page}
            _picker_state[chat_id] = state
        else:
            state["page"] = page
        await query.answer()
        msg = _build_picker_msg(command, state["selected"], state["live_data"])
        kb = _build_picker_keyboard(command, state["selected"], state["live_data"], page=page)
        await query.edit_message_text(msg, reply_markup=kb)
        return

    # --- Toggle single league ---
    if action == "tog" and len(parts) == 3:
        command, code = parts[1], parts[2]
        if not state or state["command"] != command:
            live_data = _get_live_data() if command == "live" else {}
            state = {"command": command, "selected": set(), "live_data": live_data}
            _picker_state[chat_id] = state

        from src.config import LEAGUES_SHORT, LEAGUES as _L
        _nm = LEAGUES_SHORT.get(code, _L.get(code, code))
        if code in state["selected"]:
            state["selected"].discard(code)
            await query.answer(f"\u274c B\u1ecf {_nm}")
        else:
            state["selected"].add(code)
            await query.answer(f"\u2705 Ch\u1ecdn {_nm}")

        # Update message + keyboard
        msg = _build_picker_msg(command, state["selected"], state["live_data"])
        kb = _build_picker_keyboard(command, state["selected"], state["live_data"], page=state.get("page", 0))
        await query.edit_message_text(msg, reply_markup=kb)

    # --- Toggle entire region ---
    elif action == "region" and len(parts) == 3:
        command = parts[1]
        codes = parts[2].split(",")
        if not state or state["command"] != command:
            live_data = _get_live_data() if command == "live" else {}
            state = {"command": command, "selected": set(), "live_data": live_data}
            _picker_state[chat_id] = state

        # If all in region selected -> deselect all, else select all
        all_selected = all(c in state["selected"] for c in codes)
        if all_selected:
            for c in codes:
                state["selected"].discard(c)
            await query.answer(f"\u274c B\u1ecf ch\u1ecdn khu v\u1ef1c")
        else:
            for c in codes:
                state["selected"].add(c)
            await query.answer(f"\u2705 Ch\u1ecdn khu v\u1ef1c ({len(codes)} gi\u1ea3i)")

        msg = _build_picker_msg(command, state["selected"], state["live_data"])
        kb = _build_picker_keyboard(command, state["selected"], state["live_data"], page=state.get("page", 0))
        await query.edit_message_text(msg, reply_markup=kb)

    # --- Select ALL leagues (tick every checkbox) ---
    elif action == "selectall" and len(parts) >= 2:
        command = parts[1]
        from src.config import LEAGUES as _L_ALL
        if not state or state["command"] != command:
            live_data = _get_live_data() if command == "live" else {}
            state = {"command": command, "selected": set(), "live_data": live_data, "page": 0}
            _picker_state[chat_id] = state

        state["selected"] = set(_L_ALL.keys())
        await query.answer(f"\u2705 Ch\u1ecdn t\u1ea5t c\u1ea3 {len(state['selected'])} gi\u1ea3i")

        msg = _build_picker_msg(command, state["selected"], state["live_data"])
        kb = _build_picker_keyboard(command, state["selected"], state["live_data"], page=state.get("page", 0))
        await query.edit_message_text(msg, reply_markup=kb)

    # --- Select all live leagues ---
    elif action == "alllive" and len(parts) >= 2:
        command = parts[1]
        if not state or state["command"] != command:
            live_data = _get_live_data() if command == "live" else {}
            state = {"command": command, "selected": set(), "live_data": live_data}
            _picker_state[chat_id] = state

        live_codes = set(state["live_data"].keys())
        if live_codes:
            state["selected"] = live_codes.copy()
            await query.answer(f"\u2705 Ch\u1ecdn {len(live_codes)} gi\u1ea3i \u0111ang live")
        else:
            await query.answer("\u26bd Kh\u00f4ng c\u00f3 gi\u1ea3i live")

        msg = _build_picker_msg(command, state["selected"], state["live_data"])
        kb = _build_picker_keyboard(command, state["selected"], state["live_data"], page=state.get("page", 0))
        await query.edit_message_text(msg, reply_markup=kb)

    # --- Clear all ---
    elif action == "clear" and len(parts) >= 2:
        command = parts[1]
        if state:
            state["selected"] = set()
        else:
            state = {"command": command, "selected": set(), "live_data": {}}
            _picker_state[chat_id] = state
        await query.answer("\u274c \u0110\u00e3 b\u1ecf ch\u1ecdn t\u1ea5t c\u1ea3")

        msg = _build_picker_msg(command, state["selected"], state["live_data"])
        kb = _build_picker_keyboard(command, state["selected"], state["live_data"], page=state.get("page", 0))
        await query.edit_message_text(msg, reply_markup=kb)

    # --- Run command with selected leagues ---
    elif action == "run" and len(parts) >= 2:
        command = parts[1]
        if not state or not state.get("selected"):
            await query.answer("\u26a0 Ch\u01b0a ch\u1ecdn gi\u1ea3i n\u00e0o!", show_alert=True)
            return

        from src.config import LEAGUES as _L_RUN
        selected = list(state["selected"])
        is_all = len(selected) >= len(_L_RUN)

        await query.answer(f"\u26a1 \u0110ang ch\u1ea1y {len(selected)} gi\u1ea3i...")

        # Remove keyboard from picker message
        if is_all:
            banner = (
                "\U0001f310 Ph\u00e2n t\u00edch T\u1ea4T C\u1ea2 gi\u1ea3i..."
                if command == "phantich"
                else "\U0001f310 Xem LIVE t\u1ea5t c\u1ea3 gi\u1ea3i..."
            )
            await query.edit_message_text(banner)
        else:
            sel_names = ", ".join(sorted(selected))
            await query.edit_message_text(
                f"\u2705 \u0110ang {'ph\u00e2n t\u00edch' if command == 'phantich' else 'xem live'}: {sel_names}"
            )

        # Clean up state
        _picker_state.pop(chat_id, None)

        # Create fake update pointing to callback message for replies
        class _FakeUpdate:
            def __init__(self, real_update):
                self.message = real_update.callback_query.message
                self.effective_chat = real_update.effective_chat
                self.effective_user = real_update.effective_user
                self.callback_query = None

        fake = _FakeUpdate(update)

        # Run command for selected leagues. All-selected shortcut delegates to
        # the compact top-20 / grouped-live helpers (with built-in quota guard
        # for /phantich).
        try:
            if command == "phantich":
                if is_all:
                    await _run_all_leagues_phantich(fake, context)
                else:
                    await _run_full_analysis(fake, league_codes=sorted(selected))
            elif command == "live":
                if is_all:
                    await _run_all_live_summary(fake, context)
                else:
                    for league_code in sorted(selected):
                        context.args = [league_code]
                        await cmd_live(fake, context)
        except Exception as e:
            logger.error(f"[Picker] Error running {command}: {e}")

    else:
        await query.answer()


def create_bot_app() -> Application:
    """Create and configure the Telegram bot application."""
    from telegram.request import HTTPXRequest
    request = HTTPXRequest(connect_timeout=20, read_timeout=60, write_timeout=60)
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).request(request).build()

    app.add_handler(CommandHandler("login", cmd_login))
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("tatca", cmd_matches))
    app.add_handler(CommandHandler("phantich", cmd_analyze))
    app.add_handler(CommandHandler("dongtien", cmd_dongtien))
    app.add_handler(CommandHandler("clv", cmd_clv))
    app.add_handler(CommandHandler("live_vb", cmd_live_vb))
    app.add_handler(CommandHandler("theodoi", cmd_theodoi))
    app.add_handler(CommandHandler("today", cmd_today))
    app.add_handler(CommandHandler("keoxien", cmd_keoxien))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("history", cmd_history))
    app.add_handler(CommandHandler("xoa", cmd_xoa))
    app.add_handler(CommandHandler("quanly", cmd_bookie))
    app.add_handler(CommandHandler("leagues", cmd_leagues))
    app.add_handler(CommandHandler("giahan", cmd_quota))
    app.add_handler(CommandHandler("live", cmd_live))
    app.add_handler(CommandHandler("ancan", cmd_ancan))
    app.add_handler(CommandHandler("dethang", cmd_ancan))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CallbackQueryHandler(callback_league_picker))

    return app
