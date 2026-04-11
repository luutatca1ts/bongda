"""Telegram message formatters."""

from datetime import datetime


CONFIDENCE_EMOJI = {
    "HIGH": "\U0001f534",    # 🔴
    "MEDIUM": "\U0001f7e1",  # 🟡
    "LOW": "\U0001f7e2",     # 🟢
}

RESULT_EMOJI = {
    "WIN": "\u2705",   # ✅
    "LOSE": "\u274c",  # ❌
    "PUSH": "\u2796",  # ➖
}


def format_value_bet_alert(match: dict, bet: dict, prediction: dict, all_bookmaker_odds: dict | None = None) -> str:
    """Format a value bet alert for Telegram."""
    conf_emoji = CONFIDENCE_EMOJI.get(bet.get("confidence", "LOW"), "\U0001f7e2")

    # Parse date
    utc_str = match.get("utc_date", "")
    try:
        dt = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
        time_str = dt.strftime("%H:%M - %d/%m/%Y")
    except Exception:
        time_str = utc_str

    msg = (
        f"\u26bd VALUE BET DETECTED\n"
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        f"\U0001f3c6 {match.get('competition', 'N/A')}\n"
        f"\U0001f552 {time_str}\n\n"
        f"{match['home_team']} vs {match['away_team']}\n\n"
        f"\U0001f4ca Ph\u00e2n t\u00edch:\n"
        f"  \u2022 K\u00e8o: {bet['outcome']} ({bet['market']})\n"
        f"  \u2022 Odds: {bet['odds']}\n"
        f"  \u2022 Model Probability: {bet['probability']*100:.1f}%\n"
        f"  \u2022 Expected Value: {bet['ev']*100:+.1f}%\n"
        f"  \u2022 Confidence: {conf_emoji} {bet.get('confidence', 'N/A')}\n"
    )

    # Add xG info
    if prediction:
        msg += (
            f"\n\u26a1 Expected Goals:\n"
            f"  {match['home_team']}: {prediction.get('home_xg', '?')}\n"
            f"  {match['away_team']}: {prediction.get('away_xg', '?')}\n"
        )

    # Add multi-bookmaker odds comparison
    if all_bookmaker_odds:
        msg += f"\n\U0001f4b0 Odds comparison:\n"
        for bk_name, odds_val in list(all_bookmaker_odds.items())[:5]:
            msg += f"  {bk_name}: {odds_val}\n"

    msg += f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501"
    return msg


def format_daily_report(report: dict) -> str:
    """Format daily summary report."""
    date_str = report.get("date", "N/A")
    total = report.get("total_picks", 0)
    correct = report.get("correct", 0)
    wrong = report.get("wrong", 0)
    pending = report.get("pending", 0)
    hit_rate = (correct / total * 100) if total > 0 else 0

    h_correct = report.get("high_correct", 0)
    h_total = report.get("high_total", 0)
    m_correct = report.get("medium_correct", 0)
    m_total = report.get("medium_total", 0)
    l_correct = report.get("low_correct", 0)
    l_total = report.get("low_total", 0)

    msg = (
        f"\U0001f4ca DAILY REPORT \u2014 {date_str}\n"
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        f"Total picks: {total}\n"
        f"\u2705 Correct: {correct}\n"
        f"\u274c Wrong: {wrong}\n"
        f"\u23f3 Pending: {pending}\n"
        f"\U0001f4c8 Hit rate: {hit_rate:.1f}%\n\n"
        f"\U0001f534 HIGH: {h_correct}/{h_total}\n"
        f"\U0001f7e1 MEDIUM: {m_correct}/{m_total}\n"
        f"\U0001f7e2 LOW: {l_correct}/{l_total}\n"
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501"
    )
    return msg


def format_bookmaker_list(bookmakers: list[dict]) -> str:
    """Format bookmaker list."""
    if not bookmakers:
        return "\U0001f4cb Danh s\u00e1ch nh\u00e0 c\u00e1i tr\u1ed1ng. D\u00f9ng /bookie add <t\u00ean> <url> \u0111\u1ec3 th\u00eam."

    msg = (
        f"\U0001f4cb Danh s\u00e1ch nh\u00e0 c\u00e1i:\n"
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
    )
    for i, bk in enumerate(bookmakers, 1):
        star = "\u2b50 " if bk.get("is_default") else "   "
        msg += f"{i}. {star}{bk['name']} \u2014 {bk.get('url', 'N/A')}\n"
    msg += f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501"
    return msg


def format_stats(stats: dict) -> str:
    """Format performance stats."""
    msg = (
        f"\U0001f4c8 PERFORMANCE STATS\n"
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        f"Total predictions: {stats.get('total', 0)}\n"
        f"Resolved: {stats.get('resolved', 0)}\n"
        f"Win rate: {stats.get('win_rate', 0):.1f}%\n\n"
        f"By confidence:\n"
        f"  \U0001f534 HIGH: {stats.get('high_wins', 0)}/{stats.get('high_total', 0)}\n"
        f"  \U0001f7e1 MEDIUM: {stats.get('med_wins', 0)}/{stats.get('med_total', 0)}\n"
        f"  \U0001f7e2 LOW: {stats.get('low_wins', 0)}/{stats.get('low_total', 0)}\n\n"
        f"By market:\n"
    )
    for market, data in stats.get("by_market", {}).items():
        msg += f"  {market}: {data['wins']}/{data['total']} ({data['rate']:.0f}%)\n"
    msg += f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501"
    return msg
