"""Special-match classifier: derby, cup final, knockout, relegation 6-pointer.

Tags each fixture with context flags that inform λ adjustments in the Dixon-Coles
and Bivariate Poisson models. Lookups are symmetric: `tuple(sorted([h, a]))`.

Derby list is hand-curated — only rivalries that are both well-known AND have
documented scoring deviation from league baseline (tempo/intensity). Ambiguous
pairs omitted on purpose; adding a bad entry is worse than missing a good one.

Usage from pipeline.py:

    from src.analytics.match_context import classify_match
    ctx = classify_match(home, away, competition_code, stage=None)
    prediction = model.predict(home, away, match_context=ctx, ...)
"""

from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def _canon(name: str) -> str:
    """Lower + strip for derby lookup key. Not a full team-name normalizer —
    Phase B will deliver that. Here we only need enough to match the exact
    strings stored below, which are themselves already lowercased."""
    return (name or "").lower().strip()


# ---------------------------------------------------------------
# DERBY pairs — frozenset of sorted tuples, symmetric lookup.
# ---------------------------------------------------------------
# Conservative list. Only classic local/historical rivalries. Team names use
# the lowercase form that most commonly appears in Football-Data.org payloads
# (the primary source of Match.home_team/away_team strings). When Phase B
# lands, we'll rekey this on canonical IDs.
DERBY_PAIRS: frozenset[tuple[str, str]] = frozenset(
    tuple(sorted((a.lower(), b.lower()))) for a, b in [
        # --- ENGLAND ---
        ("manchester united fc", "liverpool fc"),
        ("manchester united fc", "manchester city fc"),
        ("manchester united fc", "leeds united fc"),
        ("liverpool fc", "everton fc"),
        ("liverpool fc", "manchester city fc"),
        ("arsenal fc", "tottenham hotspur fc"),
        ("arsenal fc", "chelsea fc"),
        ("chelsea fc", "tottenham hotspur fc"),
        ("chelsea fc", "fulham fc"),
        ("newcastle united fc", "sunderland afc"),
        ("aston villa fc", "birmingham city fc"),
        ("west ham united fc", "tottenham hotspur fc"),
        # --- SPAIN ---
        ("real madrid cf", "fc barcelona"),
        ("real madrid cf", "club atlético de madrid"),
        ("club atlético de madrid", "fc barcelona"),
        ("sevilla fc", "real betis balompié"),
        ("athletic club", "real sociedad de fútbol"),
        ("valencia cf", "villarreal cf"),
        # --- ITALY ---
        ("fc internazionale milano", "ac milan"),
        ("as roma", "ss lazio"),
        ("juventus fc", "torino fc"),
        ("juventus fc", "fc internazionale milano"),
        ("juventus fc", "ac milan"),
        ("genoa cfc", "uc sampdoria"),
        ("ssc napoli", "as roma"),
        # --- GERMANY ---
        ("borussia dortmund", "fc schalke 04"),
        ("borussia dortmund", "fc bayern münchen"),
        ("bayer 04 leverkusen", "1. fc köln"),
        ("hamburger sv", "werder bremen"),
        ("hertha bsc", "1. fc union berlin"),
        # --- FRANCE ---
        ("paris saint-germain fc", "olympique de marseille"),
        ("olympique lyonnais", "as saint-étienne"),
        ("olympique de marseille", "olympique lyonnais"),
        # --- NETHERLANDS ---
        ("afc ajax", "feyenoord rotterdam"),
        ("afc ajax", "psv"),
        ("feyenoord rotterdam", "psv"),
        # --- PORTUGAL ---
        ("sl benfica", "fc porto"),
        ("sl benfica", "sporting cp"),
        ("fc porto", "sporting cp"),
        # --- SCOTLAND (Old Firm) ---
        ("celtic fc", "rangers fc"),
        # --- TURKEY ---
        ("galatasaray", "fenerbahçe"),
        ("galatasaray sk", "fenerbahçe sk"),
        ("beşiktaş", "fenerbahçe"),
        # --- GREECE ---
        ("olympiacos", "panathinaikos"),
        ("olympiakos piraeus", "panathinaikos fc"),
        # --- ARGENTINA (Superclásico) ---
        ("club atlético river plate", "club atlético boca juniors"),
        ("river plate", "boca juniors"),
        # --- BRAZIL ---
        ("flamengo", "fluminense"),
        ("são paulo", "corinthians"),
        ("palmeiras", "corinthians"),
    ]
)


# ---------------------------------------------------------------
# Knockout / cup classification
# ---------------------------------------------------------------
# League codes where the format is ALWAYS knockout past the group stage,
# or pure cup. Matches the LEAGUES dict in src/config.py.
KNOCKOUT_COMPETITIONS: frozenset[str] = frozenset({
    # English cups
    "FAC",
    # Spanish cup
    "CDR",
    # German cup
    "DFB",
    # French cup
    "CDF",
    # UEFA — group + knockout; stage field decides knockout proper
    "CL", "EL", "ECL",
    # South American cups
    "COP", "CSU",
    # International tournaments
    "WC", "EC", "CAM", "AFN",
})

CUP_COMPETITIONS: frozenset[str] = frozenset({
    "FAC", "CDR", "DFB", "CDF",
    "CL", "EL", "ECL", "COP", "CSU",
    "WC", "EC", "CAM", "AFN", "NL",
})


# Stages where cup final adjustment applies.
FINAL_STAGES: frozenset[str] = frozenset({
    "final", "FINAL", "Final", "f",
})

KNOCKOUT_STAGES: frozenset[str] = frozenset({
    "r16", "R16", "round_of_16", "ROUND_OF_16", "Round of 16",
    "qf", "QF", "quarter_finals", "QUARTER_FINALS", "Quarter Finals",
    "sf", "SF", "semi_finals", "SEMI_FINALS", "Semi Finals",
    "final", "FINAL", "Final",
    "playoff", "play-off",
})


def _normalize_stage(stage: Optional[str]) -> Optional[str]:
    if not stage:
        return None
    return stage.strip().lower().replace(" ", "_").replace("-", "_")


def is_derby(home: str, away: str) -> bool:
    key = tuple(sorted([_canon(home), _canon(away)]))
    return key in DERBY_PAIRS


def classify_match(
    home: str,
    away: str,
    competition_code: Optional[str] = None,
    stage: Optional[str] = None,
) -> dict:
    """Tag a fixture with special-match context flags.

    Args:
        home, away: team names (whatever the upstream payload gave us).
        competition_code: LEAGUES code, e.g. "PL", "CL", "FAC".
        stage: optional knockout stage string from odds/fixtures API —
               "Final", "Semi Finals", "Quarter Finals", "Round of 16", etc.

    Returns a dict shaped for model.predict(match_context=...).
    All flags default False; tournament_stage may be None.
    """
    ctx = {
        "is_derby": False,
        "is_cup_final": False,
        "is_knockout": False,
        "is_relegation_6pointer": False,  # reserved for Phase B — needs league table
        "tournament_stage": None,
    }

    ctx["is_derby"] = is_derby(home, away)

    norm_stage = _normalize_stage(stage)
    if norm_stage:
        ctx["tournament_stage"] = norm_stage

    if competition_code:
        if competition_code in CUP_COMPETITIONS and norm_stage == "final":
            ctx["is_cup_final"] = True
        if competition_code in KNOCKOUT_COMPETITIONS and norm_stage in {
            "r16", "round_of_16",
            "qf", "quarter_finals",
            "sf", "semi_finals",
            "final",
            "playoff",
        }:
            ctx["is_knockout"] = True

    return ctx


# ---------------------------------------------------------------
# λ adjustments applied when USE_MATCH_CONTEXT == "on".
# Additive on the marginal goal rates (λ_home, λ_away). Values chosen
# conservatively — big enough to matter, small enough that a bad classification
# on 1-2 matches a week does not derail the model.
# ---------------------------------------------------------------
LAMBDA_ADJUSTMENTS = {
    "is_derby":                 (+0.15, +0.15),
    "is_cup_final":             (-0.20, -0.20),
    "is_knockout":              (-0.10, -0.10),
    "is_relegation_6pointer":   (-0.15, -0.15),
}


def apply_lambda_adjustment(
    lam_home: float,
    lam_away: float,
    context: Optional[dict],
) -> tuple[float, float]:
    """Apply additive λ adjustments for special-match context flags.

    Only called when config USE_MATCH_CONTEXT == "on". "log_only" mode
    skips this path and passes the unadjusted λ through.

    Adjustments compound across flags (derby + knockout = derby + knockout
    effects both applied), floored at 0.1 so models stay well-defined.
    """
    if not context:
        return lam_home, lam_away

    dh = 0.0
    da = 0.0
    for flag, (adj_h, adj_a) in LAMBDA_ADJUSTMENTS.items():
        if context.get(flag):
            dh += adj_h
            da += adj_a

    if dh == 0.0 and da == 0.0:
        return lam_home, lam_away

    new_h = max(0.1, lam_home + dh)
    new_a = max(0.1, lam_away + da)
    return new_h, new_a


def context_summary(context: Optional[dict]) -> str:
    """One-line human-readable context tag for logs/alerts. Returns "" if no flags."""
    if not context:
        return ""
    parts = []
    if context.get("is_derby"):
        parts.append("Derby")
    if context.get("is_cup_final"):
        parts.append("Chung kết")
    if context.get("is_knockout"):
        parts.append("Knockout")
    if context.get("is_relegation_6pointer"):
        parts.append("6-pointer trụ hạng")
    if not parts and context.get("tournament_stage"):
        parts.append(f"Stage={context['tournament_stage']}")
    return " | ".join(parts)
