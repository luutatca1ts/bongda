import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
FOOTBALL_DATA_API_KEY = os.getenv("FOOTBALL_DATA_API_KEY")
ODDS_API_KEY = os.getenv("ODDS_API_KEY")
API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "")
BOT_PASSWORD = os.getenv("BOT_PASSWORD", "")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///football_analytics.db")

# ================================================================
# SUPPORTED LEAGUES — organized by region
# ================================================================

# All leagues: code -> display name (tiếng Việt, tên đầy đủ)
# Synced with Pinnacle coverage on The Odds API (47 verified + DED/DFB off-season
# + cyclical international tournaments). Source: scripts/check_pinnacle.py
LEAGUES = {
    # --- ANH ---
    "PL": "Giải Ngoại hạng Anh",
    "ELC": "Giải hạng Nhất Anh",
    "EL1": "Giải hạng Hai Anh",
    "EL2": "Giải hạng Ba Anh",
    "FAC": "Cúp FA Anh",
    # --- TÂY BAN NHA ---
    "PD": "Giải Vô địch Quốc gia Tây Ban Nha (La Liga)",
    "SD": "Giải hạng Hai Tây Ban Nha (La Liga 2)",
    "CDR": "Cúp Nhà vua Tây Ban Nha",
    # --- ĐỨC ---
    "BL1": "Giải Vô địch Quốc gia Đức (Bundesliga)",
    "BL2": "Giải hạng Hai Đức (Bundesliga 2)",
    "BL3": "Giải hạng Ba Đức (3. Liga)",
    "DFB": "Cúp Quốc gia Đức",
    # --- Ý ---
    "SA": "Giải Vô địch Quốc gia Ý (Serie A)",
    "SB": "Giải hạng Hai Ý (Serie B)",
    # --- PHÁP ---
    "FL1": "Giải Vô địch Quốc gia Pháp (Ligue 1)",
    "FL2": "Giải hạng Hai Pháp (Ligue 2)",
    "CDF": "Cúp Quốc gia Pháp",
    # --- HÀ LAN ---
    "DED": "Giải Vô địch Quốc gia Hà Lan (Eredivisie)",
    # --- BỒ ĐÀO NHA ---
    "PPL": "Giải Vô địch Quốc gia Bồ Đào Nha",
    # --- BỈ ---
    "JPL": "Giải Vô địch Quốc gia Bỉ",
    # --- THỔ NHĨ KỲ ---
    "TSL": "Giải Vô địch Quốc gia Thổ Nhĩ Kỳ",
    # --- HY LẠP ---
    "GSL": "Giải Vô địch Quốc gia Hy Lạp",
    # --- ÁO ---
    "ABL": "Giải Vô địch Quốc gia Áo (Bundesliga Áo)",
    # --- THỤY SĨ ---
    "SSL": "Giải Vô địch Quốc gia Thụy Sĩ",
    # --- ĐAN MẠCH ---
    "DSL": "Giải Vô địch Quốc gia Đan Mạch",
    # --- THỤY ĐIỂN ---
    "ASV": "Giải Vô địch Quốc gia Thụy Điển (Allsvenskan)",
    "SUP": "Giải hạng Hai Thụy Điển (Superettan)",
    # --- NA UY ---
    "NES": "Giải Vô địch Quốc gia Na Uy",
    # --- BA LAN ---
    "EPK": "Giải Vô địch Quốc gia Ba Lan",
    # --- NGA ---
    "RPL": "Giải Vô địch Quốc gia Nga",
    # --- IRELAND ---
    "LOI": "Giải Vô địch Quốc gia Ireland",
    # --- PHẦN LAN ---
    "VLG": "Giải Vô địch Quốc gia Phần Lan",
    # --- UEFA ---
    "CL": "Cúp C1 Châu Âu (Champions League)",
    "EL": "Cúp C2 Châu Âu (Europa League)",
    "ECL": "Cúp C3 Châu Âu (Conference League)",
    # --- NAM MỸ ---
    "BSA": "Giải Vô địch Quốc gia Brazil",
    "BSB": "Giải hạng Hai Brazil",
    "ALP": "Giải Vô địch Quốc gia Argentina",
    "CHI": "Giải Vô địch Quốc gia Chile",
    "COP": "Cúp Nam Mỹ các Câu lạc bộ (Copa Libertadores)",
    "CSU": "Cúp Nam Mỹ hạng Hai (Copa Sudamericana)",
    # --- BẮC MỸ ---
    "MLS": "Giải Nhà nghề Mỹ (MLS)",
    "LMX": "Giải Vô địch Quốc gia Mexico (Liga MX)",
    # --- CHÂU Á ---
    "JL1": "Giải Vô địch Quốc gia Nhật Bản (J1 League)",
    "KL1": "Giải Vô địch Quốc gia Hàn Quốc (K League 1)",
    "CSL": "Giải Vô địch Quốc gia Trung Quốc",
    "SPL2": "Giải Vô địch Quốc gia Ả Rập Saudi",
    # --- CHÂU ĐẠI DƯƠNG ---
    "ALG": "Giải Vô địch Quốc gia Úc (A-League)",
    # --- QUỐC TẾ (giải theo chu kỳ) ---
    "WC": "Cúp Thế giới FIFA",
    "EC": "Giải Vô địch Bóng đá Châu Âu (EURO)",
    "CAM": "Cúp Bóng đá Nam Mỹ (Copa América)",
    "NL": "Giải Vô địch các Quốc gia Châu Âu (Nations League)",
    "AFN": "Cúp các Quốc gia Châu Phi",
}

# Short Vietnamese labels for Telegram picker buttons (fit ~3 per row)
# LEAGUES keeps the full names for /leagues and message text; this dict is
# used only for inline-keyboard buttons where long text wraps poorly.
LEAGUES_SHORT = {
    # Anh
    "PL": "Ngoại hạng Anh",
    "ELC": "Hạng Nhất Anh",
    "EL1": "Hạng Hai Anh",
    "EL2": "Hạng Ba Anh",
    "FAC": "Cúp FA Anh",
    # Tây Ban Nha
    "PD": "La Liga",
    "SD": "La Liga 2",
    "CDR": "Cúp Nhà vua TBN",
    # Đức
    "BL1": "Bundesliga",
    "BL2": "Bundesliga 2",
    "BL3": "Hạng Ba Đức",
    "DFB": "Cúp Đức",
    # Ý
    "SA": "Serie A",
    "SB": "Serie B",
    # Pháp
    "FL1": "Ligue 1",
    "FL2": "Ligue 2",
    "CDF": "Cúp Pháp",
    # Hà Lan
    "DED": "Eredivisie",
    # Bồ Đào Nha
    "PPL": "Bồ Đào Nha",
    # Bỉ
    "JPL": "Bỉ Pro League",
    # Thổ Nhĩ Kỳ
    "TSL": "Süper Lig",
    # Hy Lạp
    "GSL": "Hy Lạp",
    # Áo
    "ABL": "Bundesliga Áo",
    # Thụy Sĩ
    "SSL": "Thụy Sĩ",
    # Đan Mạch
    "DSL": "Đan Mạch",
    # Thụy Điển
    "ASV": "Allsvenskan",
    "SUP": "Superettan",
    # Na Uy
    "NES": "Eliteserien",
    # Ba Lan
    "EPK": "Ekstraklasa",
    # Nga
    "RPL": "Premier Nga",
    # Ireland
    "LOI": "Ireland",
    # Phần Lan
    "VLG": "Veikkausliiga",
    # UEFA
    "CL": "Cúp C1",
    "EL": "Cúp C2",
    "ECL": "Cúp C3",
    # Nam Mỹ
    "BSA": "Brazil Serie A",
    "BSB": "Brazil Serie B",
    "ALP": "Argentina",
    "CHI": "Chile",
    "COP": "Libertadores",
    "CSU": "Sudamericana",
    # Bắc Mỹ
    "MLS": "MLS",
    "LMX": "Liga MX",
    # Châu Á
    "JL1": "J1 League",
    "KL1": "K League 1",
    "CSL": "Trung Quốc",
    "SPL2": "Ả Rập Saudi",
    # Châu Đại Dương
    "ALG": "A-League",
    # Quốc tế
    "WC": "World Cup",
    "EC": "EURO",
    "CAM": "Copa América",
    "NL": "Nations League",
    "AFN": "Cúp Châu Phi",
}

# League regions for /leagues display
LEAGUE_REGIONS = {
    "🏴󠁧󠁢󠁥󠁮󠁧󠁿 ANH": ["PL", "ELC", "EL1", "EL2", "FAC"],
    "🇪🇸 TÂY BAN NHA": ["PD", "SD", "CDR"],
    "🇩🇪 ĐỨC": ["BL1", "BL2", "BL3", "DFB"],
    "🇮🇹 Ý": ["SA", "SB"],
    "🇫🇷 PHÁP": ["FL1", "FL2", "CDF"],
    "🇳🇱 HÀ LAN": ["DED"],
    "🇵🇹 BỒ ĐÀO NHA": ["PPL"],
    "🇧🇪 BỈ": ["JPL"],
    "🇹🇷 THỔ NHĨ KỲ": ["TSL"],
    "🇬🇷 HY LẠP": ["GSL"],
    "🇦🇹 ÁO": ["ABL"],
    "🇨🇭 THỤY SĨ": ["SSL"],
    "🇩🇰 ĐAN MẠCH": ["DSL"],
    "🇸🇪 THỤY ĐIỂN": ["ASV", "SUP"],
    "🇳🇴 NA UY": ["NES"],
    "🇵🇱 BA LAN": ["EPK"],
    "🇷🇺 NGA": ["RPL"],
    "🇮🇪 IRELAND": ["LOI"],
    "🇫🇮 PHẦN LAN": ["VLG"],
    "🏆 UEFA": ["CL", "EL", "ECL"],
    "🌎 NAM MỸ": ["BSA", "BSB", "ALP", "CHI", "COP", "CSU"],
    "🌎 BẮC MỸ": ["MLS", "LMX"],
    "🌏 CHÂU Á": ["JL1", "KL1", "CSL", "SPL2"],
    "🌏 CHÂU ĐẠI DƯƠNG": ["ALG"],
    "🌍 QUỐC TẾ": ["WC", "EC", "CAM", "NL", "AFN"],
}

# The Odds API sport keys — verified 2026-04-13 via scripts/check_pinnacle.py
ODDS_SPORTS = {
    # England
    "PL": "soccer_epl",
    "ELC": "soccer_efl_champ",
    "EL1": "soccer_england_league1",
    "EL2": "soccer_england_league2",
    "FAC": "soccer_fa_cup",
    # Spain
    "PD": "soccer_spain_la_liga",
    "SD": "soccer_spain_segunda_division",
    "CDR": "soccer_spain_copa_del_rey",
    # Germany
    "BL1": "soccer_germany_bundesliga",
    "BL2": "soccer_germany_bundesliga2",
    "BL3": "soccer_germany_liga3",
    "DFB": "soccer_germany_dfb_pokal",
    # Italy
    "SA": "soccer_italy_serie_a",
    "SB": "soccer_italy_serie_b",
    # France
    "FL1": "soccer_france_ligue_one",
    "FL2": "soccer_france_ligue_two",
    "CDF": "soccer_france_coupe_de_france",
    # Netherlands
    "DED": "soccer_netherlands_eredivisie",
    # Portugal
    "PPL": "soccer_portugal_primeira_liga",
    # Belgium
    "JPL": "soccer_belgium_first_div",
    # Turkey
    "TSL": "soccer_turkey_super_league",
    # Greece
    "GSL": "soccer_greece_super_league",
    # Austria
    "ABL": "soccer_austria_bundesliga",
    # Switzerland
    "SSL": "soccer_switzerland_superleague",
    # Denmark
    "DSL": "soccer_denmark_superliga",
    # Sweden
    "ASV": "soccer_sweden_allsvenskan",
    "SUP": "soccer_sweden_superettan",
    # Norway
    "NES": "soccer_norway_eliteserien",
    # Poland
    "EPK": "soccer_poland_ekstraklasa",
    # Russia
    "RPL": "soccer_russia_premier_league",
    # Ireland
    "LOI": "soccer_league_of_ireland",
    # Finland
    "VLG": "soccer_finland_veikkausliiga",
    # UEFA
    "CL": "soccer_uefa_champs_league",
    "EL": "soccer_uefa_europa_league",
    "ECL": "soccer_uefa_europa_conference_league",
    # South America
    "BSA": "soccer_brazil_campeonato",
    "BSB": "soccer_brazil_serie_b",
    "ALP": "soccer_argentina_primera_division",
    "CHI": "soccer_chile_campeonato",
    "COP": "soccer_conmebol_copa_libertadores",
    "CSU": "soccer_conmebol_copa_sudamericana",
    # North America
    "MLS": "soccer_usa_mls",
    "LMX": "soccer_mexico_ligamx",
    # Asia
    "JL1": "soccer_japan_j_league",
    "KL1": "soccer_korea_kleague1",
    "CSL": "soccer_china_superleague",
    "SPL2": "soccer_saudi_arabia_pro_league",
    # Oceania
    "ALG": "soccer_australia_aleague",
    # International (cyclical — sport key exists but inactive outside tournament)
    "WC": "soccer_fifa_world_cup",
    "EC": "soccer_uefa_european_championship",
    "CAM": "soccer_conmebol_copa_america",
    "NL": "soccer_uefa_nations_league",
    "AFN": "soccer_africa_cup_of_nations",
}

# API-Football league IDs (api-sports.io)
API_FOOTBALL_LEAGUES = {
    # England
    "PL": 39,
    "ELC": 40,
    "EL1": 41,
    "EL2": 42,
    "FAC": 45,
    # Spain
    "PD": 140,
    "SD": 141,
    "CDR": 143,
    # Germany
    "BL1": 78,
    "BL2": 79,
    "BL3": 80,
    "DFB": 81,
    # Italy
    "SA": 135,
    "SB": 136,
    # France
    "FL1": 61,
    "FL2": 62,
    "CDF": 66,
    # Netherlands
    "DED": 88,
    # Portugal
    "PPL": 94,
    # Belgium
    "JPL": 144,
    # Turkey
    "TSL": 203,
    # Greece
    "GSL": 197,
    # Austria
    "ABL": 218,
    # Switzerland
    "SSL": 207,
    # Denmark
    "DSL": 119,
    # Sweden
    "ASV": 113,
    "SUP": 114,
    # Norway
    "NES": 103,
    # Poland
    "EPK": 106,
    # Russia
    "RPL": 235,
    # Ireland
    "LOI": 357,
    # Finland
    "VLG": 244,
    # UEFA
    "CL": 2,
    "EL": 3,
    "ECL": 848,
    # South America
    "BSA": 71,
    "BSB": 72,
    "ALP": 128,
    "CHI": 265,
    "COP": 13,
    "CSU": 11,
    # North America
    "MLS": 253,
    "LMX": 262,
    # Asia
    "JL1": 98,
    "KL1": 292,
    "CSL": 169,
    "SPL2": 307,
    # Oceania
    "ALG": 188,
    # International
    "WC": 1,
    "EC": 4,
    "CAM": 9,
    "NL": 5,
    "AFN": 6,
}

# Football-Data.org supported codes (free tier limited)
# Only these can fetch fixtures/results from football-data.org
FOOTBALL_DATA_LEAGUES = {"PL", "PD", "BL1", "SA", "FL1", "CL", "ELC", "DED", "PPL", "BSA", "EC", "WC"}

# ================================================================
# MERGE discovered Pinnacle leagues (from discover_pinnacle_sports.py)
# ================================================================
# Additive-only: entry với code đã tồn tại (ví dụ "PL") giữ nguyên VN title.
# Chỉ thêm code mới từ discovery (thường dạng SPAIN_LA_LIGA_2 vv). Nhờ vậy
# LEAGUES_SHORT cho picker vẫn trỏ tới code gốc khi có, và pipeline vẫn
# nhận diện được giải chưa từng thấy trước đây.
try:
    from src.config_full_leagues import LEAGUES_FULL, ODDS_SPORTS_FULL
except Exception:
    LEAGUES_FULL, ODDS_SPORTS_FULL = {}, {}

for _code, _title in LEAGUES_FULL.items():
    if _code not in LEAGUES:
        LEAGUES[_code] = _title
for _code, _sk in ODDS_SPORTS_FULL.items():
    if _code not in ODDS_SPORTS:
        ODDS_SPORTS[_code] = _sk
# Low-confidence: giải chưa có trong FOOTBALL_DATA_LEAGUES *và* không nằm
# trong original LEAGUES → data historical yếu, dùng implied probability.
# Set này được pipeline dùng để set low_confidence_league flag.
LOW_CONFIDENCE_LEAGUES = {
    code for code in ODDS_SPORTS
    if code not in FOOTBALL_DATA_LEAGUES and code in LEAGUES_FULL
}

# Confidence thresholds
CONFIDENCE = {
    "HIGH": {"min_ev": 0.08, "min_agreement": 0.80},
    "MEDIUM": {"min_ev": 0.04, "min_agreement": 0.65},
    "LOW": {"min_ev": 0.01, "min_agreement": 0.50},
}

# Model selection. True → DixonColesModel (τ correction for low scores,
# time-decay weights, optional xG input). False → plain PoissonModel.
# Both implement the same predict() shape so the pipeline is model-agnostic.
USE_DIXON_COLES = True

# Bivariate Poisson model — enabled ONLY for top-5 European leagues with
# ≥100 matches in 90d (see _select_model in pipeline.py). Captures draw
# correlation via shared latent component Y3 → better 1-1/2-2 cells and
# +2-4% draw probability vs DC. Safe default False: shipping it on requires
# validation on live league data first.
USE_BIVARIATE_POISSON = True

# Leagues eligible for BivariatePoisson when USE_BIVARIATE_POISSON is on.
# Restricted to top-5 European → they consistently have enough historical
# matches AND xG coverage to make the extra λ3 parameter identifiable.
BIVARIATE_POISSON_LEAGUES = {"PL", "PD", "BL1", "SA", "FL1"}

# API-Football quota floor — below this, we skip xG fetches to protect
# live-match calls (live uses same quota). MEGA plan = 150K/day.
API_FOOTBALL_QUOTA_FLOOR = 20000

# Special-match context (derby, cup final, knockout, 6-pointer).
# Three modes:
#   "off"      → ignore entirely
#   "log_only" → compute context, log it, save to Prediction.match_context,
#                but DO NOT adjust model λ
#   "on"       → compute + log + save + apply λ adjustments (see
#                src/analytics/match_context.LAMBDA_ADJUSTMENTS)
# Default "log_only" ships the classifier cold, lets us verify on live data
# that DERBY_PAIRS isn't over/under-tagging before we let it steer probabilities.
USE_MATCH_CONTEXT = "log_only"

# Phase B2 — canonical team mapping gate.
#   "off":      fetch xG by team name only (legacy path, no mapping lookup).
#   "log_only": also fetch by api_id, log side-by-side comparison, but the
#               model consumes the name-based result. Default — lets us see
#               how often id-match vs name-match disagree before trusting it.
#   "on":       prefer api_id lookup; fall back to name when api_id is NULL
#               or the id-keyed bucket misses.
# Populated by migrate_team_mapping.py + artifacts/team_mapping.json (Phase B1).
USE_TEAM_MAPPING = "on"

# Phase 2.1 — pre-match fixture_id resolver for /chot lineup + injuries.
#   "off":      resolver never called, /chot only uses LiveMatchState (legacy).
#   "log_only": resolver called, hit rate + quota logged, but fixture_id NOT
#               assigned to the signals collector. Safe observation mode.
#   "on":       resolver called, fixture_id assigned → lineup + injuries actually
#               fire pre-match.
# Flip to "on" only after measuring hit rate + quota delta from log_only logs.
USE_PREMATCH_FIXTURE_RESOLVER = "log_only"

# Standalone Steam Move Telegram alerts (scheduled_steam_check).
#   True  = push one alert per detected steam (legacy behavior).
#   False = detection still runs, DB still persists, /chot card (Phase 3) still
#           surfaces the steam signal inline — only the standalone notification
#           is suppressed. Use when inline display is enough and per-steam pings
#           are noisy.
USE_STEAM_MOVE_ALERTS = False

# Tắt push "VALUE BET DETECTED" qua Telegram. Pipeline vẫn tạo Prediction
# để bot học + cho /ancan /phantich /chot dùng. False = chỉ log, không push.
USE_VALUE_BET_ALERTS = False

# ================================================================
# LIVE / IN-PLAY CONFIG
# ================================================================

# xG availability trong API-Football /fixtures/statistics.
# - True  → dùng expected_goals trực tiếp
# - False → dùng xG proxy (shots_on_target × 0.25 + shots_off_target × 0.05)
# Runtime vẫn sẽ tự fallback sang proxy nếu field expected_goals rỗng, bất kể
# flag này. Đặt False mặc định cho plan free (chưa verify được live).
# Scripts/probe_live_xg.py có thể chạy để probe thủ công khi có API key.
LIVE_XG_AVAILABLE = False

# Quota protection cho live pipeline
LIVE_MAX_MATCHES_PER_CYCLE = 50
LIVE_QUOTA_MIN_THRESHOLD = 5000  # Pause live khi Odds API remaining < threshold

# Live tracking: broadened to ALL Pinnacle-covered leagues that also have an
# API-Football league_id. Trước đây giới hạn Big 5 + UEFA để tiết kiệm quota;
# giờ dùng LIVE_MAX_MATCHES_PER_CYCLE (50) làm backstop quota thay vì cứng danh
# sách giải. Các giải không có API-Football id sẽ bị bỏ qua tự nhiên.
LIVE_LEAGUE_CODES = [c for c in ODDS_SPORTS if c in API_FOOTBALL_LEAGUES]
LIVE_LEAGUE_IDS = {API_FOOTBALL_LEAGUES[c] for c in LIVE_LEAGUE_CODES}
