import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
FOOTBALL_DATA_API_KEY = os.getenv("FOOTBALL_DATA_API_KEY")
ODDS_API_KEY = os.getenv("ODDS_API_KEY")
API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "")
BOT_PASSWORD = os.getenv("BOT_PASSWORD", "")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///football_analytics.db")

# ================================================================
# SUPPORTED LEAGUES — organized by region
# ================================================================

# All leagues: code -> display name
LEAGUES = {
    # --- ENGLAND ---
    "PL": "Premier League",
    "ELC": "Championship",
    "EL1": "League One",
    "EL2": "League Two",
    "FAC": "FA Cup",
    # --- SPAIN ---
    "PD": "La Liga",
    "SD": "La Liga 2",
    # --- GERMANY ---
    "BL1": "Bundesliga",
    "BL2": "Bundesliga 2",
    "DFB": "DFB-Pokal",
    # --- ITALY ---
    "SA": "Serie A",
    "SB": "Serie B",
    # --- FRANCE ---
    "FL1": "Ligue 1",
    "FL2": "Ligue 2",
    # --- NETHERLANDS ---
    "DED": "Eredivisie",
    # --- PORTUGAL ---
    "PPL": "Primeira Liga",
    # --- BELGIUM ---
    "JPL": "Jupiler Pro League",
    # --- TURKEY ---
    "TSL": "Süper Lig",
    # --- GREECE ---
    "GSL": "Super League Greece",
    # --- SCOTLAND ---
    "SPL": "Scottish Premiership",
    # --- AUSTRIA ---
    "ABL": "Austrian Bundesliga",
    # --- SWITZERLAND ---
    "SSL": "Super League Switzerland",
    # --- DENMARK ---
    "DSL": "Superliga Denmark",
    # --- SWEDEN ---
    "ASV": "Allsvenskan",
    # --- NORWAY ---
    "NES": "Eliteserien",
    # --- POLAND ---
    "EPK": "Ekstraklasa",
    # --- CZECH ---
    "CFL": "Czech First League",
    # --- ROMANIA ---
    "RSL": "Liga 1 Romania",
    # --- UKRAINE ---
    "UPL": "Ukrainian Premier League",
    # --- RUSSIA ---
    "RPL": "Russian Premier League",
    # --- FINLAND ---
    "VLG": "Veikkausliiga",
    # --- UEFA ---
    "CL": "Champions League",
    "EL": "Europa League",
    "ECL": "Conference League",
    # --- SOUTH AMERICA ---
    "BSA": "Brasileirão Série A",
    "BSB": "Brasileirão Série B",
    "ALP": "Argentina Liga Profesional",
    "COP": "Copa Libertadores",
    "CSU": "Copa Sudamericana",
    # --- NORTH AMERICA ---
    "MLS": "MLS",
    "LMX": "Liga MX",
    # --- ASIA ---
    "JL1": "J1 League",
    "KL1": "K League 1",
    "CSL": "Chinese Super League",
    "SPL2": "Saudi Pro League",
    "ISL": "Indian Super League",
    # --- OCEANIA ---
    "ALG": "A-League",
    # --- INTERNATIONAL ---
    "WC": "FIFA World Cup",
    "EC": "UEFA Euro",
    "CAM": "Copa América",
    "NL": "UEFA Nations League",
    "AFN": "Africa Cup of Nations",
    "ACL": "AFC Champions League",
}

# League regions for /leagues display
LEAGUE_REGIONS = {
    "🏴󠁧󠁢󠁥󠁮󠁧󠁿 ANH": ["PL", "ELC", "EL1", "EL2", "FAC"],
    "🇪🇸 TÂY BAN NHA": ["PD", "SD"],
    "🇩🇪 ĐỨC": ["BL1", "BL2", "DFB"],
    "🇮🇹 Ý": ["SA", "SB"],
    "🇫🇷 PHÁP": ["FL1", "FL2"],
    "🇳🇱 HÀ LAN": ["DED"],
    "🇵🇹 BỒ ĐÀO NHA": ["PPL"],
    "🇧🇪 BỈ": ["JPL"],
    "🇹🇷 THỔ NHĨ KỲ": ["TSL"],
    "🇬🇷 HY LẠP": ["GSL"],
    "🏴󠁧󠁢󠁳󠁣󠁴󠁿 SCOTLAND": ["SPL"],
    "🇦🇹 ÁO": ["ABL"],
    "🇨🇭 THỤY SĨ": ["SSL"],
    "🇩🇰 ĐAN MẠCH": ["DSL"],
    "🇸🇪 THỤY ĐIỂN": ["ASV"],
    "🇳🇴 NA UY": ["NES"],
    "🇵🇱 BA LAN": ["EPK"],
    "🇨🇿 SÉC": ["CFL"],
    "🇷🇴 ROMANIA": ["RSL"],
    "🇺🇦 UKRAINE": ["UPL"],
    "🇷🇺 NGA": ["RPL"],
    "🇫🇮 PHẦN LAN": ["VLG"],
    "🏆 UEFA": ["CL", "EL", "ECL"],
    "🌎 NAM MỸ": ["BSA", "BSB", "ALP", "COP", "CSU"],
    "🌎 BẮC MỸ": ["MLS", "LMX"],
    "🌏 CHÂU Á": ["JL1", "KL1", "CSL", "SPL2", "ISL"],
    "🌏 CHÂU ĐẠI DƯƠNG": ["ALG"],
    "🌍 QUỐC TẾ": ["WC", "EC", "CAM", "NL", "AFN", "ACL"],
}

# The Odds API sport keys
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
    # Germany
    "BL1": "soccer_germany_bundesliga",
    "BL2": "soccer_germany_bundesliga2",
    "DFB": "soccer_germany_dfb_pokal",
    # Italy
    "SA": "soccer_italy_serie_a",
    "SB": "soccer_italy_serie_b",
    # France
    "FL1": "soccer_france_ligue_one",
    "FL2": "soccer_france_ligue_two",
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
    # Scotland
    "SPL": "soccer_spl",
    # Austria
    "ABL": "soccer_austria_bundesliga",
    # Switzerland
    "SSL": "soccer_switzerland_superleague",
    # Denmark
    "DSL": "soccer_denmark_superliga",
    # Sweden
    "ASV": "soccer_sweden_allsvenskan",
    # Norway
    "NES": "soccer_norway_eliteserien",
    # Poland
    "EPK": "soccer_poland_ekstraklasa",
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
    "COP": "soccer_conmebol_copa_libertadores",
    "CSU": "soccer_conmebol_copa_sudamericana",
    # North America
    "MLS": "soccer_usa_mls",
    "LMX": "soccer_mexico_ligamx",
    # Asia
    "JL1": "soccer_japan_j_league",
    "KL1": "soccer_korea_kleague1",
    "CSL": "soccer_china_superleague",
    "SPL2": "soccer_saudi_professional_league",
    # Oceania
    "ALG": "soccer_australia_aleague",
    # International
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
    # Germany
    "BL1": 78,
    "BL2": 79,
    "DFB": 81,
    # Italy
    "SA": 135,
    "SB": 136,
    # France
    "FL1": 61,
    "FL2": 62,
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
    # Scotland
    "SPL": 179,
    # Austria
    "ABL": 218,
    # Switzerland
    "SSL": 207,
    # Denmark
    "DSL": 120,
    # Sweden
    "ASV": 113,
    # Norway
    "NES": 103,
    # Poland
    "EPK": 106,
    # Czech
    "CFL": 345,
    # Romania
    "RSL": 283,
    # Ukraine
    "UPL": 333,
    # Russia
    "RPL": 235,
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
    "ISL": 323,
    # Oceania
    "ALG": 188,
    # International
    "WC": 1,
    "EC": 4,
    "CAM": 9,
    "NL": 5,
    "AFN": 6,
    "ACL": 17,
}

# Football-Data.org supported codes (free tier limited)
# Only these can fetch fixtures/results from football-data.org
FOOTBALL_DATA_LEAGUES = {"PL", "PD", "BL1", "SA", "FL1", "CL", "ELC", "DED", "PPL", "BSA", "EC", "WC"}

# Confidence thresholds
CONFIDENCE = {
    "HIGH": {"min_ev": 0.08, "min_agreement": 0.80},
    "MEDIUM": {"min_ev": 0.04, "min_agreement": 0.65},
    "LOW": {"min_ev": 0.01, "min_agreement": 0.50},
}
