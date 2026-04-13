#!/usr/bin/env python3
"""
Port Operations Analytics Warehouse — Synthetic Data Generator (v2)
====================================================================
Generates realistic synthetic data for three ports:
  • Port of Baltimore   (USBAL)  ~750k TEU / year  → heavy RoRo + bulk
  • Port of Valencia    (ESVLC)  ~5.7M TEU / year  → dominant containers
  • Port of Naples      (ITNAP)  ~650k TEU / year  → mixed cargo

KEY IMPROVEMENTS OVER v1
-------------------------
1. Realistic monthly seasonality per port:
     - Valencia    peaks June–August (summer container surge)
     - Baltimore   peaks October–December (retail import season)
     - Naples      peaks April–September (Mediterranean trade season)
2. Carrier concentration by route:
     - MSC + Maersk dominate Asia–Europe lanes (routes 4, 5)
     - Hapag-Lloyd strong on Trans-Atlantic (routes 2, 3)
     - Regional carriers fill intra-Med and short-sea trades
3. Correlated delay patterns:
     - Baltimore delay rate rises in winter months (Nov–Feb: +40%)
     - Naples delay rate rises in summer (Jun–Aug: +35% congestion)
     - Berth-waiting hours scale proportionally with delay rate
4. Cargo type mix calibrated per port (see PORT_CONFIG)
5. Year-on-year growth of 3–5% TEU from 2022 → 2024
6. Public holiday and season columns added to dim_date:
     - US federal holidays for Baltimore
     - Spanish national holidays for Valencia
     - Italian national holidays for Naples
     - Season names (Northern hemisphere: Winter/Spring/Summer/Autumn)

All CSV output filenames and column names are preserved from v1.

Run: python scripts/generate_data.py
Output: seeds/*.csv
"""

import os
import random
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta

# ── Reproducibility ────────────────────────────────────────────
random.seed(42)
np.random.seed(42)

# ── I/O ───────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, '..', 'seeds')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Date window ───────────────────────────────────────────────
START_DATE = date(2022, 1, 1)
END_DATE   = date(2024, 12, 31)
YEARS      = [2022, 2023, 2024]

# ── Year-on-year TEU growth multipliers (compound: ~3–5% per year) ──
YOY_GROWTH = {2022: 1.000, 2023: 1.038, 2024: 1.078}

# ── Monthly delay multipliers: index 0 = January ──────────────
# Baltimore: winter delays (Nov-Feb)
_BAL_DELAY_MULT = [1.35, 1.40, 1.15, 0.90, 0.85, 0.80,
                   0.80, 0.85, 0.95, 1.00, 1.25, 1.30]
# Naples: summer congestion (Jun-Aug)
_NAP_DELAY_MULT = [0.80, 0.80, 0.85, 0.90, 1.00, 1.35,
                   1.40, 1.35, 1.10, 0.90, 0.85, 0.80]
# Valencia: relatively stable but mild peaks in spring
_VLC_DELAY_MULT = [0.90, 0.90, 0.95, 1.00, 1.05, 1.10,
                   1.10, 1.05, 1.00, 0.95, 0.90, 0.90]

# ── Carrier-to-route affinity weights ─────────────────────────
# carrier_key: {route_key: weight_multiplier}
# Base weights are defined per port; this map boosts specific pairings.
CARRIER_ROUTE_AFFINITY = {
    1:  {4: 3.5, 5: 3.5, 9: 2.0, 6: 2.0},          # MSC → Asia-Med, intra-Med
    2:  {4: 3.0, 5: 3.0, 2: 2.5, 3: 2.5, 1: 2.0},  # Maersk → Asia-Med, Trans-Atl
    3:  {4: 2.5, 5: 2.5, 6: 2.0, 9: 1.5},           # CMA CGM → Asia-Med
    4:  {4: 2.0, 5: 2.0, 12: 2.5},                   # Evergreen → Asia routes
    5:  {4: 2.0, 5: 2.0, 1: 1.5, 12: 1.5},           # COSCO → Asia routes
    6:  {2: 3.0, 3: 3.0, 10: 2.5, 1: 2.0},           # Hapag-Lloyd → Trans-Atl
    7:  {4: 2.0, 5: 2.0, 12: 2.0},                   # ONE → Asia routes
    8:  {12: 2.5, 1: 2.0, 4: 1.5},                   # ZIM → Trans-Pacific, Asia
    9:  {6: 2.5, 9: 2.5, 11: 2.0},                   # MSC Cruises → Intra-Med
    10: {6: 3.0, 9: 2.5, 14: 2.0, 13: 1.5},          # Grimaldi → Intra-Med
    11: {15: 3.0, 6: 2.5, 9: 2.0},                   # Short-sea specialist
    12: {14: 2.5, 13: 2.0, 6: 1.5},                  # South America routes
    13: {6: 2.0, 9: 2.0, 11: 1.5},                   # Intra-Med specialist
    14: {2: 2.5, 3: 2.5, 10: 2.0},                   # Trans-Atlantic specialist
    15: {15: 3.5, 6: 2.0},                            # Coastal/domestic
}

# =============================================================
# PORT CONFIGURATION
# Annual figures are for 2022; apply YOY_GROWTH for later years.
# =============================================================
PORT_CONFIG = {
    1: {   # Baltimore — heavy on RoRo and bulk, retail import peak Oct-Dec
        'code': 'USBAL', 'name': 'Port of Baltimore',
        'annual_calls':        650,
        'annual_teu':      750_000,
        'annual_tonnes': 9_500_000,
        'terminals':       [1, 2, 3],
        'terminal_weights':[0.55, 0.30, 0.15],
        # cargo_type_keys: 1=FCL, 2=LCL, 3=REF, 4=DRY, 5=LIQ, 6=ROR, 7=BBK, 8=HAZ
        # Baltimore: strong RoRo (6) and dry bulk (4); moderate FCL (1)
        'cargo_type_w':    [0.30, 0.04, 0.06, 0.18, 0.04, 0.28, 0.08, 0.02],
        # direction_keys: 1=IMP, 2=EXP, 3=TSH, 4=CST
        'direction_w':     [0.40, 0.38, 0.12, 0.10],
        # Carriers: MSC(1), Maersk(2), CMA CGM(3), Hapag(6), ZIM(8), Yang Ming(11), HMM(14)
        'carrier_keys':    [1, 2, 3, 6, 8, 11, 14],
        'carrier_w':       [0.18, 0.16, 0.12, 0.20, 0.12, 0.12, 0.10],  # Hapag dominant
        'route_keys':      [2, 3, 1, 10, 15],
        'route_w':         [0.30, 0.25, 0.20, 0.15, 0.10],
        # Seasonality: peaks Oct–Dec for retail imports (indices 0-based)
        'seasonality':     [0.060, 0.060, 0.075, 0.080, 0.085, 0.080,
                            0.075, 0.080, 0.090, 0.105, 0.110, 0.100],
        'avg_waiting_hrs': 8.5,
        'avg_berth_hrs':   36.0,
        'delay_rate':      0.22,      # base; multiplied by _BAL_DELAY_MULT per month
        'delay_mult':      _BAL_DELAY_MULT,
        'country_keys':    [1, 2, 3, 4, 23, 21, 22, 5, 6, 7],
        'holiday_country': 'US',
        'hemisphere':      'N',
    },
    2: {   # Valencia — dominant containers; June-Aug summer peak
        'code': 'ESVLC', 'name': 'Port of Valencia',
        'annual_calls':       1050,
        'annual_teu':      5_700_000,
        'annual_tonnes':  16_000_000,
        'terminals':       [4, 5, 6],
        'terminal_weights':[0.50, 0.35, 0.15],
        # Valencia: overwhelmingly FCL containers; notable reefer and transship
        'cargo_type_w':    [0.58, 0.07, 0.10, 0.04, 0.02, 0.12, 0.05, 0.02],
        'direction_w':     [0.35, 0.33, 0.25, 0.07],
        # MSC(1) dominant (MSC Terminal), Maersk(2), CMA CGM(3), Evergreen(4), COSCO(5)
        'carrier_keys':    [1, 2, 3, 4, 5, 6, 7, 9, 10, 13],
        'carrier_w':       [0.22, 0.15, 0.13, 0.10, 0.10, 0.08, 0.08, 0.06, 0.05, 0.03],
        'route_keys':      [4, 5, 6, 9, 11, 13, 14],
        'route_w':         [0.22, 0.18, 0.20, 0.15, 0.10, 0.08, 0.07],
        # Seasonality: peaks June-August (summer Med trade surge)
        'seasonality':     [0.065, 0.065, 0.075, 0.085, 0.090, 0.100,
                            0.105, 0.105, 0.095, 0.085, 0.070, 0.060],
        'avg_waiting_hrs': 6.0,
        'avg_berth_hrs':   28.0,
        'delay_rate':      0.15,
        'delay_mult':      _VLC_DELAY_MULT,
        'country_keys':    [13, 12, 2, 3, 9, 14, 15, 16, 4, 11],
        'holiday_country': 'ES',
        'hemisphere':      'N',
    },
    3: {   # Naples — mixed; peaks April-September for Mediterranean trade
        'code': 'ITNAP', 'name': 'Port of Naples',
        'annual_calls':        550,
        'annual_teu':       650_000,
        'annual_tonnes':  5_500_000,
        'terminals':       [7, 8, 9],
        'terminal_weights':[0.50, 0.30, 0.20],
        # Naples: strong RoRo (Grimaldi home port), mixed FCL, break-bulk
        'cargo_type_w':    [0.38, 0.05, 0.07, 0.10, 0.04, 0.24, 0.10, 0.02],
        'direction_w':     [0.42, 0.38, 0.12, 0.08],
        # MSC(1), CMA CGM(3), Hapag(6), ONE(7), Grimaldi/10, ZIM(8), PIL(15)
        'carrier_keys':    [1, 3, 6, 7, 10, 13, 14],
        'carrier_w':       [0.20, 0.16, 0.12, 0.12, 0.18, 0.12, 0.10],  # Grimaldi/10 strong
        'route_keys':      [6, 9, 4, 5, 11, 13, 15],
        'route_w':         [0.28, 0.20, 0.15, 0.12, 0.10, 0.08, 0.07],
        # Seasonality: peaks April-September (Mediterranean trade + tourism cargo)
        'seasonality':     [0.055, 0.055, 0.075, 0.095, 0.100, 0.105,
                            0.105, 0.100, 0.095, 0.080, 0.065, 0.070],
        'avg_waiting_hrs': 10.0,
        'avg_berth_hrs':   30.0,
        'delay_rate':      0.28,      # base; multiplied by _NAP_DELAY_MULT per month
        'delay_mult':      _NAP_DELAY_MULT,
        'country_keys':    [12, 13, 2, 14, 11, 3, 15, 16, 4, 18],
        'holiday_country': 'IT',
        'hemisphere':      'N',
    },
}


# =============================================================
# HOLIDAY DEFINITIONS
# Fixed-date national public holidays per country.
# Easter-based holidays are approximated to fixed popular dates.
# =============================================================

def _us_holidays(year: int) -> dict:
    """US federal public holidays relevant to Baltimore trade."""
    holidays = {
        date(year, 1, 1):   "New Year's Day",
        date(year, 7, 4):   "Independence Day",
        date(year, 11, 11): "Veterans Day",
        date(year, 12, 25): "Christmas Day",
    }
    # MLK Day: 3rd Monday in January
    d = date(year, 1, 1)
    mondays = [d + timedelta(days=i) for i in range(31) if (d + timedelta(i)).weekday() == 0]
    if len(mondays) >= 3:
        holidays[mondays[2]] = "Martin Luther King Jr. Day"
    # Presidents Day: 3rd Monday in February
    d = date(year, 2, 1)
    mondays = [d + timedelta(days=i) for i in range(28) if (d + timedelta(i)).weekday() == 0]
    if len(mondays) >= 3:
        holidays[mondays[2]] = "Presidents' Day"
    # Memorial Day: last Monday in May
    d = date(year, 5, 31)
    while d.weekday() != 0:
        d -= timedelta(days=1)
    holidays[d] = "Memorial Day"
    # Labor Day: 1st Monday in September
    d = date(year, 9, 1)
    while d.weekday() != 0:
        d += timedelta(days=1)
    holidays[d] = "Labor Day"
    # Thanksgiving: 4th Thursday in November
    d = date(year, 11, 1)
    thursdays = [d + timedelta(days=i) for i in range(30) if (d + timedelta(i)).weekday() == 3]
    if len(thursdays) >= 4:
        holidays[thursdays[3]] = "Thanksgiving Day"
    return holidays


def _es_holidays(year: int) -> dict:
    """Spanish national public holidays relevant to Valencia trade."""
    return {
        date(year, 1, 1):   "Año Nuevo",
        date(year, 1, 6):   "Epifanía del Señor",
        date(year, 3, 19):  "San José (aprox.)",        # Valencian Community
        date(year, 5, 1):   "Fiesta del Trabajo",
        date(year, 8, 15):  "Asunción de la Virgen",
        date(year, 10, 12): "Fiesta Nacional de España",
        date(year, 11, 1):  "Todos los Santos",
        date(year, 12, 6):  "Día de la Constitución",
        date(year, 12, 8):  "Inmaculada Concepción",
        date(year, 12, 25): "Navidad",
    }


def _it_holidays(year: int) -> dict:
    """Italian national public holidays relevant to Naples trade."""
    return {
        date(year, 1, 1):   "Capodanno",
        date(year, 1, 6):   "Epifania",
        date(year, 4, 25):  "Festa della Liberazione",
        date(year, 5, 1):   "Festa dei Lavoratori",
        date(year, 6, 2):   "Festa della Repubblica",
        date(year, 8, 15):  "Ferragosto",
        date(year, 11, 1):  "Ognissanti",
        date(year, 12, 8):  "Immacolata Concezione",
        date(year, 12, 25): "Natale",
        date(year, 12, 26): "Santo Stefano",
        date(year, 9, 19):  "San Gennaro (Naples patron)",  # Naples specific
    }


# Season names per calendar month (Northern hemisphere)
_NORTH_SEASONS = {
    1: 'Winter', 2: 'Winter', 3: 'Spring',
    4: 'Spring', 5: 'Spring', 6: 'Summer',
    7: 'Summer', 8: 'Summer', 9: 'Autumn',
    10: 'Autumn', 11: 'Autumn', 12: 'Winter',
}


def _build_holiday_lookup() -> dict:
    """
    Build a lookup: {date: (is_holiday, holiday_name)}.
    Uses a union of all three countries' holidays — port-specific
    flags are applied in gen_dim_date via per-country mapping.
    """
    lookup = {}
    for year in YEARS:
        for country, fn in [('US', _us_holidays), ('ES', _es_holidays), ('IT', _it_holidays)]:
            for d, name in fn(year).items():
                if d not in lookup:
                    lookup[d] = {}
                lookup[d][country] = name
    return lookup


# =============================================================
# 1. DIM_DATE  (enhanced with holiday + season columns)
# =============================================================
def gen_dim_date() -> pd.DataFrame:
    """
    Date dimension covering 2022-01-01 to 2024-12-31.
    New columns vs v1:
      is_public_holiday  SMALLINT  1 = holiday in at least one study port
      holiday_name       VARCHAR   Pipe-separated list of holiday names
      season_name        VARCHAR   Winter / Spring / Summer / Autumn (N. hemisphere)
    """
    dates      = pd.date_range(START_DATE, END_DATE, freq='D')
    hol_lookup = _build_holiday_lookup()

    is_holiday_list  = []
    holiday_name_list = []
    season_name_list  = []

    for d in dates.date:
        hols = hol_lookup.get(d, {})
        if hols:
            is_holiday_list.append(1)
            # Collect unique holiday names across countries
            unique_names = list(dict.fromkeys(hols.values()))
            holiday_name_list.append(' | '.join(unique_names))
        else:
            is_holiday_list.append(0)
            holiday_name_list.append('')

        season_name_list.append(_NORTH_SEASONS[d.month])

    df = pd.DataFrame({
        'date_key':         dates.strftime('%Y%m%d').astype(int),
        'full_date':        dates.date,
        'year':             dates.year.astype('int16'),
        'quarter':          dates.quarter.astype('int16'),
        'month':            dates.month.astype('int16'),
        'month_name':       dates.strftime('%B'),
        'week_of_year':     dates.isocalendar().week.astype(int).astype('int16'),
        'day_of_month':     dates.day.astype('int16'),
        'day_of_week':      (dates.dayofweek + 1).astype('int16'),   # 1=Mon
        'day_name':         dates.strftime('%A'),
        'is_weekend':       (dates.dayofweek >= 5).astype('int16'),
        'fiscal_year':      np.where(dates.month >= 10,
                                     dates.year + 1, dates.year).astype('int16'),
        'fiscal_quarter':   ((dates.month - 1) // 3 + 1).astype('int16'),
        'is_public_holiday': is_holiday_list,
        'holiday_name':     holiday_name_list,
        'season_name':      season_name_list,
    })
    return df


# =============================================================
# 2. DIM_PORT  (unchanged — preserve existing seed data structure)
# =============================================================
def gen_dim_port() -> pd.DataFrame:
    return pd.DataFrame([
        {
            'port_key': 1, 'port_code': 'USBAL',
            'port_name': 'Port of Baltimore',
            'country_code': 'US', 'country_name': 'United States',
            'region': 'North America', 'continent': 'Americas',
            'un_locode': 'US BAL', 'latitude': 39.271300,
            'longitude': -76.578600, 'port_type': 'Multi-purpose',
            'annual_capacity_teu': 1_200_000,
        },
        {
            'port_key': 2, 'port_code': 'ESVLC',
            'port_name': 'Port of Valencia',
            'country_code': 'ES', 'country_name': 'Spain',
            'region': 'Southern Europe', 'continent': 'Europe',
            'un_locode': 'ES VLC', 'latitude': 39.442900,
            'longitude': -0.324500, 'port_type': 'Container Hub',
            'annual_capacity_teu': 8_000_000,
        },
        {
            'port_key': 3, 'port_code': 'ITNAP',
            'port_name': 'Port of Naples',
            'country_code': 'IT', 'country_name': 'Italy',
            'region': 'Southern Europe', 'continent': 'Europe',
            'un_locode': 'IT NAP', 'latitude': 40.838800,
            'longitude': 14.264900, 'port_type': 'Multi-purpose',
            'annual_capacity_teu': 1_000_000,
        },
    ])


# =============================================================
# 3. DIM_TERMINAL  (unchanged)
# =============================================================
def gen_dim_terminal() -> pd.DataFrame:
    return pd.DataFrame([
        # ── Baltimore ────────────────────────────────────────
        {'terminal_key': 1, 'port_key': 1, 'terminal_code': 'BAL-SGT',
         'terminal_name': 'Seagirt Marine Terminal',
         'terminal_type': 'Container', 'berths': 4,
         'max_draft_m': 15.2, 'crane_count': 5, 'annual_capacity_teu': 800_000},
        {'terminal_key': 2, 'port_key': 1, 'terminal_code': 'BAL-SLP',
         'terminal_name': 'South Locust Point Terminal',
         'terminal_type': 'RoRo', 'berths': 3,
         'max_draft_m': 12.0, 'crane_count': 0, 'annual_capacity_teu': 0},
        {'terminal_key': 3, 'port_key': 1, 'terminal_code': 'BAL-DMT',
         'terminal_name': 'Dundalk Marine Terminal',
         'terminal_type': 'Multi-purpose', 'berths': 6,
         'max_draft_m': 10.5, 'crane_count': 2, 'annual_capacity_teu': 400_000},
        # ── Valencia ─────────────────────────────────────────
        {'terminal_key': 4, 'port_key': 2, 'terminal_code': 'VLC-MSC',
         'terminal_name': 'MSC Terminal Valencia',
         'terminal_type': 'Container', 'berths': 6,
         'max_draft_m': 16.0, 'crane_count': 12, 'annual_capacity_teu': 4_500_000},
        {'terminal_key': 5, 'port_key': 2, 'terminal_code': 'VLC-NCT',
         'terminal_name': 'Noatum Container Terminal',
         'terminal_type': 'Container', 'berths': 5,
         'max_draft_m': 14.5, 'crane_count': 10, 'annual_capacity_teu': 2_800_000},
        {'terminal_key': 6, 'port_key': 2, 'terminal_code': 'VLC-GRM',
         'terminal_name': 'Grimaldi Terminal Valencia',
         'terminal_type': 'RoRo', 'berths': 3,
         'max_draft_m': 11.0, 'crane_count': 0, 'annual_capacity_teu': 0},
        # ── Naples ───────────────────────────────────────────
        {'terminal_key': 7, 'port_key': 3, 'terminal_code': 'NAP-CNT',
         'terminal_name': 'Conateco Container Terminal',
         'terminal_type': 'Container', 'berths': 3,
         'max_draft_m': 13.5, 'crane_count': 4, 'annual_capacity_teu': 600_000},
        {'terminal_key': 8, 'port_key': 3, 'terminal_code': 'NAP-FGT',
         'terminal_name': 'Flavio Gioia Terminal',
         'terminal_type': 'RoRo', 'berths': 4,
         'max_draft_m': 10.0, 'crane_count': 0, 'annual_capacity_teu': 0},
        {'terminal_key': 9, 'port_key': 3, 'terminal_code': 'NAP-MAT',
         'terminal_name': 'Molo Angioino Terminal',
         'terminal_type': 'Multi-purpose', 'berths': 2,
         'max_draft_m': 9.5, 'crane_count': 1, 'annual_capacity_teu': 400_000},
    ])


# =============================================================
# 4. DIM_SHIP_SIZE_CLASS  (unchanged)
# =============================================================
def gen_dim_ship_size_class() -> pd.DataFrame:
    return pd.DataFrame([
        {'ship_size_class_key': 1, 'class_name': 'Feeder',
         'min_teu': 0,     'max_teu': 999,
         'min_dwt': 1000,  'max_dwt': 15000,
         'description': 'Small feeder vessels serving regional/short-sea routes'},
        {'ship_size_class_key': 2, 'class_name': 'Small Feeder',
         'min_teu': 1000,  'max_teu': 1999,
         'min_dwt': 15000, 'max_dwt': 25000,
         'description': 'Small to medium feeder vessels'},
        {'ship_size_class_key': 3, 'class_name': 'Regional',
         'min_teu': 2000,  'max_teu': 4999,
         'min_dwt': 25000, 'max_dwt': 60000,
         'description': 'Regional and inter-regional trade vessels'},
        {'ship_size_class_key': 4, 'class_name': 'Post-Panamax',
         'min_teu': 5000,  'max_teu': 9999,
         'min_dwt': 60000, 'max_dwt': 120000,
         'description': 'Large ocean-going post-Panamax vessels'},
        {'ship_size_class_key': 5, 'class_name': 'New Panamax',
         'min_teu': 10000, 'max_teu': 14499,
         'min_dwt': 120000,'max_dwt': 165000,
         'description': 'New Panamax / neo-Panamax class vessels'},
        {'ship_size_class_key': 6, 'class_name': 'Ultra Large',
         'min_teu': 14500, 'max_teu': 24000,
         'min_dwt': 165000,'max_dwt': 240000,
         'description': 'Ultra large container vessels (ULCV / VLCS)'},
    ])


# =============================================================
# 5. DIM_VESSEL  (unchanged structure)
# =============================================================
_PREFIXES = ['MSC', 'Maersk', 'CMA CGM', 'Evergreen', 'COSCO',
             'ONE',  'Hapag',  'Yang Ming', 'HMM', 'PIL', 'ZIM', 'MOL']
_NAMES    = ['Eagle', 'Falcon', 'Phoenix', 'Titan', 'Atlas', 'Orion', 'Polaris',
             'Horizon', 'Pacific', 'Atlantic', 'Meridian', 'Apex', 'Unity',
             'Pioneer', 'Voyager', 'Liberty', 'Victory', 'Fortune', 'Prosperity',
             'Excellence', 'Endeavour', 'Singapore', 'Rotterdam', 'Antwerp',
             'Hamburg', 'Valencia', 'Algeciras', 'Genova', 'Piraeus', 'Shanghai',
             'Star', 'Sun', 'Nova', 'Cosmos', 'Galaxy', 'Nebula', 'Triton', 'Zephyr',
             'Condor', 'Osprey', 'Swift', 'Ranger', 'Sentinel', 'Vanguard']
_FLAGS    = ['Panama', 'Liberia', 'Marshall Islands', 'Bahamas', 'Malta',
             'Cyprus', 'Singapore', 'Hong Kong', 'Greece', 'China']
_VTYPES   = ['Container Ship', 'Bulk Carrier', 'RoRo Vessel', 'General Cargo', 'Tanker']


def gen_dim_vessel(n: int = 200) -> pd.DataFrame:
    rows, used = [], set()
    _teu_ranges = {1: (300, 999), 2: (1000, 1999), 3: (2000, 4999),
                   4: (5000, 9999), 5: (10000, 14499), 6: (14500, 23000)}

    for i in range(1, n + 1):
        prefix = random.choice(_PREFIXES)
        name   = random.choice(_NAMES)
        full   = f'{prefix} {name}'
        if full in used:
            full = f'{full} {random.randint(1, 99)}'
        used.add(full)

        vtype = random.choices(_VTYPES, weights=[0.55, 0.15, 0.15, 0.10, 0.05])[0]
        built = random.randint(2000, 2023)

        if vtype == 'Container Ship':
            ssk = random.choices([1, 2, 3, 4, 5, 6],
                                  weights=[0.10, 0.15, 0.25, 0.25, 0.15, 0.10])[0]
            teu = random.randint(*_teu_ranges[ssk])
            dwt = int(teu * random.uniform(1.5, 2.2))
            loa = int(150 + (teu / 24000) * 250)
        elif vtype == 'Bulk Carrier':
            ssk = random.choices([1, 2, 3], weights=[0.30, 0.40, 0.30])[0]
            teu = 0
            dwt = random.randint(15000, 120000)
            loa = int(150 + dwt / 2000)
        elif vtype == 'RoRo Vessel':
            ssk = random.choices([1, 2, 3], weights=[0.40, 0.40, 0.20])[0]
            teu = 0
            dwt = random.randint(8000, 65000)
            loa = random.randint(150, 230)
        else:
            ssk = 1
            teu = 0
            dwt = random.randint(5000, 30000)
            loa = random.randint(100, 180)

        gt = int(dwt * random.uniform(0.55, 0.80))

        rows.append({
            'vessel_key':         i,
            'imo_number':         9000000 + i * 97,
            'vessel_name':        full,
            'vessel_type':        vtype,
            'flag_country':       random.choice(_FLAGS),
            'built_year':         built,
            'gross_tonnage':      gt,
            'deadweight_tonnes':  dwt,
            'teu_capacity':       teu,
            'loa_meters':         min(loa, 400),
            'ship_size_class_key': ssk,
        })
    return pd.DataFrame(rows)


# =============================================================
# 6. DIM_CARRIER  (15 carriers with realistic alliance info)
# =============================================================
def gen_dim_carrier() -> pd.DataFrame:
    return pd.DataFrame([
        {'carrier_key':  1, 'carrier_code': 'MSC',  'carrier_name': 'Mediterranean Shipping Company',
         'alliance': 'MSC/TIL',        'hq_country': 'Switzerland'},
        {'carrier_key':  2, 'carrier_code': 'MAE',  'carrier_name': 'Maersk Line',
         'alliance': 'Gemini',         'hq_country': 'Denmark'},
        {'carrier_key':  3, 'carrier_code': 'CMA',  'carrier_name': 'CMA CGM',
         'alliance': 'Ocean Alliance', 'hq_country': 'France'},
        {'carrier_key':  4, 'carrier_code': 'EVG',  'carrier_name': 'Evergreen Marine',
         'alliance': 'Ocean Alliance', 'hq_country': 'Taiwan'},
        {'carrier_key':  5, 'carrier_code': 'CSC',  'carrier_name': 'COSCO Shipping Lines',
         'alliance': 'Ocean Alliance', 'hq_country': 'China'},
        {'carrier_key':  6, 'carrier_code': 'HPL',  'carrier_name': 'Hapag-Lloyd',
         'alliance': 'Gemini',         'hq_country': 'Germany'},
        {'carrier_key':  7, 'carrier_code': 'ONE',  'carrier_name': 'Ocean Network Express',
         'alliance': 'Premier Alliance','hq_country': 'Japan'},
        {'carrier_key':  8, 'carrier_code': 'ZIM',  'carrier_name': 'ZIM Integrated Shipping',
         'alliance': 'Independent',    'hq_country': 'Israel'},
        {'carrier_key':  9, 'carrier_code': 'YML',  'carrier_name': 'Yang Ming Marine Transport',
         'alliance': 'Premier Alliance','hq_country': 'Taiwan'},
        {'carrier_key': 10, 'carrier_code': 'GRM',  'carrier_name': 'Grimaldi Lines',
         'alliance': 'Independent',    'hq_country': 'Italy'},
        {'carrier_key': 11, 'carrier_code': 'PIL',  'carrier_name': 'Pacific International Lines',
         'alliance': 'Independent',    'hq_country': 'Singapore'},
        {'carrier_key': 12, 'carrier_code': 'COE',  'carrier_name': 'COLI Ocean Express',
         'alliance': 'Independent',    'hq_country': 'China'},
        {'carrier_key': 13, 'carrier_code': 'HMM',  'carrier_name': 'HMM Co.',
         'alliance': 'Premier Alliance','hq_country': 'South Korea'},
        {'carrier_key': 14, 'carrier_code': 'ARK',  'carrier_name': 'Arkas Container Transport',
         'alliance': 'Independent',    'hq_country': 'Turkey'},
        {'carrier_key': 15, 'carrier_code': 'SHL',  'carrier_name': 'Sealand (Maersk feeder)',
         'alliance': 'Gemini',         'hq_country': 'Denmark'},
    ])


# =============================================================
# 7. DIM_CARGO_TYPE  (unchanged)
# =============================================================
def gen_dim_cargo_type() -> pd.DataFrame:
    return pd.DataFrame([
        {'cargo_type_key': 1, 'cargo_type_code': 'FCL', 'cargo_type_name': 'Full Container Load',
         'cargo_category': 'Container', 'unit': 'TEU',    'is_hazmat_capable': False},
        {'cargo_type_key': 2, 'cargo_type_code': 'LCL', 'cargo_type_name': 'Less-than-Container Load',
         'cargo_category': 'Container', 'unit': 'TEU',    'is_hazmat_capable': False},
        {'cargo_type_key': 3, 'cargo_type_code': 'REF', 'cargo_type_name': 'Refrigerated Cargo',
         'cargo_category': 'Container', 'unit': 'TEU',    'is_hazmat_capable': False},
        {'cargo_type_key': 4, 'cargo_type_code': 'DRY', 'cargo_type_name': 'Dry Bulk',
         'cargo_category': 'Bulk',      'unit': 'Tonnes', 'is_hazmat_capable': False},
        {'cargo_type_key': 5, 'cargo_type_code': 'LIQ', 'cargo_type_name': 'Liquid Bulk',
         'cargo_category': 'Bulk',      'unit': 'Tonnes', 'is_hazmat_capable': True},
        {'cargo_type_key': 6, 'cargo_type_code': 'ROR', 'cargo_type_name': 'Roll-on/Roll-off',
         'cargo_category': 'RoRo',      'unit': 'Units',  'is_hazmat_capable': False},
        {'cargo_type_key': 7, 'cargo_type_code': 'BBK', 'cargo_type_name': 'Break Bulk',
         'cargo_category': 'General',   'unit': 'Tonnes', 'is_hazmat_capable': False},
        {'cargo_type_key': 8, 'cargo_type_code': 'HAZ', 'cargo_type_name': 'Hazardous Cargo',
         'cargo_category': 'Container', 'unit': 'TEU',    'is_hazmat_capable': True},
    ])


# =============================================================
# 8. DIM_DIRECTION  (unchanged)
# =============================================================
def gen_dim_direction() -> pd.DataFrame:
    return pd.DataFrame([
        {'direction_key': 1, 'direction_code': 'IMP', 'direction_name': 'Import',
         'description': 'Goods arriving at port for inland distribution'},
        {'direction_key': 2, 'direction_code': 'EXP', 'direction_name': 'Export',
         'description': 'Goods departing port for overseas destination'},
        {'direction_key': 3, 'direction_code': 'TSH', 'direction_name': 'Transshipment',
         'description': 'Cargo transferred between vessels at the port'},
        {'direction_key': 4, 'direction_code': 'CST', 'direction_name': 'Coastal',
         'description': 'Domestic coastal/short-sea movements'},
    ])


# =============================================================
# 9. DIM_COUNTRY  (unchanged)
# =============================================================
def gen_dim_country() -> pd.DataFrame:
    return pd.DataFrame([
        {'country_key':  1, 'country_code': 'US', 'country_name': 'United States',  'region': 'North America',      'continent': 'Americas'},
        {'country_key':  2, 'country_code': 'CN', 'country_name': 'China',           'region': 'East Asia',          'continent': 'Asia'},
        {'country_key':  3, 'country_code': 'DE', 'country_name': 'Germany',         'region': 'Western Europe',     'continent': 'Europe'},
        {'country_key':  4, 'country_code': 'GB', 'country_name': 'United Kingdom',  'region': 'Northern Europe',    'continent': 'Europe'},
        {'country_key':  5, 'country_code': 'JP', 'country_name': 'Japan',           'region': 'East Asia',          'continent': 'Asia'},
        {'country_key':  6, 'country_code': 'KR', 'country_name': 'South Korea',     'region': 'East Asia',          'continent': 'Asia'},
        {'country_key':  7, 'country_code': 'IN', 'country_name': 'India',           'region': 'South Asia',         'continent': 'Asia'},
        {'country_key':  8, 'country_code': 'SG', 'country_name': 'Singapore',       'region': 'Southeast Asia',     'continent': 'Asia'},
        {'country_key':  9, 'country_code': 'NL', 'country_name': 'Netherlands',     'region': 'Western Europe',     'continent': 'Europe'},
        {'country_key': 10, 'country_code': 'BE', 'country_name': 'Belgium',         'region': 'Western Europe',     'continent': 'Europe'},
        {'country_key': 11, 'country_code': 'FR', 'country_name': 'France',          'region': 'Western Europe',     'continent': 'Europe'},
        {'country_key': 12, 'country_code': 'IT', 'country_name': 'Italy',           'region': 'Southern Europe',    'continent': 'Europe'},
        {'country_key': 13, 'country_code': 'ES', 'country_name': 'Spain',           'region': 'Southern Europe',    'continent': 'Europe'},
        {'country_key': 14, 'country_code': 'GR', 'country_name': 'Greece',          'region': 'Southern Europe',    'continent': 'Europe'},
        {'country_key': 15, 'country_code': 'TR', 'country_name': 'Turkey',          'region': 'Middle East',        'continent': 'Asia'},
        {'country_key': 16, 'country_code': 'AE', 'country_name': 'UAE',             'region': 'Middle East',        'continent': 'Asia'},
        {'country_key': 17, 'country_code': 'SA', 'country_name': 'Saudi Arabia',    'region': 'Middle East',        'continent': 'Asia'},
        {'country_key': 18, 'country_code': 'MA', 'country_name': 'Morocco',         'region': 'North Africa',       'continent': 'Africa'},
        {'country_key': 19, 'country_code': 'EG', 'country_name': 'Egypt',           'region': 'North Africa',       'continent': 'Africa'},
        {'country_key': 20, 'country_code': 'ZA', 'country_name': 'South Africa',    'region': 'Sub-Saharan Africa', 'continent': 'Africa'},
        {'country_key': 21, 'country_code': 'BR', 'country_name': 'Brazil',          'region': 'South America',      'continent': 'Americas'},
        {'country_key': 22, 'country_code': 'MX', 'country_name': 'Mexico',          'region': 'North America',      'continent': 'Americas'},
        {'country_key': 23, 'country_code': 'CA', 'country_name': 'Canada',          'region': 'North America',      'continent': 'Americas'},
        {'country_key': 24, 'country_code': 'AU', 'country_name': 'Australia',       'region': 'Oceania',            'continent': 'Oceania'},
        {'country_key': 25, 'country_code': 'MY', 'country_name': 'Malaysia',        'region': 'Southeast Asia',     'continent': 'Asia'},
        {'country_key': 26, 'country_code': 'TH', 'country_name': 'Thailand',        'region': 'Southeast Asia',     'continent': 'Asia'},
        {'country_key': 27, 'country_code': 'VN', 'country_name': 'Vietnam',         'region': 'Southeast Asia',     'continent': 'Asia'},
        {'country_key': 28, 'country_code': 'PK', 'country_name': 'Pakistan',        'region': 'South Asia',         'continent': 'Asia'},
        {'country_key': 29, 'country_code': 'ID', 'country_name': 'Indonesia',       'region': 'Southeast Asia',     'continent': 'Asia'},
        {'country_key': 30, 'country_code': 'PL', 'country_name': 'Poland',          'region': 'Eastern Europe',     'continent': 'Europe'},
    ])


# =============================================================
# 10. DIM_ROUTE  (unchanged)
# =============================================================
def gen_dim_route() -> pd.DataFrame:
    return pd.DataFrame([
        {'route_key':  1, 'route_code': 'ATA', 'route_name': 'Asia-Trans-Atlantic',
         'trade_lane': 'Asia-Americas',     'origin_region': 'East Asia',       'destination_region': 'North America',    'avg_transit_days': 28},
        {'route_key':  2, 'route_code': 'TAE', 'route_name': 'Trans-Atlantic Eastbound',
         'trade_lane': 'Trans-Atlantic',    'origin_region': 'North America',   'destination_region': 'Europe',           'avg_transit_days': 14},
        {'route_key':  3, 'route_code': 'TAW', 'route_name': 'Trans-Atlantic Westbound',
         'trade_lane': 'Trans-Atlantic',    'origin_region': 'Europe',          'destination_region': 'North America',    'avg_transit_days': 14},
        {'route_key':  4, 'route_code': 'ASM', 'route_name': 'Asia-Mediterranean',
         'trade_lane': 'Asia-Europe',       'origin_region': 'East Asia',       'destination_region': 'Southern Europe',  'avg_transit_days': 25},
        {'route_key':  5, 'route_code': 'MAS', 'route_name': 'Mediterranean-Asia',
         'trade_lane': 'Asia-Europe',       'origin_region': 'Southern Europe', 'destination_region': 'East Asia',        'avg_transit_days': 25},
        {'route_key':  6, 'route_code': 'IMD', 'route_name': 'Intra-Mediterranean',
         'trade_lane': 'Intra-Europe',      'origin_region': 'Southern Europe', 'destination_region': 'Southern Europe',  'avg_transit_days':  5},
        {'route_key':  7, 'route_code': 'MEA', 'route_name': 'Mediterranean-East Africa',
         'trade_lane': 'Med-Africa',        'origin_region': 'Southern Europe', 'destination_region': 'East Africa',      'avg_transit_days': 12},
        {'route_key':  8, 'route_code': 'MEW', 'route_name': 'Mediterranean-West Africa',
         'trade_lane': 'Med-Africa',        'origin_region': 'Southern Europe', 'destination_region': 'West Africa',      'avg_transit_days': 10},
        {'route_key':  9, 'route_code': 'NER', 'route_name': 'North Europe-Mediterranean',
         'trade_lane': 'Intra-Europe',      'origin_region': 'Northern Europe', 'destination_region': 'Southern Europe',  'avg_transit_days':  6},
        {'route_key': 10, 'route_code': 'ANE', 'route_name': 'Americas-North Europe',
         'trade_lane': 'Trans-Atlantic',    'origin_region': 'Americas',        'destination_region': 'Northern Europe',  'avg_transit_days': 12},
        {'route_key': 11, 'route_code': 'MEG', 'route_name': 'Mediterranean-Middle East',
         'trade_lane': 'Med-Middle East',   'origin_region': 'Southern Europe', 'destination_region': 'Middle East',      'avg_transit_days': 10},
        {'route_key': 12, 'route_code': 'PSW', 'route_name': 'Pacific Southwest',
         'trade_lane': 'Trans-Pacific',     'origin_region': 'East Asia',       'destination_region': 'North America',    'avg_transit_days': 16},
        {'route_key': 13, 'route_code': 'ISC', 'route_name': 'Indian Subcontinent-Med',
         'trade_lane': 'South Asia-Europe', 'origin_region': 'South Asia',      'destination_region': 'Southern Europe',  'avg_transit_days': 20},
        {'route_key': 14, 'route_code': 'SAM', 'route_name': 'South America-Mediterranean',
         'trade_lane': 'Americas-Europe',   'origin_region': 'South America',   'destination_region': 'Southern Europe',  'avg_transit_days': 18},
        {'route_key': 15, 'route_code': 'CST', 'route_name': 'Coastal/Domestic',
         'trade_lane': 'Domestic',          'origin_region': 'Domestic',        'destination_region': 'Domestic',         'avg_transit_days':  2},
    ])


# =============================================================
# 11. DIM_INCOTERM  (unchanged)
# =============================================================
def gen_dim_incoterm() -> pd.DataFrame:
    return pd.DataFrame([
        {'incoterm_key':  1, 'incoterm_code': 'EXW', 'incoterm_name': 'Ex Works',
         'risk_transfer': 'Seller premises',   'freight_responsibility': 'Buyer',  'insurance_responsibility': 'Buyer'},
        {'incoterm_key':  2, 'incoterm_code': 'FCA', 'incoterm_name': 'Free Carrier',
         'risk_transfer': 'Named place',       'freight_responsibility': 'Buyer',  'insurance_responsibility': 'Buyer'},
        {'incoterm_key':  3, 'incoterm_code': 'FAS', 'incoterm_name': 'Free Alongside Ship',
         'risk_transfer': 'Origin port',       'freight_responsibility': 'Buyer',  'insurance_responsibility': 'Buyer'},
        {'incoterm_key':  4, 'incoterm_code': 'FOB', 'incoterm_name': 'Free on Board',
         'risk_transfer': 'On board vessel',   'freight_responsibility': 'Buyer',  'insurance_responsibility': 'Buyer'},
        {'incoterm_key':  5, 'incoterm_code': 'CFR', 'incoterm_name': 'Cost and Freight',
         'risk_transfer': 'On board vessel',   'freight_responsibility': 'Seller', 'insurance_responsibility': 'Buyer'},
        {'incoterm_key':  6, 'incoterm_code': 'CIF', 'incoterm_name': 'Cost Insurance and Freight',
         'risk_transfer': 'On board vessel',   'freight_responsibility': 'Seller', 'insurance_responsibility': 'Seller'},
        {'incoterm_key':  7, 'incoterm_code': 'CPT', 'incoterm_name': 'Carriage Paid To',
         'risk_transfer': 'Named destination', 'freight_responsibility': 'Seller', 'insurance_responsibility': 'Buyer'},
        {'incoterm_key':  8, 'incoterm_code': 'CIP', 'incoterm_name': 'Carriage and Insurance Paid',
         'risk_transfer': 'Named destination', 'freight_responsibility': 'Seller', 'insurance_responsibility': 'Seller'},
        {'incoterm_key':  9, 'incoterm_code': 'DAP', 'incoterm_name': 'Delivered at Place',
         'risk_transfer': 'Destination',       'freight_responsibility': 'Seller', 'insurance_responsibility': 'Seller'},
        {'incoterm_key': 10, 'incoterm_code': 'DPU', 'incoterm_name': 'Delivered at Place Unloaded',
         'risk_transfer': 'After unloading',   'freight_responsibility': 'Seller', 'insurance_responsibility': 'Seller'},
        {'incoterm_key': 11, 'incoterm_code': 'DDP', 'incoterm_name': 'Delivered Duty Paid',
         'risk_transfer': 'Buyer premises',    'freight_responsibility': 'Seller', 'insurance_responsibility': 'Seller'},
    ])


# =============================================================
# HELPER: build carrier-route affinity weights for a given port
# =============================================================
def _build_carrier_route_weights(carrier_key: int,
                                  port_route_keys: list,
                                  port_route_w: list) -> list:
    """
    Apply CARRIER_ROUTE_AFFINITY boosts on top of base port route weights.
    Returns normalised weight list aligned to port_route_keys.
    """
    affinity = CARRIER_ROUTE_AFFINITY.get(carrier_key, {})
    adjusted = []
    for rk, bw in zip(port_route_keys, port_route_w):
        boost = affinity.get(rk, 1.0)
        adjusted.append(bw * boost)
    total = sum(adjusted)
    return [w / total for w in adjusted]


# =============================================================
# 12. FACT_PORT_CALLS
# =============================================================
def gen_fact_port_calls(vessels_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate port call records with:
    - Year-on-year call volume growth (~3-5%)
    - Monthly seasonality per port
    - Monthly-modulated delay rates and waiting hours
    - Carrier-route concentration
    """
    all_calls = []
    call_id   = 1
    all_dates = pd.date_range(START_DATE, END_DATE, freq='D')

    for year in YEARS:
        growth   = YOY_GROWTH[year]
        year_dates = [d for d in all_dates if d.year == year]

        for port_key, cfg in PORT_CONFIG.items():
            # Scale annual calls by YoY growth
            annual_calls = round(cfg['annual_calls'] * growth)

            for month in range(1, 13):
                n_month = round(annual_calls * cfg['seasonality'][month - 1])
                m_dates = [d for d in year_dates if d.month == month]
                if not m_dates or n_month < 1:
                    continue

                # Monthly delay rate multiplier (seasonal correlation)
                delay_mult = cfg['delay_mult'][month - 1]
                month_delay_rate = min(0.75, cfg['delay_rate'] * delay_mult)

                # Monthly waiting hours scale with delay rate
                month_waiting = cfg['avg_waiting_hrs'] * (
                    0.7 + 0.6 * (month_delay_rate / cfg['delay_rate'])
                )

                chosen_dates = np.random.choice(m_dates, size=n_month, replace=True)

                for call_date in chosen_dates:
                    vessel_row   = vessels_df.sample(1).iloc[0]
                    terminal_key = random.choices(cfg['terminals'],
                                                  weights=cfg['terminal_weights'])[0]

                    # Carrier selection with port-level weights
                    carrier_key = random.choices(cfg['carrier_keys'],
                                                  weights=cfg['carrier_w'])[0]

                    # Route selection with carrier-route affinity
                    route_w_adj = _build_carrier_route_weights(
                        carrier_key, cfg['route_keys'], cfg['route_w']
                    )
                    route_key = random.choices(cfg['route_keys'], weights=route_w_adj)[0]

                    # Stochastic operational times
                    arrival_dt  = datetime.combine(
                        call_date.date(), datetime.min.time()
                    ).replace(
                        hour=random.randint(0, 23),
                        minute=random.randint(0, 59)
                    )
                    waiting_hrs = max(0.0, np.random.exponential(month_waiting))
                    berth_hrs   = max(4.0, np.random.normal(cfg['avg_berth_hrs'],
                                                             cfg['avg_berth_hrs'] * 0.25))
                    is_delayed  = 1 if random.random() < month_delay_rate else 0
                    delay_hrs   = max(0.0, np.random.exponential(12)) if is_delayed else 0.0

                    berth_dt     = arrival_dt + timedelta(hours=waiting_hrs)
                    departure_dt = berth_dt   + timedelta(hours=berth_hrs + delay_hrs)
                    turnaround   = waiting_hrs + berth_hrs + delay_hrs

                    crane_moves = (max(0, int(np.random.normal(600, 200)))
                                   if vessel_row['vessel_type'] == 'Container Ship'
                                   else 0)

                    all_calls.append({
                        'port_call_id':        call_id,
                        'date_key':            int(call_date.strftime('%Y%m%d')),
                        'port_key':            port_key,
                        'terminal_key':        int(terminal_key),
                        'vessel_key':          int(vessel_row['vessel_key']),
                        'carrier_key':         int(carrier_key),
                        'ship_size_class_key': int(vessel_row['ship_size_class_key']),
                        'route_key':           int(route_key),
                        'arrival_datetime':    arrival_dt.strftime('%Y-%m-%d %H:%M:%S'),
                        'berth_datetime':      berth_dt.strftime('%Y-%m-%d %H:%M:%S'),
                        'departure_datetime':  departure_dt.strftime('%Y-%m-%d %H:%M:%S'),
                        'berth_waiting_hours': round(waiting_hrs, 2),
                        'at_berth_hours':      round(berth_hrs,   2),
                        'turnaround_hours':    round(turnaround,  2),
                        'crane_moves_total':   crane_moves,
                        'is_delayed':          is_delayed,
                        'delay_hours':         round(delay_hrs, 2),
                        'year':                year,
                        'month':               int(call_date.month),
                    })
                    call_id += 1

    return pd.DataFrame(all_calls)


# =============================================================
# 13. FACT_CARGO_MOVEMENTS
# =============================================================
# Approximate cargo value per TEU by cargo type (USD, mid-range)
_VALUE_PER_TEU = {1: 45_000, 2: 30_000, 3: 60_000, 8: 55_000}
_INCOTERM_W    = [0.03, 0.08, 0.04, 0.30, 0.12, 0.20, 0.05, 0.05, 0.08, 0.02, 0.03]


def gen_fact_cargo_movements(fpc: pd.DataFrame,
                              target_rows: int = 100_000) -> pd.DataFrame:
    """
    Generate cargo movement records with:
    - Port-calibrated cargo type mix
    - YoY TEU volume growth embedded in movement volumes
    - Correlated hazardous flag for liquid bulk
    """
    avg_per_call   = target_rows / len(fpc)
    all_movements  = []
    mv_id          = 1

    for _, call in fpc.iterrows():
        port_key = int(call['port_key'])
        cfg      = PORT_CONFIG[port_key]
        year     = int(call['year'])
        growth   = YOY_GROWTH[year]

        n_mv = max(1, int(np.random.poisson(avg_per_call)))

        for _ in range(n_mv):
            ctype_key   = random.choices(range(1, 9), weights=cfg['cargo_type_w'])[0]
            dir_key     = random.choices([1, 2, 3, 4], weights=cfg['direction_w'])[0]
            country_key = random.choice(cfg['country_keys'])
            route_key   = random.choices(cfg['route_keys'], weights=cfg['route_w'])[0]
            incoterm_key= random.choices(range(1, 12), weights=_INCOTERM_W)[0]

            # Volume generation calibrated per cargo category; scale by YoY growth
            if ctype_key in (1, 2, 3, 8):   # Container types
                teu   = max(1, int(random.randint(1, 500) * growth))
                wt    = teu * random.uniform(8, 14)
                units = teu
                val   = teu * _VALUE_PER_TEU.get(ctype_key, 40_000) \
                              * random.uniform(0.7, 1.3)
            elif ctype_key == 6:              # RoRo
                units = random.randint(10, 800)
                teu   = 0
                wt    = units * random.uniform(1.5, 4.0)
                val   = units * random.uniform(20_000, 60_000)
            elif ctype_key in (4, 5, 7):      # Bulk / break-bulk
                teu   = 0
                units = 0
                wt    = random.uniform(500, 50_000) * growth
                val   = wt * random.uniform(100, 800)
            else:
                teu   = random.randint(1, 100)
                wt    = teu * 10
                units = teu
                val   = teu * 30_000

            is_haz = 1 if (ctype_key == 8 or
                           (ctype_key == 5 and random.random() < 0.30)) else 0

            all_movements.append({
                'cargo_movement_id':   mv_id,
                'port_call_id':        int(call['port_call_id']),
                'date_key':            int(call['date_key']),
                'port_key':            port_key,
                'terminal_key':        int(call['terminal_key']),
                'vessel_key':          int(call['vessel_key']),
                'carrier_key':         int(call['carrier_key']),
                'cargo_type_key':      ctype_key,
                'direction_key':       dir_key,
                'country_key':         country_key,
                'route_key':           route_key,
                'incoterm_key':        incoterm_key,
                'ship_size_class_key': int(call['ship_size_class_key']),
                'teu_count':           teu,
                'weight_tonnes':       round(wt, 2),
                'unit_count':          units,
                'cargo_value_usd':     round(val, 2),
                'is_hazardous':        is_haz,
            })
            mv_id += 1

    return pd.DataFrame(all_movements)


# =============================================================
# MAIN
# =============================================================
def save(df: pd.DataFrame, name: str) -> None:
    path = os.path.join(OUTPUT_DIR, f'{name}.csv')
    # Fill NaN in string columns with empty string before writing
    # Prevents Postgres COPY from receiving NULL for NOT NULL text columns
    str_cols = df.select_dtypes(include=['object']).columns
    df[str_cols] = df[str_cols].fillna('')
    df.to_csv(path, index=False)
    print(f'  ✓  {name:<30}  {len(df):>8,} rows  →  {path}')


def main() -> None:
    print('=' * 65)
    print('Port Analytics — Synthetic Data Generator v2')
    print('=' * 65)

    print('\n[1/3] Generating dimension tables …')
    dims = {
        'dim_date':            gen_dim_date(),
        'dim_port':            gen_dim_port(),
        'dim_terminal':        gen_dim_terminal(),
        'dim_ship_size_class': gen_dim_ship_size_class(),
        'dim_vessel':          gen_dim_vessel(200),
        'dim_carrier':         gen_dim_carrier(),
        'dim_cargo_type':      gen_dim_cargo_type(),
        'dim_direction':       gen_dim_direction(),
        'dim_country':         gen_dim_country(),
        'dim_route':           gen_dim_route(),
        'dim_incoterm':        gen_dim_incoterm(),
    }

    print('\n[2/3] Generating fact tables …')
    fpc = gen_fact_port_calls(dims['dim_vessel'])
    fcm = gen_fact_cargo_movements(fpc, target_rows=100_000)

    print('\n[3/3] Writing CSVs …')
    for name, df in dims.items():
        save(df, name)
    save(fpc, 'fact_port_calls')
    save(fcm, 'fact_cargo_movements')

    print('\n' + '=' * 65)
    print(f'fact_port_calls      : {len(fpc):>8,} rows')
    print(f'fact_cargo_movements : {len(fcm):>8,} rows')
    print('Done. Run load_data.py to push CSVs into Postgres.')
    print('=' * 65)


if __name__ == '__main__':
    main()
