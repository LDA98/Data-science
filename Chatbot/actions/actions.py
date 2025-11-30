"""
Azioni custom del TennisBot.

Database principale: tennisbot.db con le tabelle:
  - players
  - matches

Il modulo espone le action Rasa:
  - action_player_info
  - action_player_stats
  - action_apply_filters
  - action_head_to_head
  - action_tournament_info
  - action_match_result
  - action_ongoing_tournaments
  - action_default_fallback
  - action_reset_slots
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Text, Union

from rasa_sdk import Action, Tracker
from rasa_sdk.events import FollowupAction, SlotSet
from rasa_sdk.executor import CollectingDispatcher

# ==============================================================================
# Config / helpers
# ==============================================================================

# Percorso del database SQLite utilizzato da tutte le query.
DB_PATH = os.getenv("TENNISBOT_DB", "tennisbot.db")


# Mappa i codici ATP alle etichette leggibili in italiano.
SURFACE_LABELS = {
    "Hard": "cemento",
    "Grass": "erba",
    "Clay": "terra battuta",
    "Carpet": "moquette",
}


# Sinonimi riconosciuti per inferire il codice superficie dal testo libero.
SURFACE_KEYWORDS = {
    "terra battuta": "Clay",
    "terra": "Clay",
    "clay": "Clay",
    "erba": "Grass",
    "grass": "Grass",
    "cemento": "Hard",
    "cement": "Hard",
    "hard": "Hard",
    "moquette": "Carpet",
    "carpet": "Carpet",
}

# Set di codici superficie ammessi dal DB.
VALID_SURFACE_CODES = set(SURFACE_LABELS.keys())
# Slot tecnico per ricordare quale action è stata eseguita prima di un filtro.
LAST_CONTEXT_SLOT = "last_context_action"


def action_ran_after_latest_user(tracker: Tracker, action_name: Text) -> bool:
    """Restituisce True se l'action indicata è già stata eseguita dopo l'ultimo messaggio utente."""
    events = getattr(tracker, "events", None)
    if not events:
        return False

    last_user_index: Optional[int] = None
    for idx, event in enumerate(events):
        if getattr(event, "event", None) == "user":
            last_user_index = idx

    if last_user_index is None or last_user_index == len(events) - 1:
        return False

    for event in events[last_user_index + 1 :]:
        if getattr(event, "event", None) == "action" and getattr(event, "name", None) == action_name:
            return True

    return False


def describe_filters(year: Optional[str], surface: Optional[str], tournament: Optional[str]) -> List[str]:
    """Restituisce una lista di filtri leggibili (anno/superficie/torneo)."""
    parts: List[str] = []
    if year:
        parts.append(f"anno: {year}")
    if surface:
        parts.append(f"superficie: {SURFACE_LABELS.get(surface, surface)}")
    if tournament:
        parts.append(f"torneo: {tournament}")
    return parts


def get_db_connection() -> sqlite3.Connection:
    """Restituisce una connessione SQLite usando il percorso assoluto del DB."""
    db_path = DB_PATH
    if not os.path.isabs(db_path):
        db_path = os.path.abspath(db_path)
    return sqlite3.connect(db_path)
    # Restituisce una connessione SQLite; ricordarsi di chiamare `conn.close()` dopo l'uso.


def format_tournament_date(date_str: Optional[str]) -> str:
    """Converte date nel formato YYYYMMDD in DD/MM/YYYY quando possibile."""
    if not date_str:
        return "N/A"
    s = str(date_str)
    if len(s) != 8 or not s.isdigit():
        return s
    return f"{s[6:8]}/{s[4:6]}/{s[0:4]}"
    # Converte 'YYYYMMDD' in formato leggibile 'DD/MM/YYYY'. Se non valido, ritorna l'input.


def get_player_name_by_id(player_id: str, cursor: sqlite3.Cursor) -> str:
    if not player_id:
        return "Unknown"
    cursor.execute("SELECT player_name FROM players WHERE id = ?", (player_id,))
    row = cursor.fetchone()
    return row[0] if row else player_id
    # Se l'id non è presente nel DB, ritorniamo l'id grezzo come fallback.


def normalize_year_field(value: Optional[str]) -> str:
    """Normalizza il campo turned_pro rendendolo un anno leggibile."""
    if value is None:
        return ""
    # Restituisce anno normalizzato o stringa vuota se non estraibile.
    try:
        s = str(value).strip()
        match = re.search(r"(19|20)\d{2}", s)
        if match:
            return match.group(0)
        return str(int(float(s)))
    except Exception:
        return ""


def ioc_to_flag(ioc_code: Optional[str]) -> str:
    """Converte un codice IOC a tre lettere aggiungendo la bandiera emoji quando possibile."""
    if not ioc_code:
        return ""
    code = str(ioc_code).strip().upper()
    mapping = {
        "USA": "US",
        "GBR": "GB",
        "ESP": "ES",
        "ITA": "IT",
        "FRA": "FR",
        "GER": "DE",
        "DEU": "DE",
        "ARG": "AR",
        "AUS": "AU",
        "SRB": "RS",
        "RUS": "RU",
        "SUI": "CH",
        "NED": "NL",
        "SWE": "SE",
        "NOR": "NO",
        "POL": "PL",
        "JPN": "JP",
        "CHN": "CN",
        "KOR": "KR",
        "KAZ": "KZ",
        "CZE": "CZ",
        "SVK": "SK",
        "CRO": "HR",
        "BEL": "BE",
        "POR": "PT",
        "DEN": "DK",
        "GRE": "GR",
        "BUL": "BG",
        "ROU": "RO",
        "HUN": "HU",
        "TUR": "TR",
        "MEX": "MX",
        "BRA": "BR",
        "CAN": "CA",
        "NZL": "NZ",
        "IRL": "IE",
        "ISR": "IL",
        "IND": "IN",
        "THA": "TH",
        "VNM": "VN",
        "ZAF": "ZA",
        "ZIM": "ZW",
        "EGY": "EG",
        "SAU": "SA",
        "UAE": "AE",
        "COL": "CO",
        "URU": "UY",
        "CHL": "CL",
        "PER": "PE",
        "LTU": "LT",
        "LUX": "LU",
        "LVA": "LV",
        "EST": "EE",
        "SVN": "SI",
    }
    alpha2 = mapping.get(code)
    if not alpha2:
        return code
    try:
        flag = "".join(chr(ord(ch) - ord("A") + 0x1F1E6) for ch in alpha2.upper())
        return f"{code} {flag}"
    except Exception:
        return code
    # Ritorna codice IOC e emoji bandiera quando possibile.


def extract_year_from_text(text: str) -> Optional[str]:
    """Restituisce il primo anno YYYY trovato nel testo."""
    if not text:
        return None
    match = re.search(r"\b(?:19|20)\d{2}\b", text)
    return match.group(0) if match else None


def extract_name_from_text(text: str) -> str:
    """Helper legacy mantenuto per retrocompatibilità."""
    if not text:
        return ""
    s = str(text).strip()
    m = re.match(r"(?i)^\s*chi\s*(?:\u00E8|e'|e)\s+(.+)$", s)
    if not m:
        m = re.match(r"(?i)^\s*who\s+is\s+(.+)$", s)
    if m:
        name = m.group(1).strip()
        return re.sub(r"[\?\!\.,]+$", "", name).strip()
    return ""


def extract_name_from_text_fixed(text: str) -> str:
    """Estrae il nome del giocatore da domande del tipo 'chi è Sinner?'."""
    name = extract_name_from_text(text)
    return name or ""


def extract_surface_from_text(text: str) -> Optional[str]:
    """Normalizza eventuali riferimenti alla superficie nel valore salvato a DB."""
    if not text:
        return None
    s = str(text).lower()
    for key, value in SURFACE_KEYWORDS.items():
        if key in s:
            return value
    return None


def normalize_surface_value(value: Optional[str]) -> Optional[str]:
    """Normalizza il valore della superficie in un codice compatibile con il DB."""
    if not value:
        return None
    return SURFACE_KEYWORDS.get(str(value).lower(), extract_surface_from_text(str(value)))


def normalize_year_value(value: Optional[str]) -> Optional[str]:
    """Normalizza il valore dell'anno nel formato YYYY."""
    if not value:
        return None
    s = str(value)
    match = re.search(r"(19|20)\d{2}", s)
    return match.group(0) if match else None


@dataclass
class FilterContext:
    """Contesto centralizzato dei filtri estratti dall'utente."""
    intent_name: Text
    text: Text
    entities: List[Dict[Text, Any]]
    year: Optional[str]
    surface: Optional[str]
    tournament: Optional[str]
    message_year: Optional[str]
    message_surface: Optional[str]
    message_tournament: Optional[str]
    slot_year: Optional[str]
    slot_surface: Optional[str]
    slot_tournament: Optional[str]
    explicit_year: bool
    explicit_surface: bool
    explicit_tournament: bool

    @property
    def is_inform(self) -> bool:
        return self.intent_name == "inform_filters"

    def describe(self) -> List[str]:
        return describe_filters(self.year, self.surface, self.tournament)

    def slot_events(self, clear_unset: bool = False) -> List[SlotSet]:
        events: List[SlotSet] = []
        if self.is_inform:
            if self.explicit_year:
                events.append(SlotSet("year", self.message_year))
            if self.explicit_surface:
                events.append(SlotSet("surface", self.message_surface))
            if self.explicit_tournament:
                events.append(SlotSet("tournament_name", self.message_tournament))
            return events

        if self.explicit_year:
            events.append(SlotSet("year", self.year))
        elif clear_unset:
            events.append(SlotSet("year", None))

        if self.explicit_surface:
            events.append(SlotSet("surface", self.surface))
        elif clear_unset:
            events.append(SlotSet("surface", None))

        if self.explicit_tournament:
            events.append(SlotSet("tournament_name", self.tournament))
        elif clear_unset:
            events.append(SlotSet("tournament_name", None))

        return events

    def active_slot_events(self) -> List[SlotSet]:
        events: List[SlotSet] = []
        if self.year:
            events.append(SlotSet("year", self.year))
        if self.surface:
            events.append(SlotSet("surface", self.surface))
        if self.tournament:
            events.append(SlotSet("tournament_name", self.tournament))
        return events


def build_filter_context(tracker: Tracker) -> FilterContext:
    """Aggrega le informazioni dell'ultimo messaggio e degli slot in un unico contesto filtro."""
    latest = tracker.latest_message or {}
    intent_name = (latest.get("intent") or {}).get("name") or ""
    text = latest.get("text", "") or ""
    entities = (latest.get("entities") or [])[:]

    year_entities = [e.get("value") for e in entities if e.get("entity") == "year"]
    surface_entities = [e.get("value") for e in entities if e.get("entity") == "surface"]
    tournament_entities = [e.get("value") for e in entities if e.get("entity") == "tournament"]

    # Analizza il testo e le entità per capire se l'utente ha specificato anno/superficie/torneo
    message_year = normalize_year_value(year_entities[0]) if year_entities else extract_year_from_text(text)
    message_surface = (
        normalize_surface_value(surface_entities[0]) if surface_entities else extract_surface_from_text(text)
    )
    message_tournament = tournament_entities[0] if tournament_entities else None

    slot_year = tracker.get_slot("year")
    slot_surface = tracker.get_slot("surface")
    slot_tournament = tracker.get_slot("tournament_name")

    if intent_name == "inform_filters":
        year = message_year or slot_year
        surface = message_surface or slot_surface
        tournament = message_tournament or slot_tournament
        explicit_year = message_year is not None
        explicit_surface = message_surface is not None
        explicit_tournament = message_tournament is not None
    else:
        year = message_year
        surface = message_surface
        tournament = message_tournament
        explicit_year = year is not None
        explicit_surface = surface is not None
        explicit_tournament = tournament is not None

    # Raccoglie i dati normalizzati in un'unica struttura comoda da passare alle azioni
    return FilterContext(
        intent_name=intent_name,
        text=text,
        entities=entities,
        year=year,
        surface=surface,
        tournament=tournament,
        message_year=message_year,
        message_surface=message_surface,
        message_tournament=message_tournament,
        slot_year=slot_year,
        slot_surface=slot_surface,
        slot_tournament=slot_tournament,
        explicit_year=explicit_year,
        explicit_surface=explicit_surface,
        explicit_tournament=explicit_tournament,
    )


def fetch_rows_as_dicts(
    cursor: sqlite3.Cursor,
    query: str,
    params: List[Any],
    include_raw: bool = False,
) -> List[Dict[str, Any]]:
    cursor.execute(query, params)
    columns = [desc[0] for desc in cursor.description]
    rows = []
    for raw in cursor.fetchall():
        data = dict(zip(columns, raw))
        if include_raw:
            data["__raw__"] = raw
        rows.append(data)
    return rows


def make_match_signature(row: Union[Dict[str, Any], Tuple[Any, ...]]) -> Tuple[Any, ...]:
    """Costruisce una firma hashabile per riconoscere i match unici."""
    if isinstance(row, dict):
        tourney_date = row.get("tourney_date")
        tourney_name = (row.get("tourney_name") or "").strip().lower()
        round_name = (row.get("round") or "").strip().lower()
        match_num = row.get("match_num")
        winner_id = str(row.get("winner_id") or "")
        loser_id = str(row.get("loser_id") or "")
        score = (row.get("score") or "").strip()
    else:
        tourney_date = row[5] if len(row) > 5 else None
        tourney_name = (row[1] or "").strip().lower() if len(row) > 1 else ""
        round_name = (row[13] or "").strip().lower() if len(row) > 13 else ""
        match_num = row[6] if len(row) > 6 else None
        winner_id = str(row[7] or "") if len(row) > 7 else ""
        loser_id = str(row[8] or "") if len(row) > 8 else ""
        score = (row[11] or "").strip() if len(row) > 11 else ""

    participants = tuple(sorted((winner_id, loser_id)))
    return (
        str(tourney_date or ""),
        tourney_name,
        round_name,
        participants,
        str(match_num or ""),
        score,
    )


def deduplicate_matches(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Rimuove i duplicati (stesso evento/data/vincitore/perdente/punteggio)."""
    seen: set = set()
    unique_rows: List[Dict[str, Any]] = []
    for row in rows:
        key = make_match_signature(row)
        if key not in seen:
            seen.add(key)
            unique_rows.append(row)
    return unique_rows


def fetch_unique_match_dicts(
    cursor: sqlite3.Cursor,
    query: str,
    params: List[Any],
) -> List[Dict[str, Any]]:
    """Esegue la query restituendo dizionari di match (con la tupla raw) senza duplicati."""
    # Uso il raw tuple per avere tutte le colonne disponibili nelle stampe dettagliate
    rows = fetch_rows_as_dicts(cursor, query, params, include_raw=True)
    unique_rows: List[Dict[str, Any]] = []
    seen: set = set()
    for row in rows:
        raw = row.get("__raw__")
        key = make_match_signature(raw if raw is not None else row)
        if key in seen:
            continue
        seen.add(key)
        unique_rows.append(row)
    return unique_rows


def find_best_tournament(
    cursor: sqlite3.Cursor,
    candidate: Optional[str],
) -> Optional[str]:
    """Restituisce il nome del torneo che più si avvicina al candidato, se presente."""
    if not candidate:
        return None
    clean = candidate.strip()
    if len(clean) < 3:
        return None

    cursor.execute(
        """
        SELECT tourney_name
        FROM matches
        WHERE LOWER(tourney_name) = LOWER(?)
        ORDER BY tourney_date DESC
        LIMIT 1
        """,
        (clean,),
    )
    row = cursor.fetchone()
    if row:
        return row[0]

    cursor.execute(
        """
        SELECT tourney_name, COUNT(*) AS cnt
        FROM matches
        WHERE LOWER(tourney_name) LIKE LOWER(?)
        GROUP BY tourney_name
        ORDER BY cnt DESC
        LIMIT 1
        """,
        (f"%{clean}%",),
    )
    row = cursor.fetchone()
    if row:
        return row[0]

    cursor.execute(
        """
        SELECT tourney_name, COUNT(*) AS cnt
        FROM matches
        WHERE LOWER(tourney_name) LIKE LOWER(?)
        GROUP BY tourney_name
        ORDER BY cnt DESC
        LIMIT 1
        """,
        (f"%{'%'.join(clean.split())}%",),
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    return None


def guess_tournament_from_text(
    cursor: sqlite3.Cursor,
    text: str,
    exclude: Optional[List[str]] = None,
) -> Optional[str]:
    """Prova a individuare in modo euristico un torneo citato nel testo."""
    if not text:
        return None
    exclude_set = {s.strip().lower() for s in (exclude or []) if s}
    tokens = re.findall(r"[A-Za-zÀ-ÖØ-öø-ÿ']+", text)
    tokens = [t for t in tokens if len(t) >= 3]
    if not tokens:
        return None

    candidates: List[str] = []
    max_len = min(3, len(tokens))
    for length in range(max_len, 0, -1):
        for i in range(len(tokens) - length + 1):
            phrase = " ".join(tokens[i : i + length])
            if phrase.lower() in exclude_set:
                continue
            candidates.append(phrase)

    tried: set = set()
    for phrase in candidates:
        key = phrase.lower()
        if key in tried:
            continue
        tried.add(key)
        tournament = find_best_tournament(cursor, phrase)
        if tournament:
            return tournament
    return None


def format_match_details(match_row: Dict[str, Any], cursor: sqlite3.Cursor) -> List[str]:
    """Restituisce una descrizione testuale dettagliata di un singolo match."""
    raw = match_row.get("__raw__")
    info = get_match_display_info(raw, cursor) if raw else {}

    # Se il DB ha ID ma non i nomi, recupero i nomi leggibili tramite get_match_display_info
    tournament = info.get("tournament") or display_value(match_row.get("tourney_name"))
    round_name = info.get("round") or display_value(match_row.get("round"))
    year = info.get("year") or (str(match_row.get("tourney_date") or "")[:4] or "N/A")
    header = to_unicode_bold(f"{tournament} {year}") + f" - {round_name}"

    winner_name = info.get("winner") or display_value(match_row.get("winner_id"))
    loser_name = info.get("loser") or display_value(match_row.get("loser_id"))
    score = info.get("score") or display_value(match_row.get("score"))

    surface_code = match_row.get("surface")
    surface_label = SURFACE_LABELS.get(surface_code, display_value(surface_code))
    level = display_value(match_row.get("tourney_level"))
    match_date = format_tournament_date(match_row.get("tourney_date"))
    draw_size = display_value(match_row.get("draw_size"))
    match_num = display_value(match_row.get("match_num"))
    best_of = display_value(match_row.get("best_of"))
    minutes = safe_int(match_row.get("minutes"))
    winner_seed = display_value(match_row.get("winner_seed"), "-")
    loser_seed = display_value(match_row.get("loser_seed"), "-")
    ongoing_flag = safe_int(match_row.get("ongoing"))

    def pct(numer: int, denom: int) -> float:
        return (numer / denom * 100.0) if denom > 0 else 0.0

    lines: List[str] = [
        header,
        f"{winner_name} bt {loser_name} {score}",
        f"Superficie: {surface_label}",
        f"Livello: {level}",
    ]
    if match_date != "N/A":
        lines.append(f"Data: {match_date}")
    lines.append(f"Draw: {draw_size} | Match #: {match_num} | Best of: {best_of}")
    if minutes:
        lines.append(f"Durata: {minutes} minuti")
    if winner_seed != "-" or loser_seed != "-":
        lines.append(f"Seed: {winner_name} {winner_seed} / {loser_name} {loser_seed}")
    lines.append(f"Match ID interno: {display_value(match_row.get('match_id'))}")
    if ongoing_flag:
        lines.append("Stato: incontro in corso (dati parziali)")

    # Winner stats
    w_ace = safe_int(match_row.get("w_ace"))
    w_df = safe_int(match_row.get("w_df"))
    w_svpt = safe_int(match_row.get("w_svpt"))
    w_1stIn = safe_int(match_row.get("w_1stIn"))
    w_1stWon = safe_int(match_row.get("w_1stWon"))
    w_2ndWon = safe_int(match_row.get("w_2ndWon"))
    w_SvGms = safe_int(match_row.get("w_SvGms"))
    w_bpSaved = safe_int(match_row.get("w_bpSaved"))
    w_bpFaced = safe_int(match_row.get("w_bpFaced"))
    w_second_total = max(w_svpt - w_1stIn, 0)

    lines.append("")
    lines.append(to_unicode_bold(f"Statistiche {winner_name}"))
    lines.append(f"Ace: {w_ace} | Doppi falli: {w_df}")
    if w_svpt:
        lines.append(f"Prime in: {w_1stIn}/{w_svpt} ({pct(w_1stIn, w_svpt):.1f}%)")
    else:
        lines.append("Prime in: 0/0 (-)")
    if w_1stIn:
        lines.append(f"Punti vinti con la 1a: {w_1stWon}/{w_1stIn} ({pct(w_1stWon, w_1stIn):.1f}%)")
    else:
        lines.append("Punti vinti con la 1a: 0/0 (-)")
    if w_second_total:
        lines.append(f"Punti vinti con la 2a: {w_2ndWon}/{w_second_total} ({pct(w_2ndWon, w_second_total):.1f}%)")
    else:
        lines.append("Punti vinti con la 2a: 0/0 (-)")
    lines.append(f"Game al servizio: {w_SvGms}")
    if w_bpFaced:
        lines.append(f"Break point salvati: {w_bpSaved}/{w_bpFaced} ({pct(w_bpSaved, w_bpFaced):.1f}%)")
    elif w_bpSaved:
        lines.append(f"Break point salvati: {w_bpSaved}/0 (-)")
    else:
        lines.append("Break point salvati: n/d (nessuno affrontato)")

    # Loser stats
    l_ace = safe_int(match_row.get("l_ace"))
    l_df = safe_int(match_row.get("l_df"))
    l_svpt = safe_int(match_row.get("l_svpt"))
    l_1stIn = safe_int(match_row.get("l_1stIn"))
    l_1stWon = safe_int(match_row.get("l_1stWon"))
    l_2ndWon = safe_int(match_row.get("l_2ndWon"))
    l_SvGms = safe_int(match_row.get("l_SvGms"))
    l_bpSaved = safe_int(match_row.get("l_bpSaved"))
    l_bpFaced = safe_int(match_row.get("l_bpFaced"))
    l_second_total = max(l_svpt - l_1stIn, 0)

    lines.append("")
    lines.append(to_unicode_bold(f"Statistiche {loser_name}"))
    lines.append(f"Ace: {l_ace} | Doppi falli: {l_df}")
    if l_svpt:
        lines.append(f"Prime in: {l_1stIn}/{l_svpt} ({pct(l_1stIn, l_svpt):.1f}%)")
    else:
        lines.append("Prime in: 0/0 (-)")
    if l_1stIn:
        lines.append(f"Punti vinti con la 1a: {l_1stWon}/{l_1stIn} ({pct(l_1stWon, l_1stIn):.1f}%)")
    else:
        lines.append("Punti vinti con la 1a: 0/0 (-)")
    if l_second_total:
        lines.append(f"Punti vinti con la 2a: {l_2ndWon}/{l_second_total} ({pct(l_2ndWon, l_second_total):.1f}%)")
    else:
        lines.append("Punti vinti con la 2a: 0/0 (-)")
    lines.append(f"Game al servizio: {l_SvGms}")
    if l_bpFaced:
        lines.append(f"Break point salvati: {l_bpSaved}/{l_bpFaced} ({pct(l_bpSaved, l_bpFaced):.1f}%)")
    elif l_bpSaved:
        lines.append(f"Break point salvati: {l_bpSaved}/0 (-)")
    else:
        lines.append("Break point salvati: n/d (nessuno affrontato)")

    return lines


def safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def display_value(value: Any, default: str = "N/A") -> str:
    """Restituisce una stringa leggibile, usando `default` quando il valore è vuoto."""
    if value is None:
        return default
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or default
    return str(value)


def validate_and_find_player(
    player_name: str, cursor: sqlite3.Cursor
) -> Tuple[Optional[str], Optional[str]]:
    """Trova l'ID del giocatore tramite match esatto, per prefisso o fuzzy."""
    if not player_name or len(player_name.strip()) < 2:
        return None, None
    clean = player_name.strip()

    cursor.execute(
        "SELECT id, player_name FROM players WHERE LOWER(player_name) = LOWER(?)",
        (clean,),
    )
    row = cursor.fetchone()
    if row:
        return row[0], row[1]

    cursor.execute(
        """
        SELECT p.id, p.player_name, MAX(m.tourney_date) AS last_match
        FROM players p
        LEFT JOIN matches m ON (p.id = m.winner_id OR p.id = m.loser_id)
        WHERE LOWER(p.player_name) LIKE LOWER(?)
        GROUP BY p.id, p.player_name
        ORDER BY (last_match IS NULL) ASC, last_match DESC
        LIMIT 1
        """,
        (f"{clean}%",),
    )
    row = cursor.fetchone()
    if row:
        return row[0], row[1]

    cursor.execute(
        """
        SELECT p.id, p.player_name, MAX(m.tourney_date) AS last_match, COUNT(m.match_id) AS match_count
        FROM players p
        LEFT JOIN matches m ON (p.id = m.winner_id OR p.id = m.loser_id)
        WHERE LOWER(p.player_name) LIKE LOWER(?)
        GROUP BY p.id, p.player_name
        ORDER BY (last_match IS NULL) ASC, last_match DESC, match_count DESC
        LIMIT 1
        """,
        (f"%{clean}%",),
    )
    row = cursor.fetchone()
    if row:
        return row[0], row[1]

    return None, None


def find_similar_names(
    query: str, table: str, column: str, limit: int = 3
) -> List[str]:
    """Restituisce al massimo `limit` nomi simili (LIKE + fuzzy).
    Cerca nel DB nomi che contengono la query; se non trova usa fuzzy matching.
    Restituisce nomi effettivamente presenti nella tabella richiesta.
    """
    import difflib

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute(
            f"SELECT DISTINCT {column} FROM {table} WHERE LOWER({column}) LIKE LOWER(?) LIMIT ?",
            (f"%{query}%", limit),
        )
        results = [r[0] for r in cur.fetchall()]
        if results:
            conn.close()
            return results

        cur.execute(f"SELECT DISTINCT {column} FROM {table}")
        all_names = [r[0] for r in cur.fetchall() if r[0]]
        conn.close()

        last_name_map: Dict[str, List[str]] = {}
        for name in all_names:
            parts = re.split(r"\s+", name.strip())
            last = parts[-1].lower() if parts else name.lower()
            last_name_map.setdefault(last, []).append(name)

        q = str(query).strip().lower()
        if " " in q:
            lc_to_orig = {n.lower(): n for n in all_names}
            matches = difflib.get_close_matches(q, list(lc_to_orig.keys()), n=limit, cutoff=0.6)
            if matches:
                return [lc_to_orig[m] for m in matches]

        scored: List[tuple] = []
        for last, names in last_name_map.items():
            ratio = 1.0 - difflib.SequenceMatcher(None, q, last).ratio()
            scored.append((ratio, last, names))
        scored.sort(key=lambda x: x[0])

        suggestions: List[str] = []
        for ratio, _, names in scored:
            if ratio <= 0.5:
                for name in names:
                    if name not in suggestions:
                        suggestions.append(name)
                        if len(suggestions) >= limit:
                            return suggestions

        lc_to_orig = {n.lower(): n for n in all_names}
        matches = difflib.get_close_matches(q, list(lc_to_orig.keys()), n=limit, cutoff=0.5)
        return [lc_to_orig[m] for m in matches]
    except Exception:
        return []


def make_intent_payload(intent: str, entities: Dict[str, Any]) -> str:
    """Costruisce il payload Rasa `/intent{"entity":"value"}`."""
    try:
        return f"/{intent}{json.dumps(entities, ensure_ascii=False)}"
    except Exception:
        return f"/{intent}"


def to_unicode_bold(text: str) -> str:
    """Restituisce il testo in grassetto usando i caratteri Unicode "mathematical bold"."""
    result: List[str] = []
    for ch in text:
        if "A" <= ch <= "Z":
            result.append(chr(ord(ch) - ord("A") + 0x1D400))
        elif "a" <= ch <= "z":
            result.append(chr(ord(ch) - ord("a") + 0x1D41A))
        elif "0" <= ch <= "9":
            result.append(chr(ord(ch) - ord("0") + 0x1D7CE))
        else:
            result.append(ch)
    return "".join(result)


def get_match_display_info(match_row: Tuple[Any, ...], cursor: sqlite3.Cursor) -> Dict[str, Any]:
    """Converte la tupla grezza del match in un dizionario leggibile."""
    if not match_row or len(match_row) < 34:
        return {}

    (
        match_id,
        tourney_name,
        surface,
        draw_size,
        tourney_level,
        tourney_date,
        match_num,
        winner_id,
        loser_id,
        winner_seed,
        loser_seed,
        score,
        best_of,
        round_name,
        minutes,
        w_ace,
        w_df,
        w_svpt,
        w_1stIn,
        w_1stWon,
        w_2ndWon,
        w_SvGms,
        w_bpSaved,
        w_bpFaced,
        l_ace,
        l_df,
        l_svpt,
        l_1stIn,
        l_1stWon,
        l_2ndWon,
        l_SvGms,
        l_bpSaved,
        l_bpFaced,
        ongoing,
    ) = match_row

    winner_name = get_player_name_by_id(winner_id, cursor)
    loser_name = get_player_name_by_id(loser_id, cursor)
    year = str(tourney_date)[:4] if tourney_date else "N/A"

    return {
        "match_id": match_id,
        "tournament": tourney_name,
        "surface": surface,
        "level": tourney_level,
        "date": format_tournament_date(tourney_date),
        "year": year,
        "winner": winner_name,
        "loser": loser_name,
        "winner_id": winner_id,
        "loser_id": loser_id,
        "score": score,
        "round": round_name,
        "minutes": minutes or 0,
        "ongoing": bool(ongoing),
    }


# ==============================================================================
# Actions
# ==============================================================================


class ActionPlayerInfo(Action):
    """Restituisce le informazioni principali del giocatore e l'ultimo match.

    Action che risponde all'intent 'player_info'. Usa entità/slot per trovare il
    giocatore; se non trovato propone suggerimenti presi dal DB.
    """

    def name(self) -> Text:
        return "action_player_info"

    def run(
        self,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any],
    ) -> List[Dict[Text, Any]]:
        entities = tracker.latest_message.get("entities", [])
        players = [e.get("value") for e in entities if e.get("entity") == "player"]
        raw_name = players[0] if players else extract_name_from_text_fixed(tracker.latest_message.get("text", ""))
        if not raw_name:
            raw_name = tracker.get_slot("player_name")

        if not raw_name:
            dispatcher.utter_message(response="utter_ask_player_name")
            return []

        conn: Optional[sqlite3.Connection] = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()

            player_id, canonical_name = validate_and_find_player(raw_name, cur)
            if not player_id:
                similar = find_similar_names(raw_name, "players", "player_name", limit=3)
                if similar:
                    buttons = [
                        {"title": s, "payload": make_intent_payload("player_info", {"player": s})}
                        for s in similar
                    ]
                    dispatcher.utter_message(
                        text=f"Giocatore '{raw_name}' non trovato. Forse cercavi:",
                        buttons=buttons,
                    )
                else:
                    dispatcher.utter_message(text=f"Giocatore '{raw_name}' non trovato.")
                    dispatcher.utter_message(response="utter_ask_player_name")
                return []

            cur.execute(
                """
                SELECT id, player_name, atpname, birthdate, weight, height,
                       turned_pro, birthplace, coaches, hand, backhand, ioc, active
                FROM players
                WHERE id = ?
                """,
                (player_id,),
            )
            row = cur.fetchone()

            lines: List[str] = []
            if row:
                (
                    _pid,
                    name,
                    _atpname,
                    birthdate,
                    weight,
                    height,
                    turned_pro,
                    birthplace,
                    coaches,
                    hand,
                    backhand,
                    ioc,
                    active,
                ) = row
                display_name = str(name or canonical_name or raw_name or "").strip()
                if display_name:
                    lines.append(to_unicode_bold(display_name))
                if birthplace:
                    lines.append(f"Nato: {birthplace}")
                if birthdate:
                    lines.append(f"Data di nascita: {format_tournament_date(str(birthdate))}")
                if height:
                    lines.append(f"Altezza: {height} cm")
                if weight:
                    lines.append(f"Peso: {weight} kg")
                if ioc:
                    lines.append(f"Nazionalita: {ioc_to_flag(ioc)}")
                if coaches:
                    coach_text = str(coaches).strip()
                    if coach_text:
                        lines.append(f"Allenatore: {coach_text}")
                tp_year = normalize_year_field(turned_pro)
                if tp_year:
                    lines.append(f"Professionista dal: {tp_year}")
                if hand:
                    hand_str = str(hand).strip().upper()
                    if hand_str == "R":
                        lines.append("Mano: destra")
                    elif hand_str == "L":
                        lines.append("Mano: sinistra")
                    else:
                        lines.append(f"Mano: {hand}")
                if backhand:
                    back = str(backhand).strip().upper()
                    if back == "2H":
                        lines.append("Rovescio: due mani")
                    elif back == "1H":
                        lines.append("Rovescio: una mano")
                if active is not None:
                    lines.append(f"Stato: {'Attivo' if int(active) == 1 else 'Non attivo'}")

            cur.execute(
                """
                SELECT *
                FROM matches
                WHERE winner_id = ? OR loser_id = ?
                ORDER BY tourney_date DESC, match_id DESC
                LIMIT 1
                """,
                (player_id, player_id),
            )
            last_match = cur.fetchone()
            if last_match:
                info = get_match_display_info(last_match, cur)
                result = "W" if str(info.get("winner_id")) == str(player_id) else "L"
                opponent = info["loser"] if result == "W" else info["winner"]
                lines.append("")
                lines.append("Ultima partita:")
                lines.append(f"{info['tournament']} ({info['year']}) - {result} - {info.get('round') or 'N/A'}")
                lines.append(f"vs {opponent} - {info.get('score') or 'N/A'}")

            if lines:
                lines.append("")
                lines.append("Se vuoi possiamo consultare le sue statistiche o fare un confronto con un altro giocatore ATP!")

            dispatcher.utter_message(text="\n".join(lines))
            return [
                SlotSet("player_name", canonical_name),
                SlotSet("player1", canonical_name),
                SlotSet("player2", None),
                SlotSet("year", None),
                SlotSet("surface", None),
                SlotSet("tournament_name", None),
            ]
        except Exception as exc:
            dispatcher.utter_message(text=f"Errore nel recuperare info giocatore: {exc}")
            return []
        finally:
            if conn:
                conn.close()


class ActionPlayerStats(Action):
    """Restituisce le statistiche del giocatore con eventuali filtri applicati.

    Action che calcola statistiche di un giocatore. Supporta filtri (anno,
    superficie, torneo) estratti dal contesto conversazionale.
    """

    def name(self) -> Text:
        return "action_player_stats"

    def run(
        self,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any],
    ) -> List[Dict[Text, Any]]:
        # Contesto comune che ci permette di capire se l'utente sta filtrando anno/superficie/torneo
        ctx = build_filter_context(tracker)
        entities = ctx.entities
        players = [e.get("value") for e in entities if e.get("entity") == "player"]
        raw_name = players[0] if players else tracker.get_slot("player_name")

        if (
            not players
            and ctx.is_inform
            and action_ran_after_latest_user(tracker, "action_player_stats")
        ):
            return [FollowupAction("action_listen")]

        if not raw_name:
            dispatcher.utter_message(response="utter_ask_player_name")
            events: List[Any] = ctx.active_slot_events()
            events.append(FollowupAction("action_listen"))
            return events

        year_filter = ctx.year
        surface_filter = ctx.surface
        tournament_filter = ctx.tournament
        has_year = ctx.explicit_year
        has_surface = ctx.explicit_surface
        has_tournament = ctx.explicit_tournament

        conn: Optional[sqlite3.Connection] = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()

            player_id, canonical_name = validate_and_find_player(raw_name, cur)
            if not player_id:
                similar = find_similar_names(raw_name, "players", "player_name", limit=3)
                if similar:
                    buttons = [
                        {"title": s, "payload": make_intent_payload("player_stats", {"player": s})}
                        for s in similar
                    ]
                    dispatcher.utter_message(
                        text=f"Giocatore '{raw_name}' non trovato. Forse cercavi:",
                        buttons=buttons,
                    )
                else:
                    dispatcher.utter_message(text=f"Giocatore '{raw_name}' non trovato.")
                    dispatcher.utter_message(response="utter_ask_player_name")
                events_nf: List[Any] = ctx.slot_events()
                if not events_nf and (ctx.year or ctx.surface or ctx.tournament):
                    events_nf = ctx.active_slot_events()
                events_nf.append(FollowupAction("action_listen"))
                return events_nf

            matches_query = (
                "SELECT match_id, tourney_name, surface, tourney_date, winner_id, loser_id, score, round, minutes, "
                "w_ace, w_df, w_svpt, w_1stIn, w_1stWon, w_2ndWon, w_SvGms, w_bpSaved, w_bpFaced, "
                "l_ace, l_df, l_svpt, l_1stIn, l_1stWon, l_2ndWon, l_SvGms, l_bpSaved, l_bpFaced "
                "FROM matches WHERE (winner_id = ? OR loser_id = ?)"
            )
            params: List[Any] = [player_id, player_id]
            if year_filter:
                matches_query += " AND tourney_date LIKE ?"
                params.append(f"{year_filter}%")
            if surface_filter:
                matches_query += " AND surface = ?"
                params.append(surface_filter)
            if tournament_filter:
                matches_query += " AND LOWER(tourney_name) LIKE LOWER(?)"
                params.append(f"%{tournament_filter}%")
            matches_query += " ORDER BY tourney_date DESC, match_id DESC"

            match_rows = deduplicate_matches(
                fetch_rows_as_dicts(cur, matches_query, params, include_raw=False)
            )

            total = len(match_rows)
            filters_desc_full = ctx.describe()
            if total == 0:
                message = f"Nessun dato disponibile per {canonical_name}."
                if filters_desc_full:
                    message += "\nFiltri usati: " + ", ".join(filters_desc_full)
                dispatcher.utter_message(text=message)
                reset_events: List[Any] = [SlotSet("player_name", canonical_name)]
                if has_year or year_filter or ctx.slot_year:
                    reset_events.append(SlotSet("year", None))
                if has_surface or surface_filter or ctx.slot_surface:
                    reset_events.append(SlotSet("surface", None))
                if has_tournament or tournament_filter or ctx.slot_tournament:
                    reset_events.append(SlotSet("tournament_name", None))
                reset_events.append(SlotSet(LAST_CONTEXT_SLOT, None))
                reset_events.append(FollowupAction("action_listen"))
                return reset_events

            player_id_str = str(player_id)
            wins = sum(1 for row in match_rows if str(row["winner_id"]) == player_id_str)
            losses = total - wins
            win_rate = (wins / total * 100.0) if total else 0.0

            title = f"Statistiche {canonical_name}"
            subtitle_parts: List[str] = []
            if year_filter:
                subtitle_parts.append(str(year_filter))
            if surface_filter:
                subtitle_parts.append(SURFACE_LABELS.get(surface_filter, surface_filter))
            if tournament_filter:
                subtitle_parts.append(str(tournament_filter))
            if subtitle_parts:
                title += " (" + ", ".join(subtitle_parts) + ")"

            sum_aces = sum_dfs = sum_svpt = 0
            sum_1st_in = sum_1st_won = sum_2nd_won = 0
            sum_svgms = sum_bp_saved = sum_bp_faced = 0
            minutes_total = 0
            minutes_count = 0
            retires_total = retires_wins = retires_losses = 0
            finals_played = titles_won = tb_matches = 0
            surface_stats: Dict[str, Dict[str, int]] = {}
            tournament_stats: Dict[str, Dict[str, int]] = {}
            year_stats: Dict[str, Dict[str, int]] = {}

            for match in match_rows:
                is_winner = str(match["winner_id"]) == player_id_str

                surface_code = (match.get("surface") or "").strip()
                if not surface_code:
                    surface_code = "N/A"
                s_entry = surface_stats.setdefault(surface_code, {"matches": 0, "wins": 0})
                s_entry["matches"] += 1
                if is_winner:
                    s_entry["wins"] += 1

                tournament_name = (match.get("tourney_name") or "Sconosciuto").strip()
                t_entry = tournament_stats.setdefault(tournament_name, {"matches": 0, "wins": 0})
                t_entry["matches"] += 1
                if is_winner:
                    t_entry["wins"] += 1

                if not year_filter:
                    raw_date = str(match.get("tourney_date") or "")
                    year = raw_date[:4] if len(raw_date) >= 4 else ""
                    if year.isdigit():
                        y_entry = year_stats.setdefault(year, {"matches": 0, "wins": 0})
                        y_entry["matches"] += 1
                        if is_winner:
                            y_entry["wins"] += 1

                ace = safe_int(match["w_ace"] if is_winner else match["l_ace"])
                df = safe_int(match["w_df"] if is_winner else match["l_df"])
                svpt = safe_int(match["w_svpt"] if is_winner else match["l_svpt"])
                first_in = safe_int(match["w_1stIn"] if is_winner else match["l_1stIn"])
                first_won = safe_int(match["w_1stWon"] if is_winner else match["l_1stWon"])
                second_won = safe_int(match["w_2ndWon"] if is_winner else match["l_2ndWon"])
                svgms = safe_int(match["w_SvGms"] if is_winner else match["l_SvGms"])
                bp_saved = safe_int(match["w_bpSaved"] if is_winner else match["l_bpSaved"])
                bp_faced = safe_int(match["w_bpFaced"] if is_winner else match["l_bpFaced"])

                sum_aces += ace
                sum_dfs += df
                sum_svpt += svpt
                sum_1st_in += first_in
                sum_1st_won += first_won
                sum_2nd_won += second_won
                sum_svgms += svgms
                sum_bp_saved += bp_saved
                sum_bp_faced += bp_faced

                minutes = safe_int(match.get("minutes"))
                if minutes:
                    minutes_total += minutes
                    minutes_count += 1

                score = str(match.get("score") or "")
                if "RET" in score.upper() or "W/O" in score.upper():
                    retires_total += 1
                    if is_winner:
                        retires_wins += 1
                    else:
                        retires_losses += 1
                if "TB" in score.upper() or "7-" in score:
                    tb_matches += 1
                if str(match.get("round") or "").upper() in {"F", "FIN", "FINAL"}:
                    finals_played += 1
                    if is_winner:
                        titles_won += 1

            first_in_pct = (sum_1st_in / sum_svpt * 100.0) if sum_svpt else 0.0
            first_won_pct = (sum_1st_won / sum_1st_in * 100.0) if sum_1st_in else 0.0
            second_won_pct = (sum_2nd_won / (sum_svpt - sum_1st_in) * 100.0) if (sum_svpt - sum_1st_in) else 0.0
            bp_saved_pct = (sum_bp_saved / sum_bp_faced * 100.0) if sum_bp_faced else 0.0

            lines: List[str] = [
                to_unicode_bold(title),
                "",
                to_unicode_bold("Partite analizzate") + f": {total}",
                to_unicode_bold("Vittorie") + f": {wins}",
                to_unicode_bold("Sconfitte") + f": {losses}",
                to_unicode_bold("Win rate") + f": {win_rate:.1f}%",
                "",
                to_unicode_bold("Ace totali") + f": {sum_aces} (media {sum_aces / total:.2f}/match)",
                to_unicode_bold("Doppi falli totali") + f": {sum_dfs} (media {sum_dfs / total:.2f}/match)",
                to_unicode_bold("Prime in") + f": {first_in_pct:.1f}%",
                to_unicode_bold("Punti vinti con la 1a") + f": {first_won_pct:.1f}%",
                to_unicode_bold("Punti vinti con la 2a") + f": {second_won_pct:.1f}%",
            ]
            if sum_bp_faced > 0:
                lines.append(
                    to_unicode_bold("Break point salvati")
                    + f": {sum_bp_saved}/{sum_bp_faced} ({bp_saved_pct:.1f}%)"
                )
            lines.append(to_unicode_bold("Service game totali") + f": {sum_svgms}")
            lines.append(to_unicode_bold("Partite con tie-break") + f": {tb_matches}")
            lines.append(to_unicode_bold("Finali giocate") + f": {finals_played}")
            lines.append(to_unicode_bold("Titoli") + f": {titles_won}")
            if retires_total:
                lines.append(
                    to_unicode_bold("Match con ritiro/walkover")
                    + f": {retires_total} ("
                    + to_unicode_bold("vinti")
                    + f": {retires_wins}, "
                    + to_unicode_bold("persi")
                    + f": {retires_losses})"
                )

            sorted_surface = sorted(
                surface_stats.items(), key=lambda item: item[1]["matches"], reverse=True
            )
            if sorted_surface:
                lines.append("")
                lines.append(to_unicode_bold("Per superficie:"))
                for surface_code, data in sorted_surface:
                    matches_surface = data["matches"]
                    wins_surface = data["wins"]
                    losses_surface = matches_surface - wins_surface
                    rate_surface = (wins_surface / matches_surface * 100.0) if matches_surface else 0.0
                    label = SURFACE_LABELS.get(surface_code, surface_code)
                    lines.append(
                        f"- {label}: {wins_surface}W-{losses_surface}L ({rate_surface:.1f}%)"
                    )

            sorted_tournaments = sorted(
                tournament_stats.items(),
                key=lambda item: (item[1]["wins"], item[1]["matches"]),
                reverse=True,
            )[:5]
            if sorted_tournaments:
                lines.append("")
                lines.append(to_unicode_bold("Migliori tornei:"))
                for t_name, data in sorted_tournaments:
                    matches_t = data["matches"]
                    wins_t = data["wins"]
                    losses_t = matches_t - wins_t
                    rate_t = (wins_t / matches_t * 100.0) if matches_t else 0.0
                    lines.append(f"- {t_name}: {wins_t}W-{losses_t}L ({rate_t:.1f}%)")

            if not year_filter and year_stats:
                lines.append("")
                lines.append(to_unicode_bold("Performance per anno:"))
                for year, data in sorted(year_stats.items(), key=lambda item: item[0], reverse=True)[:5]:
                    matches_y = data["matches"]
                    wins_y = data["wins"]
                    losses_y = matches_y - wins_y
                    rate_y = (wins_y / matches_y * 100.0) if matches_y else 0.0
                    lines.append(f"- {year}: {wins_y}W-{losses_y}L ({rate_y:.1f}%)")

            if not (has_year or has_surface or has_tournament):
                lines.append("")
                lines.append("Consiglio: puoi filtrare per anno, superficie o torneo.")
                lines.append("Esempi: 'nel 2024', 'su erba', 'a Wimbledon', 'su erba nel 2022'.")

            if filters_desc_full:
                lines.append("")
                lines.append("Filtri attivi: " + ", ".join(filters_desc_full))

            dispatcher.utter_message(text="\n".join(lines))

            events: List[Any] = [SlotSet("player_name", canonical_name)]
            events.extend(ctx.slot_events(clear_unset=True))
            events.append(SlotSet("player1", None))
            events.append(SlotSet("player2", None))
            events.append(SlotSet(LAST_CONTEXT_SLOT, self.name()))
            events.append(FollowupAction("action_listen"))
            return events
        except Exception as exc:
            dispatcher.utter_message(text=f"Errore nel calcolare le statistiche: {exc}")
            return [FollowupAction("action_listen")]
        finally:
            if conn:
                conn.close()


class ActionHeadToHead(Action):
    """Confronto head-to-head fra due giocatori, con filtri opzionali."""

    def name(self) -> Text:
        return "action_head_to_head"

    def run(
        self,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any],
    ) -> List[Dict[Text, Any]]:
        # Recupero giocatori e filtri contestuali per gestire anche messaggi di follow-up
        ctx = build_filter_context(tracker)
        entities = ctx.entities
        players = [e.get("value") for e in entities if e.get("entity") == "player"]

        # Per sicurezza usiamo sia gli slot del form sia le eventuali entita nell'ultimo messaggio
        p1_name = tracker.get_slot("player1")
        p2_name = tracker.get_slot("player2")
        context_player = tracker.get_slot("player_name")
        if not p1_name and context_player:
            p1_name = context_player

        if len(players) >= 2:
            p1_name, p2_name = players[0], players[1]
        elif len(players) == 1:
            candidate = players[0]
            if not p1_name:
                p1_name = candidate
            elif candidate.strip().lower() != str(p1_name).strip().lower():
                p2_name = candidate
            elif not p2_name:
                p2_name = candidate

        if p1_name and p2_name and str(p1_name).strip().lower() == str(p2_name).strip().lower():
            p2_name = None

        if not p1_name or not p2_name:
            if not p1_name and context_player:
                p1_name = context_player
            if p1_name and not p2_name:
                dispatcher.utter_message(response="utter_ask_player2")
                return [SlotSet("player1", p1_name), SlotSet("player2", None), FollowupAction("action_listen")]
            if p2_name and not p1_name:
                dispatcher.utter_message(response="utter_ask_player1")
                return [SlotSet("player2", p2_name), SlotSet("player1", None), FollowupAction("action_listen")]
            dispatcher.utter_message(text="Devi indicare due giocatori distinti.")
            return [FollowupAction("action_listen")]

        if p1_name.strip().lower() == p2_name.strip().lower():
            dispatcher.utter_message(text="Per l'head-to-head servono due giocatori distinti.")
            return [FollowupAction("action_listen")]

        year_filter = ctx.year
        surface_filter = ctx.surface
        tournament_filter = ctx.tournament
        has_year = ctx.explicit_year
        has_surface = ctx.explicit_surface
        has_tournament = ctx.explicit_tournament

        conn: Optional[sqlite3.Connection] = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()

            p1_id, p1_canonical = validate_and_find_player(p1_name, cur)
            p2_id, p2_canonical = validate_and_find_player(p2_name, cur)
            if not p1_id or not p2_id:
                response = "Non ho trovato i giocatori richiesti.\n"
                if not p1_id:
                    sim1 = find_similar_names(p1_name, "players", "player_name", limit=3)
                    if sim1:
                        response += f"Possibili alternative per '{p1_name}': {', '.join(sim1)}.\n"
                if not p2_id:
                    sim2 = find_similar_names(p2_name, "players", "player_name", limit=3)
                    if sim2:
                        response += f"Possibili alternative per '{p2_name}': {', '.join(sim2)}."
                dispatcher.utter_message(text=response.strip())
                return [FollowupAction("action_listen")]

            matches_query = (
                "SELECT * FROM matches "
                "WHERE ((winner_id = ? AND loser_id = ?) OR (winner_id = ? AND loser_id = ?))"
            )
            params: List[Any] = [p1_id, p2_id, p2_id, p1_id]
            if year_filter:
                matches_query += " AND tourney_date LIKE ?"
                params.append(f"{year_filter}%")
            if surface_filter:
                matches_query += " AND surface = ?"
                params.append(surface_filter)
            if tournament_filter:
                matches_query += " AND LOWER(tourney_name) LIKE LOWER(?)"
                params.append(f"%{tournament_filter}%")
            matches_query += " ORDER BY tourney_date DESC, match_id DESC"

            cur.execute(matches_query, params)
            raw_rows = cur.fetchall()

            seen: set = set()
            match_rows: List[Tuple[Any, ...]] = []
            for row in raw_rows:
                key = make_match_signature(row)
                if key in seen:
                    continue
                seen.add(key)
                match_rows.append(row)

            filters_desc = ctx.describe()
            total_matches = len(match_rows)
            p1_id_str = str(p1_id)
            p2_id_str = str(p2_id)

            def build_events(clear_unset: bool = True) -> List[Any]:
                events: List[Any] = [SlotSet("player1", p1_canonical), SlotSet("player2", p2_canonical)]
                events.extend(ctx.slot_events(clear_unset=clear_unset))
                events.append(SlotSet(LAST_CONTEXT_SLOT, self.name()))
                events.append(FollowupAction("action_listen"))
                return events

            if total_matches == 0:
                msg = f"Nessuna partita trovata tra {p1_canonical} e {p2_canonical}."
                if filters_desc:
                    msg += "\nFiltri usati: " + ", ".join(filters_desc)
                dispatcher.utter_message(text=msg)
                reset_events: List[Any] = [SlotSet("player1", p1_canonical), SlotSet("player2", p2_canonical)]
                if has_year or year_filter or ctx.slot_year:
                    reset_events.append(SlotSet("year", None))
                if has_surface or surface_filter or ctx.slot_surface:
                    reset_events.append(SlotSet("surface", None))
                if has_tournament or tournament_filter or ctx.slot_tournament:
                    reset_events.append(SlotSet("tournament_name", None))
                reset_events.append(SlotSet(LAST_CONTEXT_SLOT, None))
                reset_events.append(FollowupAction("action_listen"))
                return reset_events

            p1_wins = sum(1 for row in match_rows if str(row[7]) == p1_id_str)
            p2_wins = total_matches - p1_wins

            p1_rate = (p1_wins / total_matches * 100.0) if total_matches else 0.0
            p2_rate = (p2_wins / total_matches * 100.0) if total_matches else 0.0

            lines: List[str] = [
                to_unicode_bold(f"Head-to-Head: {p1_canonical} vs {p2_canonical}"),
                "",
                to_unicode_bold("Partite totali") + f": {total_matches}",
                to_unicode_bold(f"{p1_canonical}") + f": {p1_wins} vittorie ({p1_rate:.1f}%)",
                to_unicode_bold(f"{p2_canonical}") + f": {p2_wins} vittorie ({p2_rate:.1f}%)",
            ]

            surface_stats: Dict[str, Dict[str, int]] = {}
            year_stats: Dict[str, Dict[str, int]] = {}
            for row in match_rows:
                surface_code = (row[2] or "").strip() or "N/A"
                surf_entry = surface_stats.setdefault(surface_code, {"matches": 0, "wins": 0})
                surf_entry["matches"] += 1
                if str(row[7]) == p1_id_str:
                    surf_entry["wins"] += 1

                if not year_filter:
                    date_str = str(row[5] or "")
                    year = date_str[:4] if len(date_str) >= 4 else ""
                    if year.isdigit():
                        year_entry = year_stats.setdefault(year, {"matches": 0, "wins": 0})
                        year_entry["matches"] += 1
                        if str(row[7]) == p1_id_str:
                            year_entry["wins"] += 1

            if surface_stats:
                lines.append("")
                lines.append(to_unicode_bold("Per superficie:"))
                for surface_code, data in sorted(surface_stats.items(), key=lambda item: item[1]["matches"], reverse=True):
                    matches = data["matches"]
                    p1_surf_wins = data["wins"]
                    p2_surf_wins = matches - p1_surf_wins
                    p1_pct = (p1_surf_wins / matches * 100.0) if matches else 0.0
                    p2_pct = (p2_surf_wins / matches * 100.0) if matches else 0.0
                    label = SURFACE_LABELS.get(surface_code, surface_code or "N/A")
                    lines.append(f"- {label}: {p1_canonical} {p1_surf_wins}W ({p1_pct:.1f}%) / {p2_canonical} {p2_surf_wins}W ({p2_pct:.1f}%)")

            if not year_filter and year_stats:
                lines.append("")
                lines.append(to_unicode_bold("Per anno (ultimi 5):"))
                for year, data in sorted(year_stats.items(), key=lambda item: item[0], reverse=True)[:5]:
                    matches = data["matches"]
                    p1_year_wins = data["wins"]
                    p2_year_wins = matches - p1_year_wins
                    lines.append(f"- {year}: {p1_canonical} {p1_year_wins}W / {p2_canonical} {p2_year_wins}W")

            if match_rows:
                lines.append("")
                lines.append(to_unicode_bold("Ultimi incontri:"))
                for row in match_rows[:5]:
                    info = get_match_display_info(row, cur)
                    lines.append(
                        f"- {info['tournament']} ({info['year']}) - {info['winner']} bt {info['loser']} {info['score'] or 'N/A'}"
                    )

            if filters_desc:
                lines.append("")
                lines.append("Filtri attivi: " + ", ".join(filters_desc))

            dispatcher.utter_message(text="\n".join(lines))

            return build_events(clear_unset=True)
        except Exception as exc:
            dispatcher.utter_message(text=f"Errore nel calcolare l'H2H: {exc}")
            return [FollowupAction("action_listen")]
        finally:
            if conn:
                conn.close()


class ActionTournamentInfo(Action):
    """Mostra informazioni generali sul torneo e partite recenti."""

    def name(self) -> Text:
        return "action_tournament_info"

    def run(
        self,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any],
    ) -> List[Dict[Text, Any]]:
        # Normalizzo i filtri dell'ultimo messaggio cosi' ogni modalita' usa la stessa logica
        ctx = build_filter_context(tracker)
        tournament_name = ctx.message_tournament or tracker.get_slot("tournament_name")
        if not tournament_name:
            dispatcher.utter_message(response="utter_ask_tournament_name")
            return []

        conn: Optional[sqlite3.Connection] = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()

            base_query = (
                "SELECT tourney_name, surface, COUNT(*) AS match_count, "
                "MIN(tourney_date) AS first_date, MAX(tourney_date) AS last_date "
                "FROM matches WHERE LOWER(tourney_name) LIKE LOWER(?) "
                "GROUP BY tourney_name, surface "
                "ORDER BY match_count DESC "
                "LIMIT 1"
            )
            cur.execute(base_query, (f"%{tournament_name}%",))
            row = cur.fetchone()

            if not row:
                suggestions = find_similar_names(tournament_name, "matches", "tourney_name", limit=5)
                if suggestions:
                    buttons = [
                        {
                            "title": suggestion,
                            "payload": make_intent_payload("tournament_info", {"tournament": suggestion}),
                        }
                        for suggestion in suggestions
                    ]
                    dispatcher.utter_message(
                        text=f"Torneo '{tournament_name}' non trovato. Forse cercavi:",
                        buttons=buttons,
                    )
                else:
                    message = f"Torneo '{tournament_name}' non trovato."
                    dispatcher.utter_message(text=message)
                return []
            tourney_name, surface, match_count, first_date, last_date = row
            surface_label = SURFACE_LABELS.get(surface, surface or "N/A")
            lines: List[str] = [
                to_unicode_bold(f"Informazioni torneo: {tourney_name}"),
                "",
                f"Superficie principale: {surface_label}",
                f"Numero partite registrate: {match_count}",
            ]
            if first_date:
                lines.append(f"Prima edizione presente nel database: {format_tournament_date(first_date)}")
            if last_date:
                lines.append(f"Ultima edizione presente nel database: {format_tournament_date(last_date)}")
            champion_row: Optional[Tuple[Any, ...]] = None
            if last_date:
                cur.execute(
                    """
                    SELECT *
                    FROM matches
                    WHERE LOWER(tourney_name) = LOWER(?)
                      AND tourney_date = ?
                      AND UPPER(round) IN ('F', 'FIN', 'FINAL')
                    ORDER BY match_id DESC
                    LIMIT 1
                    """,
                    (tourney_name, last_date),
                )
                champion_row = cur.fetchone()
            if not champion_row:
                cur.execute(
                    """
                    SELECT *
                    FROM matches
                    WHERE LOWER(tourney_name) = LOWER(?)
                      AND UPPER(round) IN ('F', 'FIN', 'FINAL')
                    ORDER BY tourney_date DESC, match_id DESC
                    LIMIT 1
                    """,
                    (tourney_name,),
                )
                champion_row = cur.fetchone()
            if champion_row:
                champ_info = get_match_display_info(champion_row, cur)
                lines.append("")
                lines.append(
                    to_unicode_bold("Ultimo campione")
                    + f": {champ_info['winner']} ({champ_info['year']})"
                )
                lines.append(
                    f"Finale: {champ_info['winner']} bt {champ_info['loser']} {champ_info['score'] or 'N/A'}"
                )
            recent_query = (
                "SELECT * FROM matches "
                "WHERE LOWER(tourney_name) LIKE LOWER(?) "
                "ORDER BY tourney_date DESC, match_id DESC LIMIT 5"
            )
            cur.execute(recent_query, (f"%{tourney_name}%",))
            recent_matches = cur.fetchall()
            if recent_matches:
                lines.append("")
                lines.append(to_unicode_bold("Ultime partite registrate:"))
                for match_row in recent_matches:
                    info = get_match_display_info(match_row, cur)
                    lines.append(f"- {info['tournament']} ({info['year']}) - {info['winner']} bt {info['loser']} {info['score'] or 'N/A'}")
            dispatcher.utter_message(text="\n".join(lines))
            events: List[Dict[Text, Any]] = [SlotSet("tournament_name", tourney_name)]
            events.extend(ctx.slot_events(clear_unset=False))
            return events
        except Exception as exc:
            dispatcher.utter_message(text=f"Errore nel recuperare info torneo: {exc}")
            return []
        finally:
            if conn:
                conn.close()


class ActionMatchResult(Action):
    """Gestisce tutte le richieste sui risultati dei match (1 player, 2 player, torneo, anno)."""

    # Liste di parole da ignorare quando si estraggono i nomi dai messaggi liberi.
    _PREFIX_FILLERS = {
        "il",
        "lo",
        "la",
        "l",
        "l'",
        "i",
        "gli",
        "le",
        "un",
        "una",
        "uno",
        "dei",
        "degli",
        "delle",
        "del",
        "dello",
        "della",
        "di",
        "d",
        "risultato",
        "risultati",
        "punteggio",
        "match",
        "partita",
        "partite",
        "ultimi",
        "ultime",
        "ultimo",
        "ultima",
    }
    # Parole di chiusura da scartare quando compattiamo i frammenti.
    _SUFFIX_FILLERS = {"match", "partita", "partite", "vs", "contro"}
    # Token che interrompono la cattura del nome quando incontrati.
    _BREAK_TOKENS = {
        "a",
        "ad",
        "al",
        "allo",
        "alla",
        "agli",
        "alle",
        "ai",
        "nel",
        "nello",
        "nella",
        "nei",
        "nelle",
        "su",
        "sul",
        "sullo",
        "sulla",
        "sui",
        "sugli",
        "per",
        "tra",
        "fra",
        "contro",
        "vs",
        "vs.",
        "versus",
        "durante",
        "da",
        "dal",
        "dallo",
        "dalla",
        "dai",
        "dagli",
        "dalle",
    }

    def name(self) -> Text:
        return "action_match_result"

    @classmethod
    def _tokenize(cls, text: str) -> List[str]:
        """Riduce il testo ad una lista di token utili al riconoscimento dei nomi."""
        cleaned = re.sub(r"[^A-Za-zÀ-ÖØ-öø-ÿ' -]", " ", str(text))
        tokens = [tok for tok in cleaned.split() if tok]
        result: List[str] = []
        for tok in tokens:
            lower = tok.lower()
            if lower in {"vs", "vs.", "versus", "contro"}:
                continue
            if tok.isdigit():
                continue
            if not result and lower in cls._PREFIX_FILLERS:
                continue
            result.append(tok)
        return result

    @classmethod
    def _clean_player_fragment(cls, fragment: str, from_end: bool) -> str:
        """Ripulisce un frammento di frase cercando il nome più probabile."""
        tokens = cls._tokenize(fragment)
        if not tokens:
            return ""

        while tokens and tokens[0].lower().strip("’'") in cls._PREFIX_FILLERS:
            tokens.pop(0)
        while tokens and tokens[-1].lower().strip("’'") in cls._SUFFIX_FILLERS:
            tokens.pop()
        if not tokens:
            return ""

        if from_end:
            collected: List[str] = []
            for tok in reversed(tokens):
                lower = tok.lower()
                if lower in cls._BREAK_TOKENS or tok.isdigit():
                    break
                collected.append(tok)
                if len(collected) >= 3:
                    break
            if not collected:
                collected = [tokens[-1]]
            return " ".join(reversed(collected))

        collected = []
        for tok in tokens:
            lower = tok.lower()
            if lower in cls._BREAK_TOKENS or tok.isdigit():
                break
            collected.append(tok)
            if len(collected) >= 3:
                break
        if not collected:
            collected = [tokens[0]]
        return " ".join(collected)

    @classmethod
    def _candidate_aliases(cls, raw: str) -> List[str]:
        """Genera diverse varianti del nome per massimizzare il match nel DB."""
        aliases: List[str] = []
        seen: set = set()

        def add(alias: Optional[str]) -> None:
            if not alias:
                return
            candidate = alias.strip()
            if len(candidate) < 2:
                return
            key = candidate.lower()
            if key in seen:
                return
            seen.add(key)
            aliases.append(candidate)

        add(str(raw or "").strip())
        add(cls._clean_player_fragment(raw, from_end=True))
        add(cls._clean_player_fragment(raw, from_end=False))

        tokens = cls._tokenize(raw)
        if tokens:
            if len(tokens) >= 2:
                add(" ".join(tokens[-2:]))
                add(" ".join(tokens[:2]))
            add(tokens[-1])
            add(tokens[0])
        return aliases

    def run(
        self,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any],
    ) -> List[Dict[Text, Any]]:
        ctx = build_filter_context(tracker)
        entities = ctx.entities

        player_entities = [str(e.get("value")).strip() for e in entities if e.get("entity") == "player" and e.get("value")]

        year_filter = ctx.year or tracker.get_slot("year")
        surface_filter = ctx.surface or tracker.get_slot("surface")
        if surface_filter and surface_filter not in VALID_SURFACE_CODES:
            surface_filter = None

        slot_player1 = tracker.get_slot("player1")
        slot_player2 = tracker.get_slot("player2")
        slot_player_name = tracker.get_slot("player_name")
        slot_tournament = tracker.get_slot("tournament_name")

        players: List[str] = []
        players_lower: set = set()

        def add_player(value: Optional[str]) -> None:
            if not value:
                return
            candidate = str(value).strip()
            if not candidate:
                return
            key = candidate.lower()
            if key in players_lower:
                return
            players_lower.add(key)
            players.append(candidate)

        for player in player_entities:
            add_player(player)
        add_player(slot_player1)
        add_player(slot_player2)
        add_player(slot_player_name)
        if len(players) < 2:
            extra_players = self._extract_players_from_text(ctx.text)
            for name in extra_players:
                add_player(name)
                if len(players) >= 2:
                    break

        tournament_filter = ctx.message_tournament
        if not tournament_filter and len(players) < 2:
            tournament_filter = slot_tournament
        if tournament_filter and tournament_filter.lower() in players_lower:
            tournament_filter = None

        conn: Optional[sqlite3.Connection] = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()

            if len(players) >= 2:
                return self._handle_pair(
                    dispatcher,
                    cur,
                    players,
                    ctx,
                    year_filter,
                    surface_filter,
                    tournament_filter,
                )
            if len(players) == 1:
                return self._handle_single(
                    dispatcher,
                    cur,
                    players[0],
                    ctx,
                    year_filter,
                    surface_filter,
                    tournament_filter,
                )
            if tournament_filter:
                return self._handle_tournament(dispatcher, cur, ctx, tournament_filter, year_filter, surface_filter)
            return self._handle_latest(dispatcher, cur, ctx, year_filter, surface_filter)
        except Exception as exc:
            dispatcher.utter_message(text=f"Errore nel recuperare i risultati dei match: {exc}")
            return []
        finally:
            if conn:
                conn.close()

    def _handle_pair(
        self,
        dispatcher: CollectingDispatcher,
        cursor: sqlite3.Cursor,
        player_candidates: List[str],
        ctx: FilterContext,
        year_filter: Optional[str],
        surface_filter: Optional[str],
        tournament_filter: Optional[str],
    ) -> List[Dict[Text, Any]]:
        resolved_players: List[Tuple[str, str]] = []
        local_tournament = tournament_filter
        seen_aliases: set = set()

        for candidate in player_candidates:
            if len(resolved_players) >= 2:
                break
            if not candidate:
                continue
            candidate_found = False
            for alias in self._candidate_aliases(candidate):
                alias_key = alias.lower()
                if alias_key in seen_aliases:
                    continue
                seen_aliases.add(alias_key)
                player_id, canonical = validate_and_find_player(alias, cursor)
                if player_id and len(resolved_players) < 2:
                    resolved_players.append((player_id, canonical or alias))
                    candidate_found = True
                    break
            if candidate_found:
                continue

            if not local_tournament:
                tournament_match = find_best_tournament(cursor, candidate)
                if tournament_match:
                    local_tournament = tournament_match

        # Se il torneo non e' ancora chiaro provo a ricostruirlo dal testo libero
        if not local_tournament:
            local_tournament = find_best_tournament(cursor, ctx.message_tournament) or guess_tournament_from_text(
                cursor, ctx.text, [name for _, name in resolved_players]
            )

        # Ultima chance: testo libero tipo "Rinderknech vs Zverev a Shanghai 2025"
        if len(resolved_players) < 2:
            extra_names = self._extract_players_from_text(ctx.text)
            for name in extra_names:
                if len(resolved_players) >= 2:
                    break
                for alias in self._candidate_aliases(name):
                    alias_key = alias.lower()
                    if alias_key in seen_aliases:
                        continue
                    seen_aliases.add(alias_key)
                    player_id, canonical = validate_and_find_player(alias, cursor)
                    if player_id:
                        resolved_players.append((player_id, canonical or alias))
                        break

        if len(resolved_players) < 2:
            dispatcher.utter_message(text="Devi indicare due giocatori per ottenere il risultato del match.")
            return []

        (p1_id, p1_canonical), (p2_id, p2_canonical) = resolved_players
        tournament_filter = local_tournament

        query = (
            "SELECT * FROM matches "
            "WHERE ((winner_id = ? AND loser_id = ?) OR (winner_id = ? AND loser_id = ?))"
        )
        params: List[Any] = [p1_id, p2_id, p2_id, p1_id]
        if year_filter:
            query += " AND tourney_date LIKE ?"
            params.append(f"{year_filter}%")
        if surface_filter:
            query += " AND surface = ?"
            params.append(surface_filter)
        if tournament_filter:
            query += " AND LOWER(tourney_name) LIKE LOWER(?)"
            params.append(f"%{tournament_filter}%")
        query += " ORDER BY tourney_date DESC, match_id DESC LIMIT 10"

        match_rows = fetch_unique_match_dicts(cursor, query, params)  # ordina gia' per data decrescente
        filters_desc = describe_filters(year_filter, surface_filter, tournament_filter)

        if not match_rows:
            message = f"Nessuna partita trovata tra {p1_canonical} e {p2_canonical}."
            if filters_desc:
                message += "\nFiltri usati: " + ", ".join(filters_desc)
            dispatcher.utter_message(text=message)
            events = [
                SlotSet("player_name", None),
                SlotSet("player1", p1_canonical),
                SlotSet("player2", p2_canonical),
                SlotSet("year", year_filter if year_filter else None),
                SlotSet("surface", surface_filter if surface_filter else None),
                SlotSet("tournament_name", tournament_filter if tournament_filter else None),
                SlotSet(LAST_CONTEXT_SLOT, None),
            ]
            events.append(FollowupAction("action_listen"))
            return events

        selected = match_rows[0]  # prendiamo il match piu' recente rispetto ai filtri
        detail_lines = format_match_details(selected, cursor)

        if len(match_rows) > 1:
            detail_lines.append("")
            detail_lines.append(to_unicode_bold("Altri match trovati:"))
            for extra in match_rows[1:4]:
                extra_raw = extra.get("__raw__")
                extra_info = get_match_display_info(extra_raw, cursor) if extra_raw else {}
                detail_lines.append(
                    f"- {extra_info.get('tournament', 'N/A')} ({extra_info.get('year', 'N/A')}) - "
                    f"{extra_info.get('winner', 'N/A')} bt {extra_info.get('loser', 'N/A')} "
                    f"{extra_info.get('score') or 'N/A'} ({extra_info.get('round') or 'N/A'})"
                )
        detail_lines.append("")
        detail_lines.append(f"Match disponibili nel database: {len(match_rows)}")
        if filters_desc:
            detail_lines.append("Filtri attivi: " + ", ".join(filters_desc))

        dispatcher.utter_message(text="\n".join(detail_lines))
        events: List[Any] = [
            SlotSet("player_name", None),
            SlotSet("player1", p1_canonical),
            SlotSet("player2", p2_canonical),
            SlotSet("year", year_filter if year_filter else None),
            SlotSet("surface", surface_filter if surface_filter else None),
            SlotSet("tournament_name", tournament_filter if tournament_filter else None),
            SlotSet(LAST_CONTEXT_SLOT, self.name()),
        ]
        events.append(FollowupAction("action_listen"))
        return events

    def _extract_players_from_text(self, text: Optional[str]) -> List[str]:
        """Tenta di ricavare due nomi dal pattern 'X vs Y' quando le entità non arrivano."""
        if not text:
            return []
        match = re.search(r"(?i)(.+?)\s+(?:vs\.?|versus|contro)\s+(.+)", text)
        if not match:
            return []
        left = self._clean_player_fragment(match.group(1), from_end=True)
        right = self._clean_player_fragment(match.group(2), from_end=False)
        results: List[str] = []
        for candidate in (left, right):
            cleaned = candidate.strip(" ,.-")
            if cleaned and all(cleaned.lower() != existing.lower() for existing in results):
                results.append(cleaned)
        return results

    def _handle_single(
        self,
        dispatcher: CollectingDispatcher,
        cursor: sqlite3.Cursor,
        player_name: str,
        ctx: FilterContext,
        year_filter: Optional[str],
        surface_filter: Optional[str],
        tournament_filter: Optional[str],
    ) -> List[Dict[Text, Any]]:
        player_id, canonical_name = validate_and_find_player(player_name, cursor)
        if not player_id:
            suggestions = find_similar_names(player_name, "players", "player_name", limit=3)
            if suggestions:
                dispatcher.utter_message(
                    text=f"Giocatore '{player_name}' non trovato. Forse cercavi: {', '.join(suggestions)}."
                )
            else:
                dispatcher.utter_message(text=f"Giocatore '{player_name}' non trovato.")
            return []

        # Se l'utente compara un giocatore ma cita il torneo solo a parole lo intercetto qui
        tournament_filter = tournament_filter or find_best_tournament(cursor, ctx.message_tournament) or guess_tournament_from_text(
            cursor, ctx.text, [canonical_name]
        )

        query = "SELECT * FROM matches WHERE winner_id = ? OR loser_id = ?"
        params: List[Any] = [player_id, player_id]
        if year_filter:
            query += " AND tourney_date LIKE ?"
            params.append(f"{year_filter}%")
        if surface_filter:
            query += " AND surface = ?"
            params.append(surface_filter)
        if tournament_filter:
            query += " AND LOWER(tourney_name) LIKE LOWER(?)"
            params.append(f"%{tournament_filter}%")
        query += " ORDER BY tourney_date DESC, match_id DESC LIMIT 5"

        cursor.execute(query, params)
        rows = cursor.fetchall()

        filters_desc = describe_filters(year_filter, surface_filter, tournament_filter)
        if not rows:
            message = f"Nessuna partita trovata per {canonical_name}."
            if filters_desc:
                message += "\nFiltri usati: " + ", ".join(filters_desc)
            dispatcher.utter_message(text=message)
            events = [
                SlotSet("player_name", canonical_name),
                SlotSet("player1", canonical_name),
                SlotSet("player2", None),
                SlotSet("year", year_filter if year_filter else None),
                SlotSet("surface", surface_filter if surface_filter else None),
                SlotSet("tournament_name", tournament_filter if tournament_filter else None),
                SlotSet(LAST_CONTEXT_SLOT, None),
            ]
            events.append(FollowupAction("action_listen"))
            return events

        lines: List[str] = [to_unicode_bold(f"Ultimi match di {canonical_name}")]
        if filters_desc:
            lines.append("Filtri attivi: " + ", ".join(filters_desc))
        lines.append("")
        for row in rows:
            info = get_match_display_info(row, cursor)
            result = "W" if str(info.get("winner_id")) == str(player_id) else "L"
            opponent = info.get("loser") if result == "W" else info.get("winner")
            lines.append(
                f"- {info.get('tournament', 'N/A')} ({info.get('year', 'N/A')}) - "
                f"{result} vs {opponent or 'N/A'} - {info.get('score') or 'N/A'} "
                f"({info.get('round') or 'N/A'})"
            )

        dispatcher.utter_message(text="\n".join(lines))
        events = [
            SlotSet("player_name", canonical_name),
            SlotSet("player1", canonical_name),
            SlotSet("player2", None),
            SlotSet("year", year_filter if year_filter else None),
            SlotSet("surface", surface_filter if surface_filter else None),
            SlotSet("tournament_name", tournament_filter if tournament_filter else None),
            SlotSet(LAST_CONTEXT_SLOT, self.name()),
        ]
        events.append(FollowupAction("action_listen"))
        return events

    def _handle_tournament(
        self,
        dispatcher: CollectingDispatcher,
        cursor: sqlite3.Cursor,
        ctx: FilterContext,
        tournament_name: str,
        year_filter: Optional[str],
        surface_filter: Optional[str],
    ) -> List[Dict[Text, Any]]:
        exclude_players = [
            str(e.get("value")).strip()
            for e in ctx.entities
            if e.get("entity") == "player" and e.get("value")
        ]
        lookup_name = find_best_tournament(cursor, tournament_name)
        if not lookup_name:
            lookup_name = guess_tournament_from_text(cursor, ctx.text, exclude_players)
        search_name = lookup_name or tournament_name

        cursor.execute(
            """
            SELECT tourney_name
            FROM matches
            WHERE LOWER(tourney_name) LIKE LOWER(?)
            GROUP BY tourney_name
            ORDER BY COUNT(*) DESC
            LIMIT 1
            """,
            (f"%{search_name}%",),
        )
        row = cursor.fetchone()
        if not row:
            message = f"Torneo '{tournament_name}' non trovato."
            suggestions = find_similar_names(tournament_name, "matches", "tourney_name", limit=5)
            if suggestions:
                message += "\nForse cercavi: " + ", ".join(suggestions)
            dispatcher.utter_message(text=message)
            return []

        canonical_tourney = row[0]

        query = "SELECT * FROM matches WHERE tourney_name = ?"
        params: List[Any] = [canonical_tourney]
        if year_filter:
            query += " AND tourney_date LIKE ?"
            params.append(f"{year_filter}%")
        if surface_filter:
            query += " AND surface = ?"
            params.append(surface_filter)
        query += " ORDER BY tourney_date DESC, match_id DESC LIMIT 5"

        cursor.execute(query, params)
        rows = cursor.fetchall()

        filters_desc = describe_filters(year_filter, surface_filter, canonical_tourney)
        if not rows:
            message = f"Nessuna partita trovata per {canonical_tourney}."
            if filters_desc:
                message += "\nFiltri usati: " + ", ".join(filters_desc)
            dispatcher.utter_message(text=message)
            events = [
                SlotSet("player_name", None),
                SlotSet("player1", None),
                SlotSet("player2", None),
                SlotSet("tournament_name", canonical_tourney),
                SlotSet("year", year_filter if year_filter else None),
                SlotSet("surface", surface_filter if surface_filter else None),
                SlotSet(LAST_CONTEXT_SLOT, None),
            ]
            events.append(FollowupAction("action_listen"))
            return events

        lines: List[str] = [to_unicode_bold(f"Ultimi match di {canonical_tourney}")]
        if filters_desc:
            lines.append("Filtri attivi: " + ", ".join(filters_desc))
        lines.append("")
        for record in rows:
            info = get_match_display_info(record, cursor)
            lines.append(
                f"- {info.get('year', 'N/A')} - {info.get('winner', 'N/A')} bt "
                f"{info.get('loser', 'N/A')} {info.get('score') or 'N/A'} ({info.get('round') or 'N/A'})"
            )

        dispatcher.utter_message(text="\n".join(lines))
        events = [
            SlotSet("player_name", None),
            SlotSet("player1", None),
            SlotSet("player2", None),
            SlotSet("tournament_name", canonical_tourney),
            SlotSet("year", year_filter if year_filter else None),
            SlotSet("surface", surface_filter if surface_filter else None),
            SlotSet(LAST_CONTEXT_SLOT, self.name()),
        ]
        events.append(FollowupAction("action_listen"))
        return events

    def _handle_latest(
        self,
        dispatcher: CollectingDispatcher,
        cursor: sqlite3.Cursor,
        ctx: FilterContext,
        year_filter: Optional[str],
        surface_filter: Optional[str],
    ) -> List[Dict[Text, Any]]:
        query = "SELECT * FROM matches"
        conditions: List[str] = []
        params: List[Any] = []
        if year_filter:
            conditions.append("tourney_date LIKE ?")
            params.append(f"{year_filter}%")
        if surface_filter:
            conditions.append("surface = ?")
            params.append(surface_filter)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY tourney_date DESC, match_id DESC LIMIT 5"

        cursor.execute(query, params)
        rows = cursor.fetchall()

        filters_desc = describe_filters(year_filter, surface_filter, None)
        if not rows:
            message = "Non ho trovato match nel database con i filtri richiesti."
            dispatcher.utter_message(text=message)
            events = [
                SlotSet("player_name", None),
                SlotSet("player1", None),
                SlotSet("player2", None),
                SlotSet("tournament_name", None),
                SlotSet("year", year_filter if year_filter else None),
                SlotSet("surface", surface_filter if surface_filter else None),
                SlotSet(LAST_CONTEXT_SLOT, None),
            ]
            events.append(FollowupAction("action_listen"))
            return events

        lines: List[str] = [to_unicode_bold("Ultimi match registrati:") ]
        if filters_desc:
            lines.append("Filtri attivi: " + ", ".join(filters_desc))
        lines.append("")
        for row in rows:
            info = get_match_display_info(row, cursor)
            lines.append(
                f"- {info.get('tournament', 'N/A')} ({info.get('year', 'N/A')}) - "
                f"{info.get('winner', 'N/A')} bt {info.get('loser', 'N/A')} "
                f"{info.get('score') or 'N/A'} ({info.get('round') or 'N/A'})"
            )

        dispatcher.utter_message(text="\n".join(lines))
        events = [
            SlotSet("player_name", None),
            SlotSet("player1", None),
            SlotSet("player2", None),
            SlotSet("tournament_name", None),
            SlotSet("year", year_filter if year_filter else None),
            SlotSet("surface", surface_filter if surface_filter else None),
            SlotSet(LAST_CONTEXT_SLOT, self.name()),
        ]
        events.append(FollowupAction("action_listen"))
        return events


class ActionOngoingTournaments(Action):
    """Elenca gli incontri contrassegnati come in corso nel database."""

    def name(self) -> Text:
        return "action_ongoing_tournaments"

    def run(
        self,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any],
    ) -> List[Dict[Text, Any]]:
        conn: Optional[sqlite3.Connection] = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT *
                FROM matches
                WHERE ongoing = 1
                ORDER BY tourney_date DESC, match_id DESC
                LIMIT 200
                """
            )
            rows = cur.fetchall()

            if not rows:
                dispatcher.utter_message(text="Al momento non risultano match in corso nel database.")
                return [
                    SlotSet("player_name", None),
                    SlotSet("player1", None),
                    SlotSet("player2", None),
                    SlotSet("tournament_name", None),
                    SlotSet("year", None),
                    SlotSet("surface", None),
                    SlotSet(LAST_CONTEXT_SLOT, None),
                    FollowupAction("action_listen"),
                ]

            lines: List[str] = [to_unicode_bold("Tornei in corso:")]
            per_tournament: Dict[str, List[Tuple[Any, ...]]] = {}
            for raw in rows:
                tourney = raw[1] or "Torneo sconosciuto"
                per_tournament.setdefault(tourney, []).append(raw)

            for tourney_name, matches in per_tournament.items():
                lines.append("")
                lines.append(to_unicode_bold(f"{tourney_name}") + ", ultime 2 partite:")
                subset = matches[:2]
                for raw in subset:
                    info = get_match_display_info(raw, cur)
                    round_name = info.get("round") or "Round N/A"
                    score = info.get("score") or "Aggiornamento non disponibile"
                    lines.append(
                        f"    - {info.get('year', 'N/A')} {round_name} - "
                        f"{info.get('winner', 'N/A')} bt {info.get('loser', 'N/A')} {score}"
                    )

            dispatcher.utter_message(text="\n".join(lines))
            return [
                SlotSet("player_name", None),
                SlotSet("player1", None),
                SlotSet("player2", None),
                SlotSet("tournament_name", None),
                SlotSet("year", None),
                SlotSet("surface", None),
                SlotSet(LAST_CONTEXT_SLOT, None),
                FollowupAction("action_listen"),
            ]
        except Exception as exc:
            dispatcher.utter_message(text=f"Errore nel recuperare le partite in corso: {exc}")
            return []
        finally:
            if conn:
                conn.close()


class ActionApplyFilters(Action):
    """Instrada i messaggi di filtro successivi."""

    def name(self) -> Text:
        return "action_apply_filters"

    @staticmethod
    def _get_last_relevant_action(tracker: Tracker) -> Optional[str]:
        """Restituisce l'ultima action eseguita (escludendo listen/router)."""
        events = getattr(tracker, "events", []) or []
        ignored = {
            "action_listen",
            "action_apply_filters",
            "action_default_fallback",
            "action_reset_slots",
            "action_session_start",
            "action_extract_slots",
        }
        for event in reversed(events):
            if getattr(event, "event", None) != "action":
                continue
            name = getattr(event, "name", "") or ""
            if name in ignored or name.startswith("utter_"):
                continue
            return name
        return None

    def run(
        self,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any],
    ) -> List[Dict[Text, Any]]:
        # Seleziona eventuali filtri espressi nel messaggio corrente.
        ctx = build_filter_context(tracker)
        year = ctx.message_year
        surface = ctx.message_surface
        tournament = ctx.message_tournament

        last_action = self._get_last_relevant_action(tracker)
        if not last_action:
            last_action = tracker.get_slot(LAST_CONTEXT_SLOT)
        allowed_actions = {
            "action_player_stats",
            "action_head_to_head",
            "action_match_result",
        }

        if last_action not in allowed_actions:
            # Nessuna action compatibile trovata: meglio guidare l'utente a ripetere la richiesta.
            dispatcher.utter_message(
                text="Qui non posso applicare filtri. Prova a formulare una nuova richiesta completa."
            )
            return [FollowupAction("action_listen")]

        slot_updates: List[Any] = []
        if year:
            slot_updates.append(SlotSet("year", year))
        if surface:
            slot_updates.append(SlotSet("surface", surface))
        if tournament:
            slot_updates.append(SlotSet("tournament_name", tournament))

        slot_updates.append(SlotSet(LAST_CONTEXT_SLOT, last_action))
        slot_updates.append(FollowupAction(last_action))
        return slot_updates


class ActionResetSlots(Action):
    """Pulisce gli slot della conversazione al comando /start."""

    def name(self) -> Text:
        return "action_reset_slots"

    def run(
        self,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any],
    ) -> List[Dict[Text, Any]]:
        slots = ["player_name", "player1", "player2", "tournament_name", "year", "surface", LAST_CONTEXT_SLOT]
        return [SlotSet(slot, None) for slot in slots]


class ActionDefaultFallback(Action):
    """Messaggio di fallback per input non riconosciuto."""

    def name(self) -> Text:
        return "action_default_fallback"

    def run(
        self,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any],
    ) -> List[Dict[Text, Any]]:
        dispatcher.utter_message(
            text="Non ho capito. Posso aiutarti con: giocatori, tornei, statistiche, scontri diretti e risultati."
        )
        return []

























