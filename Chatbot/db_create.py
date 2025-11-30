#!/usr/bin/env python3
"""
TennisBot Database Creator
Crea e popola database SQLite per TennisBot usando dati da TennisMyLife/TML-Database
"""

import sqlite3
import pandas as pd
import sys
from datetime import datetime, timedelta
from typing import Optional
import requests
import io
from pathlib import Path


class TennisBotDatabaseCreator:
    """Classe per creare e popolare il database TennisBot."""
    
    def __init__(self, db_path: str = "tennisbot.db"):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.base_url = "https://raw.githubusercontent.com/TennisMyLife/TML-Database/master"
        
    def connect_database(self) -> None:
        """Stabilisce la connessione al database SQLite."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.execute("PRAGMA foreign_keys = ON")
            print(f"Connessione al database '{self.db_path}' stabilita")
        except sqlite3.Error as e:
            print(f"Errore nella connessione al database: {e}")
            sys.exit(1)
    
    def create_tables(self) -> None:
        """Crea le tabelle del database se non esistono."""
        if not self.conn:
            raise RuntimeError("Connessione al database non stabilita")
        
        try:
            cursor = self.conn.cursor()
            
            # Tabella players
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS players (
                    id TEXT PRIMARY KEY,
                    player_name TEXT NOT NULL,
                    atpname TEXT,
                    birthdate TEXT,
                    weight REAL,
                    height REAL,
                    turned_pro TEXT,
                    birthplace TEXT,
                    coaches TEXT,
                    hand TEXT,
                    backhand TEXT,
                    ioc TEXT,
                    active INTEGER DEFAULT 0
                )
            """)
            
            # Tabella matches (con dati del torneo integrati)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS matches (
                    match_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tourney_name TEXT NOT NULL,
                    surface TEXT,
                    draw_size INTEGER,
                    tourney_level TEXT,
                    tourney_date TEXT,
                    match_num INTEGER,
                    winner_id TEXT NOT NULL,
                    loser_id TEXT NOT NULL,
                    winner_seed TEXT,
                    loser_seed TEXT,
                    score TEXT,
                    best_of INTEGER,
                    round TEXT,
                    minutes INTEGER,
                    w_ace INTEGER,
                    w_df INTEGER,
                    w_svpt INTEGER,
                    w_1stIn INTEGER,
                    w_1stWon INTEGER,
                    w_2ndWon INTEGER,
                    w_SvGms INTEGER,
                    w_bpSaved INTEGER,
                    w_bpFaced INTEGER,
                    l_ace INTEGER,
                    l_df INTEGER,
                    l_svpt INTEGER,
                    l_1stIn INTEGER,
                    l_1stWon INTEGER,
                    l_2ndWon INTEGER,
                    l_SvGms INTEGER,
                    l_bpSaved INTEGER,
                    l_bpFaced INTEGER,
                    ongoing INTEGER DEFAULT 0,
                    FOREIGN KEY (winner_id) REFERENCES players(id),
                    FOREIGN KEY (loser_id) REFERENCES players(id)
                )
            """)
            
            self.conn.commit()
            print("Tabelle create con successo")
            
        except sqlite3.Error as e:
            print(f"Errore nella creazione delle tabelle: {e}")
            raise
    
    def download_csv_data(self, filename: str) -> Optional[pd.DataFrame]:
        """Scarica e legge un file CSV dal repository GitHub."""
        url = f"{self.base_url}/{filename}"
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            csv_data = io.StringIO(response.text)
            df = pd.read_csv(csv_data)
            df = df.where(pd.notnull(df), None)
            
            print(f"Scaricato {filename}: {len(df)} righe")
            return df
            
        except Exception as e:
            print(f"Errore nel download di {filename}: {e}")
            return None
    
    def safe_int_convert(self, value) -> Optional[int]:
        """Converte un valore in intero gestendo NaN e valori non validi."""
        try:
            if pd.isna(value) or value is None or value == '' or str(value).strip() == '':
                return None
            return int(float(str(value)))
        except (ValueError, TypeError):
            return None
    
    def safe_float_convert(self, value) -> Optional[float]:
        """Converte un valore in float gestendo NaN e valori non validi."""
        try:
            if pd.isna(value) or value is None or value == '' or str(value).strip() == '':
                return None
            return float(str(value))
        except (ValueError, TypeError):
            return None
    
    def safe_str_convert(self, value) -> Optional[str]:
        """Converte un valore in stringa gestendo NaN e valori vuoti."""
        if pd.isna(value) or value is None or str(value).strip() == '':
            return None
        return str(value).strip()
    
    def safe_date_convert(self, value) -> Optional[str]:
        """Converte un valore di data in stringa formato YYYYMMDD gestendo NaN e valori non validi."""
        try:
            if pd.isna(value) or value is None or value == '' or str(value).strip() == '':
                return None
            # Converti prima in int per rimuovere il .0, poi in stringa
            date_int = int(float(str(value)))
            date_str = str(date_int)
            # Verifica che sia nel formato YYYYMMDD (8 cifre)
            if len(date_str) == 8 and date_str.isdigit():
                return date_str
            return None
        except (ValueError, TypeError):
            return None
    
    def load_players_data(self) -> None:
        """Carica i dati dei giocatori dalla tabella ATP_Database.csv."""
        print("\nCaricamento dati giocatori...")
        
        df_players = self.download_csv_data("ATP_Database.csv")
        if df_players is None:
            print("Impossibile caricare i dati dei giocatori")
            return
        
        try:
            cursor = self.conn.cursor()
            inserted_count = 0
            skipped_count = 0
            batch_size = 1000
            players_batch = []
            
            print(f"Elaborazione di {len(df_players)} giocatori...", end='', flush=True)
            
            for idx, row in df_players.iterrows():
                player_id = self.safe_str_convert(row['id'])
                
                if player_id is None or player_id == '':
                    skipped_count += 1
                    continue
                
                player_name = self.safe_str_convert(row['player'])
                if player_name is None:
                    player_name = f"Player_{player_id}"
                
                # Converte tutti gli altri campi gestendo NaN/None
                atpname = self.safe_str_convert(row['atpname'])
                birthdate = self.safe_date_convert(row['birthdate'])
                weight = self.safe_float_convert(row['weight'])
                height = self.safe_float_convert(row['height']) 
                turned_pro = self.safe_str_convert(row['turnedpro'])
                birthplace = self.safe_str_convert(row['birthplace'])
                coaches = self.safe_str_convert(row['coaches'])
                hand = self.safe_str_convert(row['hand'])
                backhand = self.safe_str_convert(row['backhand'])
                ioc = self.safe_str_convert(row['ioc'])
                
                players_batch.append((
                    player_id, player_name, atpname, birthdate,
                    weight, height, turned_pro, birthplace,
                    coaches, hand, backhand, ioc
                ))
                inserted_count += 1
                
                # Commit batch ogni 1000 record
                if len(players_batch) >= batch_size:
                    cursor.executemany("""
                        INSERT OR REPLACE INTO players 
                        (id, player_name, atpname, birthdate, weight, height, turned_pro, 
                         birthplace, coaches, hand, backhand, ioc, active)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                    """, players_batch)
                    self.conn.commit()
                    players_batch = []
                    print(f"\rElaborati {inserted_count}/{len(df_players)} giocatori...", end='', flush=True)
            
            # Inserisci i record rimanenti
            if players_batch:
                cursor.executemany("""
                    INSERT OR REPLACE INTO players 
                    (id, player_name, atpname, birthdate, weight, height, turned_pro, 
                     birthplace, coaches, hand, backhand, ioc, active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """, players_batch)
                self.conn.commit()
            
            print(f"\rInseriti {inserted_count} giocatori" + " " * 30)
            if skipped_count > 0:
                print(f"Saltati {skipped_count} giocatori per dati non validi")
            
        except sqlite3.Error as e:
            print(f"\nErrore nell'inserimento dei giocatori: {e}")
            raise
    
    def load_historical_data(self) -> None:
        """Carica i dati storici delle partite con informazioni del torneo integrate."""
        print("\nCaricamento dati storici partite...")
        
        years = list(range(1968, 2026))
        total_years = len(years)
        matches_count = 0
        skipped_matches = 0
        
        try:
            cursor = self.conn.cursor()
            cursor.execute("PRAGMA foreign_keys = OFF")
            
            matches_batch = []
            batch_size = 5000
            
            for year_idx, year in enumerate(years, 1):
                filename = f"{year}.csv"
                print(f"Anno {year} ({year_idx}/{total_years})...", end='', flush=True)
                
                df_year = self.download_csv_data(filename)
                
                if df_year is None:
                    print(f"\rAnno {year} ({year_idx}/{total_years}) - file non trovato" + " " * 30)
                    continue
                
                year_matches = 0
                for _, row in df_year.iterrows():
                    winner_id = self.safe_str_convert(row['winner_id'])
                    loser_id = self.safe_str_convert(row['loser_id'])
                    
                    if winner_id is None or loser_id is None:
                        skipped_matches += 1
                        continue
                    
                    matches_batch.append((
                        self.safe_str_convert(row['tourney_name']),
                        self.safe_str_convert(row['surface']),
                        self.safe_int_convert(row['draw_size']),
                        self.safe_str_convert(row['tourney_level']),
                        self.safe_date_convert(row['tourney_date']),
                        self.safe_int_convert(row['match_num']),
                        winner_id, loser_id,
                        self.safe_str_convert(row['winner_seed']),
                        self.safe_str_convert(row['loser_seed']),
                        self.safe_str_convert(row['score']),
                        self.safe_int_convert(row['best_of']),
                        self.safe_str_convert(row['round']),
                        self.safe_int_convert(row['minutes']),
                        self.safe_int_convert(row.get('w_ace')),
                        self.safe_int_convert(row.get('w_df')),
                        self.safe_int_convert(row.get('w_svpt')),
                        self.safe_int_convert(row.get('w_1stIn')),
                        self.safe_int_convert(row.get('w_1stWon')),
                        self.safe_int_convert(row.get('w_2ndWon')),
                        self.safe_int_convert(row.get('w_SvGms')),
                        self.safe_int_convert(row.get('w_bpSaved')),
                        self.safe_int_convert(row.get('w_bpFaced')),
                        self.safe_int_convert(row.get('l_ace')),
                        self.safe_int_convert(row.get('l_df')),
                        self.safe_int_convert(row.get('l_svpt')),
                        self.safe_int_convert(row.get('l_1stIn')),
                        self.safe_int_convert(row.get('l_1stWon')),
                        self.safe_int_convert(row.get('l_2ndWon')),
                        self.safe_int_convert(row.get('l_SvGms')),
                        self.safe_int_convert(row.get('l_bpSaved')),
                        self.safe_int_convert(row.get('l_bpFaced'))
                    ))
                    matches_count += 1
                    year_matches += 1
                    
                    # Commit batch ogni 5000 record
                    if len(matches_batch) >= batch_size:
                        cursor.executemany("""
                            INSERT INTO matches 
                            (tourney_name, surface, draw_size, tourney_level, tourney_date,
                             match_num, winner_id, loser_id, winner_seed, loser_seed,
                             score, best_of, round, minutes, w_ace, w_df, w_svpt, w_1stIn, w_1stWon, 
                             w_2ndWon, w_SvGms, w_bpSaved, w_bpFaced, l_ace, l_df, l_svpt, l_1stIn, 
                             l_1stWon, l_2ndWon, l_SvGms, l_bpSaved, l_bpFaced, ongoing)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                        """, matches_batch)
                        self.conn.commit()
                        matches_batch = []
                
                print(f"\rAnno {year} ({year_idx}/{total_years}): {year_matches} partite - Totale: {matches_count:,}" + " " * 20)
            
            # Inserisci i record rimanenti
            if matches_batch:
                cursor.executemany("""
                    INSERT INTO matches 
                    (tourney_name, surface, draw_size, tourney_level, tourney_date,
                     match_num, winner_id, loser_id, winner_seed, loser_seed,
                     score, best_of, round, minutes, w_ace, w_df, w_svpt, w_1stIn, w_1stWon, 
                     w_2ndWon, w_SvGms, w_bpSaved, w_bpFaced, l_ace, l_df, l_svpt, l_1stIn, 
                     l_1stWon, l_2ndWon, l_SvGms, l_bpSaved, l_bpFaced, ongoing)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """, matches_batch)
                self.conn.commit()
            
            cursor.execute("PRAGMA foreign_keys = ON")
            
            print(f"\nCaricati {matches_count:,} partite totali")
            if skipped_matches > 0:
                print(f"Saltati {skipped_matches} match per ID giocatori non validi")
            
        except sqlite3.Error as e:
            print(f"\nErrore nel caricamento dati storici: {e}")
            raise
    
    def update_ongoing_matches(self) -> None:
        """Aggiorna il flag 'ongoing' per le partite dai tornei in corso e aggiunge nuovi match se necessario."""
        print("\nAggiornamento partite in corso...")
        
        df_ongoing = self.download_csv_data("ongoing_tourneys.csv")
        if df_ongoing is None:
            print("Impossibile caricare i tornei in corso")
            return
        
        try:
            cursor = self.conn.cursor()
            
            # Reimposta a 0 solo i match attualmente segnati come ongoing
            cursor.execute("UPDATE matches SET ongoing = 0 WHERE ongoing = 1")
            
            # Conta match aggiunti e aggiornati
            matches_added = 0
            matches_updated = 0
            skipped_matches = 0
            
            for _, row in df_ongoing.iterrows():
                winner_id = self.safe_str_convert(row['winner_id'])
                loser_id = self.safe_str_convert(row['loser_id'])
                
                if winner_id is None or loser_id is None:
                    skipped_matches += 1
                    continue
                
                tourney_name = self.safe_str_convert(row['tourney_name'])
                tourney_date = self.safe_date_convert(row['tourney_date'])
                match_num = self.safe_int_convert(row['match_num'])
                round_val = self.safe_str_convert(row['round'])
                
                if tourney_name and "Davis Cup" in tourney_name.lower():
                    continue
                
                # Cerca se il match esiste già (basato su chiavi identificative)
                cursor.execute("""
                    SELECT match_id FROM matches 
                    WHERE winner_id = ? AND loser_id = ? AND tourney_name = ? 
                    AND tourney_date = ? AND (match_num = ? OR (match_num IS NULL AND ? IS NULL))
                    AND (round = ? OR (round IS NULL AND ? IS NULL))
                """, (winner_id, loser_id, tourney_name, tourney_date, 
                      match_num, match_num, round_val, round_val))
                
                existing_match = cursor.fetchone()
                
                if existing_match:
                    # Match esiste: aggiorna ongoing = 1
                    cursor.execute("""
                        UPDATE matches
                        SET ongoing = 1, score = COALESCE(?, score)
                        WHERE match_id = ?
                    """, (self.safe_str_convert(row['score']), existing_match[0]))
                    matches_updated += 1
                else:
                    # Match non esiste: inseriscilo con ongoing = 1
                    cursor.execute("""
                        INSERT INTO matches 
                        (tourney_name, surface, draw_size, tourney_level, tourney_date,
                         match_num, winner_id, loser_id, winner_seed, loser_seed,
                         score, best_of, round, minutes, w_ace, w_df, w_svpt, w_1stIn, w_1stWon, 
                         w_2ndWon, w_SvGms, w_bpSaved, w_bpFaced, l_ace, l_df, l_svpt, l_1stIn, 
                         l_1stWon, l_2ndWon, l_SvGms, l_bpSaved, l_bpFaced, ongoing)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                    """, (
                        tourney_name,
                        self.safe_str_convert(row['surface']),
                        self.safe_int_convert(row['draw_size']),
                        self.safe_str_convert(row['tourney_level']),
                        tourney_date,
                        match_num,
                        winner_id, loser_id,
                        self.safe_str_convert(row['winner_seed']),
                        self.safe_str_convert(row['loser_seed']),
                        self.safe_str_convert(row['score']),
                        self.safe_int_convert(row['best_of']),
                        round_val,
                        self.safe_int_convert(row['minutes']),
                        self.safe_int_convert(row.get('w_ace')),
                        self.safe_int_convert(row.get('w_df')),
                        self.safe_int_convert(row.get('w_svpt')),
                        self.safe_int_convert(row.get('w_1stIn')),
                        self.safe_int_convert(row.get('w_1stWon')),
                        self.safe_int_convert(row.get('w_2ndWon')),
                        self.safe_int_convert(row.get('w_SvGms')),
                        self.safe_int_convert(row.get('w_bpSaved')),
                        self.safe_int_convert(row.get('w_bpFaced')),
                        self.safe_int_convert(row.get('l_ace')),
                        self.safe_int_convert(row.get('l_df')),
                        self.safe_int_convert(row.get('l_svpt')),
                        self.safe_int_convert(row.get('l_1stIn')),
                        self.safe_int_convert(row.get('l_1stWon')),
                        self.safe_int_convert(row.get('l_2ndWon')),
                        self.safe_int_convert(row.get('l_SvGms')),
                        self.safe_int_convert(row.get('l_bpSaved')),
                        self.safe_int_convert(row.get('l_bpFaced'))
                    ))
                    matches_added += 1
            
            self.conn.commit()
            
            print(f"Processati {len(df_ongoing)} match dai tornei in corso")
            if matches_added > 0:
                print(f"Aggiunti {matches_added} nuovi match")
            if matches_updated > 0:
                print(f"Aggiornati {matches_updated} match esistenti")
            if skipped_matches > 0:
                print(f"Saltati {skipped_matches} match per dati non validi")
            
        except sqlite3.Error as e:
            print(f"Errore nell'aggiornamento partite in corso: {e}")
            raise
    
    def update_active_players(self) -> None:
        """Aggiorna il flag 'active' per i giocatori attivi negli ultimi 2 anni."""
        print("\nAggiornamento giocatori attivi...")
        
        try:
            cursor = self.conn.cursor()
            
            two_years_ago = (datetime.now() - timedelta(days=730)).strftime("%Y%m%d")
            
            cursor.execute("""
                UPDATE players 
                SET active = 1 
                WHERE id IN (
                    SELECT DISTINCT winner_id 
                    FROM matches
                    WHERE tourney_date >= ?
                    UNION
                    SELECT DISTINCT loser_id 
                    FROM matches
                    WHERE tourney_date >= ?
                )
            """, (two_years_ago, two_years_ago))
            
            active_count = cursor.rowcount
            self.conn.commit()
            
            print(f"Aggiornati {active_count} giocatori come attivi")
            
        except sqlite3.Error as e:
            print(f"Errore nell'aggiornamento giocatori attivi: {e}")
            raise
    
    def get_database_stats(self) -> None:
        """Mostra statistiche del database creato."""
        print("\nStatistiche del database:")
        
        try:
            cursor = self.conn.cursor()
            
            # Statistiche tabelle principali
            tables = ['players', 'matches']
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"{table}: {count:,} record")
            
            # Giocatori attivi
            cursor.execute("SELECT COUNT(*) FROM players WHERE active = 1")
            active_players = cursor.fetchone()[0]
            print(f"Giocatori attivi: {active_players:,}")
            
            # Partite in corso
            cursor.execute("SELECT COUNT(*) FROM matches WHERE ongoing = 1")
            ongoing_matches = cursor.fetchone()[0]
            print(f"Partite in corso: {ongoing_matches:,}")
            
            # Tornei unici
            cursor.execute("SELECT COUNT(DISTINCT tourney_name) FROM matches")
            unique_tournaments = cursor.fetchone()[0]
            print(f"Tornei unici: {unique_tournaments:,}")
            
            # Superficie più comune
            cursor.execute("""
                SELECT surface, COUNT(*) as count 
                FROM matches 
                WHERE surface IS NOT NULL 
                GROUP BY surface 
                ORDER BY count DESC 
                LIMIT 1
            """)
            surface_result = cursor.fetchone()
            if surface_result:
                print(f"Superficie più comune: {surface_result[0]} ({surface_result[1]:,} partite)")
            
        except sqlite3.Error as e:
            print(f"Errore nel calcolo delle statistiche: {e}")
    
    def close_connection(self) -> None:
        """Chiude la connessione al database."""
        if self.conn:
            self.conn.close()
            print(f"\nConnessione al database chiusa")
    
    def create_database(self) -> None:
        """Metodo principale per creare e popolare il database."""
        print("TennisBot Database Creator")
        print("=" * 50)

        db_exists = Path(self.db_path).exists()
        if db_exists:
            print("Database esistente rilevato: verranno eseguiti solo gli aggiornamenti.")
        else:
            print("Nessun database trovato: avvio creazione completa.")

        try:
            self.connect_database()

            if not db_exists:
                self.create_tables()
                self.load_players_data()
                self.load_historical_data()
            else:
                print("Schema e dati già presenti, eseguo solo l'aggiornamento.")

            self.update_ongoing_matches()
            self.update_active_players()
            self.get_database_stats()

            if db_exists:
                print("\nDatabase aggiornato con successo!")
            else:
                print("\nDatabase creato con successo!")

        except Exception as e:
            print(f"\nErrore critico: {e}")
            return False
        finally:
            self.close_connection()

        return True


def main():
    """Funzione principale."""
    creator = TennisBotDatabaseCreator()
    success = creator.create_database()
    
    if success:
        print(f"\nIl database 'tennisbot.db' è pronto per l'uso!")
        sys.exit(0)
    else:
        print(f"\nCreazione del database fallita!")
        sys.exit(1)


if __name__ == "__main__":
    main()
