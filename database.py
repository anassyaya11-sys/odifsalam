# -*- coding: utf-8 -*-
"""
ODIFSALAM — Couche d'accès données PostgreSQL/Supabase
Remplace sqlite3 par psycopg2 avec pool de connexions.
Compatible avec toutes les requêtes de app.py (SQLite → PostgreSQL).
"""

import os
import re
import pandas as pd
import streamlit as st
import psycopg2
import psycopg2.extras
from psycopg2 import pool as pg_pool

# ── CONNEXION SUPABASE ──────────────────────────────────────────────────────
def _get_db_url() -> str:
    """Récupère l'URL de connexion depuis st.secrets ou variables d'environnement."""
    try:
        return st.secrets["DATABASE_URL"]
    except Exception:
        url = os.environ.get("DATABASE_URL", "")
        if url:
            return url
        # Fallback construction depuis les composantes
        host = os.environ.get("DB_HOST", "aws-0-eu-west-1.pooler.supabase.com")
        user = os.environ.get("DB_USER", "postgres.dimjiazzuqqqhgfzsmxe")
        pwd  = os.environ.get("DB_PASSWORD", "")
        db   = os.environ.get("DB_NAME", "postgres")
        port = os.environ.get("DB_PORT", "6543")
        return f"postgresql://{user}:{pwd}@{host}:{port}/{db}?sslmode=require"

@st.cache_resource
def _get_pool():
    """Crée un pool de connexions PostgreSQL (singleton via st.cache_resource)."""
    # On passe les paramètres explicitement pour éviter que psycopg2
    # tronque le username (postgres.PROJECT_REF) dans l'URL
    return pg_pool.ThreadedConnectionPool(
        minconn=1, maxconn=10,
        host="aws-0-eu-west-1.pooler.supabase.com",
        port=6543,
        dbname="postgres",
        user="postgres.dimjiazzuqqqhgfzsmxe",
        password="gUpmS3uGgNEfymaQ",
        sslmode="require"
    )

def get_conn():
    """Retourne une connexion depuis le pool."""
    return _get_pool().getconn()

def release_conn(conn):
    """Remet la connexion dans le pool."""
    try:
        _get_pool().putconn(conn)
    except Exception:
        pass

# ── ADAPTATEUR SQL SQLite → PostgreSQL ─────────────────────────────────────
def _adapt_sql(sql: str) -> str:
    """
    Convertit les particularités SQLite en syntaxe PostgreSQL :
    - ? → $1, $2, …
    - INTEGER PRIMARY KEY AUTOINCREMENT → SERIAL PRIMARY KEY (géré dans init_db)
    - PRAGMA → ignoré (pas de retour)
    """
    # Remplacer les ? par $1, $2, …
    count = [0]
    def replacer(m):
        count[0] += 1
        return f"${count[0]}"
    return re.sub(r'\?', replacer, sql)

# ── FONCTIONS PRINCIPALES ───────────────────────────────────────────────────
def qdf(sql: str, p=None) -> pd.DataFrame:
    """Exécute une SELECT et retourne un DataFrame."""
    conn = get_conn()
    try:
        adapted = _adapt_sql(sql)
        df = pd.read_sql_query(adapted, conn, params=p or [])
        return df
    except Exception as e:
        print(f"[qdf] Erreur : {e}\nSQL : {sql}")
        return pd.DataFrame()
    finally:
        release_conn(conn)

def exsql(sql: str, p=None) -> int:
    """Exécute INSERT/UPDATE/DELETE. Retourne lastrowid pour les INSERT."""
    conn = get_conn()
    try:
        adapted = _adapt_sql(sql)
        cur = conn.cursor()
        # Pour les INSERT, récupérer l'ID généré
        if sql.strip().upper().startswith("INSERT"):
            adapted = adapted + " RETURNING id" if "RETURNING" not in adapted.upper() else adapted
            cur.execute(adapted, p or [])
            row = cur.fetchone()
            lid = row[0] if row else None
        else:
            cur.execute(adapted, p or [])
            lid = None
        conn.commit()
        return lid
    except Exception as e:
        conn.rollback()
        print(f"[exsql] Erreur : {e}\nSQL : {sql}")
        raise
    finally:
        release_conn(conn)

def exmany(sql: str, rows):
    """Exécute une insertion en masse."""
    conn = get_conn()
    try:
        adapted = _adapt_sql(sql)
        cur = conn.cursor()
        cur.executemany(adapted, rows)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[exmany] Erreur : {e}")
        raise
    finally:
        release_conn(conn)

# ── INIT BASE DE DONNÉES ────────────────────────────────────────────────────
def init_db():
    """
    Crée toutes les tables si elles n'existent pas.
    Utilise la syntaxe PostgreSQL (SERIAL, TEXT, etc.).
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        # ── Tables principales ──────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dossiers (
                id SERIAL PRIMARY KEY,
                nom TEXT NOT NULL UNIQUE,
                description TEXT DEFAULT '',
                client TEXT DEFAULT '',
                date_creation TEXT DEFAULT '',
                statut TEXT DEFAULT 'En cours',
                observation TEXT DEFAULT ''
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS rues (
                id SERIAL PRIMARY KEY,
                dossier_id INTEGER REFERENCES dossiers(id) ON DELETE SET NULL,
                nom TEXT NOT NULL,
                zone TEXT DEFAULT '',
                longueur_m REAL DEFAULT 0,
                largeur_m REAL DEFAULT 0,
                observation TEXT DEFAULT '',
                numero_marche TEXT DEFAULT '',
                objet_marche TEXT DEFAULT '',
                maitre_ouvrage TEXT DEFAULT '',
                maitre_ouvrage_delegue TEXT DEFAULT '',
                entreprise TEXT DEFAULT '',
                bureau_controle TEXT DEFAULT '',
                labo TEXT DEFAULT '',
                coordinateur_securite TEXT DEFAULT '',
                date_notification TEXT DEFAULT '',
                date_demarrage TEXT DEFAULT '',
                delai_jours REAL DEFAULT 0,
                delai_mois REAL DEFAULT 0,
                statut_chantier TEXT DEFAULT 'En cours'
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS livrables (
                id SERIAL PRIMARY KEY,
                chantier_id INTEGER NOT NULL REFERENCES rues(id) ON DELETE CASCADE,
                nom TEXT NOT NULL,
                type_livrable TEXT DEFAULT 'Rue',
                description TEXT DEFAULT '',
                longueur_m REAL DEFAULT 0,
                largeur_m REAL DEFAULT 0,
                observation TEXT DEFAULT ''
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS devis_rue (
                id SERIAL PRIMARY KEY,
                rue_id INTEGER NOT NULL REFERENCES rues(id) ON DELETE CASCADE,
                livrable_id INTEGER REFERENCES livrables(id) ON DELETE SET NULL,
                code_poste TEXT DEFAULT '',
                designation TEXT NOT NULL,
                unite TEXT NOT NULL,
                quantite_marche REAL DEFAULT 0,
                prix_unitaire REAL DEFAULT 0,
                observation TEXT DEFAULT ''
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS realisations_journalieres (
                id SERIAL PRIMARY KEY,
                date_suivi TEXT NOT NULL,
                rue_id INTEGER NOT NULL REFERENCES rues(id),
                devis_id INTEGER NOT NULL REFERENCES devis_rue(id),
                quantite_jour REAL DEFAULT 0,
                observation TEXT DEFAULT ''
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS sous_traitants (
                id SERIAL PRIMARY KEY,
                nom TEXT NOT NULL UNIQUE,
                specialite TEXT DEFAULT '',
                responsable TEXT DEFAULT '',
                telephone TEXT DEFAULT '',
                email TEXT DEFAULT '',
                montant_contrat REAL DEFAULT 0,
                date_debut TEXT DEFAULT '',
                date_fin TEXT DEFAULT '',
                statut TEXT DEFAULT 'Actif',
                rue_id INTEGER REFERENCES rues(id),
                observation TEXT DEFAULT ''
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS devis_st (
                id SERIAL PRIMARY KEY,
                st_id INTEGER NOT NULL REFERENCES sous_traitants(id) ON DELETE CASCADE,
                rue_id INTEGER REFERENCES rues(id),
                code_poste TEXT DEFAULT '',
                designation TEXT NOT NULL,
                unite TEXT NOT NULL,
                quantite REAL DEFAULT 0,
                prix_unitaire REAL DEFAULT 0,
                observation TEXT DEFAULT ''
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS decomptes_st (
                id SERIAL PRIMARY KEY,
                st_id INTEGER NOT NULL REFERENCES sous_traitants(id),
                rue_id INTEGER REFERENCES rues(id),
                numero_decompte INTEGER DEFAULT 1,
                date_decompte TEXT NOT NULL,
                devis_st_id INTEGER REFERENCES devis_st(id),
                quantite_executee REAL DEFAULT 0,
                montant REAL DEFAULT 0,
                observation TEXT DEFAULT '',
                valide INTEGER DEFAULT 0
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS paiements_st (
                id SERIAL PRIMARY KEY,
                st_id INTEGER NOT NULL REFERENCES sous_traitants(id),
                rue_id INTEGER REFERENCES rues(id),
                date_paiement TEXT NOT NULL,
                montant REAL NOT NULL,
                reference TEXT DEFAULT '',
                mode_paiement TEXT DEFAULT 'Virement',
                observation TEXT DEFAULT ''
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS personnel (
                id SERIAL PRIMARY KEY,
                nom TEXT NOT NULL,
                prenom TEXT DEFAULT '',
                categorie TEXT NOT NULL,
                poste TEXT DEFAULT '',
                salaire_journalier REAL DEFAULT 0,
                telephone TEXT DEFAULT '',
                date_entree TEXT DEFAULT '',
                actif INTEGER DEFAULT 1,
                rue_id INTEGER REFERENCES rues(id),
                observation TEXT DEFAULT ''
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS pointage (
                id SERIAL PRIMARY KEY,
                date_pointage TEXT NOT NULL,
                personnel_id INTEGER NOT NULL REFERENCES personnel(id),
                rue_id INTEGER REFERENCES rues(id),
                statut TEXT NOT NULL DEFAULT 'Présent',
                heures_travaillees REAL DEFAULT 8,
                heures_normales REAL DEFAULT 8,
                heures_sup REAL DEFAULT 0,
                tache TEXT DEFAULT '',
                observation TEXT DEFAULT ''
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS materiaux (
                id SERIAL PRIMARY KEY,
                nom TEXT NOT NULL UNIQUE,
                unite TEXT NOT NULL,
                categorie TEXT DEFAULT 'Matériau',
                stock_initial REAL DEFAULT 0,
                seuil_alerte REAL DEFAULT 0,
                prix_unitaire REAL DEFAULT 0
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS approvisionnements (
                id SERIAL PRIMARY KEY,
                date_besoin TEXT NOT NULL,
                rue_id INTEGER REFERENCES rues(id),
                materiau_id INTEGER REFERENCES materiaux(id),
                designation TEXT NOT NULL,
                unite TEXT NOT NULL,
                quantite_demandee REAL DEFAULT 0,
                prix_unitaire_estime REAL DEFAULT 0,
                demandeur TEXT DEFAULT '',
                motif TEXT DEFAULT '',
                statut TEXT DEFAULT 'Besoin exprimé',
                date_validation_cc TEXT DEFAULT '',
                validateur_cc TEXT DEFAULT '',
                numero_bc TEXT DEFAULT '',
                date_bc TEXT DEFAULT '',
                fournisseur TEXT DEFAULT '',
                date_reception TEXT DEFAULT '',
                quantite_recue REAL DEFAULT 0,
                bon_livraison TEXT DEFAULT '',
                date_mise_stock TEXT DEFAULT '',
                quantite_mise_stock REAL DEFAULT 0,
                prix_unitaire_reel REAL DEFAULT 0,
                observation TEXT DEFAULT ''
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS mouvements_materiaux (
                id SERIAL PRIMARY KEY,
                date_mvt TEXT NOT NULL,
                rue_id INTEGER REFERENCES rues(id),
                materiau_id INTEGER NOT NULL REFERENCES materiaux(id),
                type_mvt TEXT NOT NULL,
                quantite REAL NOT NULL,
                prix_unitaire REAL DEFAULT 0,
                fournisseur TEXT DEFAULT '',
                bon_livraison TEXT DEFAULT '',
                appro_id INTEGER,
                observation TEXT DEFAULT ''
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS materiels (
                id SERIAL PRIMARY KEY,
                nom TEXT NOT NULL UNIQUE,
                type_materiel TEXT DEFAULT '',
                immatriculation TEXT DEFAULT '',
                marque TEXT DEFAULT '',
                annee INTEGER DEFAULT 0,
                cout_horaire REAL DEFAULT 0,
                cout_journalier REAL DEFAULT 0,
                statut TEXT DEFAULT 'Disponible',
                etat TEXT DEFAULT 'Opérationnel',
                rue_id_affectation INTEGER REFERENCES rues(id),
                date_acquisition TEXT DEFAULT '',
                date_derniere_maintenance TEXT DEFAULT '',
                prochain_entretien_heures REAL DEFAULT 0,
                heures_totales REAL DEFAULT 0,
                heure_compteur REAL DEFAULT 0,
                observation TEXT DEFAULT '',
                observations TEXT DEFAULT ''
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS suivi_materiels (
                id SERIAL PRIMARY KEY,
                date_suivi TEXT NOT NULL,
                rue_id INTEGER REFERENCES rues(id),
                materiel_id INTEGER NOT NULL REFERENCES materiels(id),
                heures_marche REAL DEFAULT 0,
                heures_arret REAL DEFAULT 0,
                heures_travail REAL DEFAULT 0,
                carburant_materiau_id INTEGER REFERENCES materiaux(id),
                carburant_l REAL DEFAULT 0,
                carburant_consomme REAL DEFAULT 0,
                cout_carburant REAL DEFAULT 0,
                chauffeur TEXT DEFAULT '',
                kilometre_debut REAL DEFAULT 0,
                kilometre_fin REAL DEFAULT 0,
                panne TEXT DEFAULT '',
                observation TEXT DEFAULT '',
                observations TEXT DEFAULT ''
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS maintenance_materiels (
                id SERIAL PRIMARY KEY,
                materiel_id INTEGER NOT NULL REFERENCES materiels(id),
                date_maintenance TEXT NOT NULL,
                type_maintenance TEXT DEFAULT 'Préventive',
                description TEXT NOT NULL,
                cout REAL DEFAULT 0,
                prestataire TEXT DEFAULT '',
                pieces_changees TEXT DEFAULT '',
                heures_compteur REAL DEFAULT 0,
                prochain_entretien_h REAL DEFAULT 0,
                observation TEXT DEFAULT ''
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS journal_chantier (
                id SERIAL PRIMARY KEY,
                date_journal TEXT NOT NULL,
                rue_id INTEGER REFERENCES rues(id),
                meteo TEXT DEFAULT '',
                temperature REAL DEFAULT 20,
                nb_ouvriers_presents INTEGER DEFAULT 0,
                nb_ouvriers INTEGER DEFAULT 0,
                nb_encadrants INTEGER DEFAULT 0,
                travaux_executes TEXT DEFAULT '',
                travaux_realises TEXT DEFAULT '',
                problemes TEXT DEFAULT '',
                decisions TEXT DEFAULT '',
                visiteurs TEXT DEFAULT '',
                redacteur TEXT DEFAULT '',
                observation TEXT DEFAULT '',
                observations TEXT DEFAULT ''
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS caisse_chantier (
                id SERIAL PRIMARY KEY,
                date_op TEXT NOT NULL,
                categorie TEXT DEFAULT 'CHANTIER',
                rue_id INTEGER REFERENCES rues(id),
                type_op TEXT NOT NULL,
                rubrique TEXT NOT NULL,
                montant REAL NOT NULL,
                beneficiaire TEXT DEFAULT '',
                reference_piece TEXT DEFAULT '',
                mode_paiement TEXT DEFAULT 'Espèces',
                valide INTEGER DEFAULT 0,
                observation TEXT DEFAULT '',
                observations TEXT DEFAULT ''
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS courriers (
                id SERIAL PRIMARY KEY,
                date_courrier TEXT NOT NULL,
                rue_id INTEGER REFERENCES rues(id),
                type_courrier TEXT NOT NULL DEFAULT 'Entrant',
                sens TEXT DEFAULT '',
                reference TEXT DEFAULT '',
                objet TEXT NOT NULL,
                expediteur_destinataire TEXT DEFAULT '',
                expediteur TEXT DEFAULT '',
                destinataire TEXT DEFAULT '',
                priorite TEXT DEFAULT 'Normale',
                resume TEXT DEFAULT '',
                actions_requises TEXT DEFAULT '',
                statut TEXT DEFAULT 'En cours',
                observation TEXT DEFAULT ''
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS incidents (
                id SERIAL PRIMARY KEY,
                date_incident TEXT NOT NULL,
                rue_id INTEGER REFERENCES rues(id),
                type_incident TEXT NOT NULL,
                gravite TEXT DEFAULT 'Mineur',
                description TEXT NOT NULL,
                personne_concernee TEXT DEFAULT '',
                mesures_prises TEXT DEFAULT '',
                nb_victimes INTEGER DEFAULT 0,
                cout_estime REAL DEFAULT 0,
                actions_correctives TEXT DEFAULT '',
                statut TEXT DEFAULT 'Ouvert',
                cloture INTEGER DEFAULT 0,
                observation TEXT DEFAULT ''
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS audit_trail (
                id SERIAL PRIMARY KEY,
                date_action TEXT NOT NULL,
                table_concernee TEXT NOT NULL,
                action TEXT NOT NULL,
                enregistrement_id INTEGER,
                details TEXT DEFAULT ''
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id SERIAL PRIMARY KEY,
                timestamp TEXT NOT NULL,
                table_name TEXT NOT NULL,
                action TEXT NOT NULL,
                record_id INTEGER,
                details TEXT DEFAULT ''
            )
        """)

        conn.commit()
        print("[init_db] ✅ Toutes les tables créées/vérifiées.")
    except Exception as e:
        conn.rollback()
        print(f"[init_db] ❌ Erreur : {e}")
        raise
    finally:
        release_conn(conn)
