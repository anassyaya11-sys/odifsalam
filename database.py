# -*- coding: utf-8 -*-
"""
ODIFSALAM — Couche d'accès données PostgreSQL/Supabase
Stratégie : connexions directes psycopg2 (pas de ThreadedConnectionPool)
             + Session Pooler Supabase port 5432 (évite le circuit breaker PgBouncer)
Python 3.12 requis (voir runtime.txt).
"""

import os
import re
import pandas as pd
import streamlit as st
import psycopg2
import psycopg2.extras

# ── CREDENTIALS ─────────────────────────────────────────────────────────────
@st.cache_resource
def _get_creds() -> dict:
    """
    Extrait les credentials une seule fois (mis en cache).
    Priorité : st.secrets > DATABASE_URL env > fallback hardcodé.
    Port 5432 = Session Pooler Supabase (pas de circuit breaker PgBouncer).
    """
    db_url = ""
    try:
        db_url = st.secrets.get("DATABASE_URL", "")
    except Exception:
        pass
    if not db_url:
        db_url = os.environ.get("DATABASE_URL", "")

    if db_url:
        m = re.match(
            r'postgres(?:ql)?(?:\+\w+)?://([^:@]+):([^@]+)@([^:/]+):?(\d+)?/([^?#]+)',
            db_url
        )
        if m:
            return {
                "user":     m.group(1),
                "password": m.group(2),
                "host":     m.group(3),
                # Forcer port 5432 (Session Pooler) même si l'URL contient 6543
                "port":     5432,
                "dbname":   m.group(5),
                "sslmode":  "require",
            }

    # Fallback — Session Pooler port 5432
    return {
        "user":     os.environ.get("DB_USER",     "postgres.dimjiazzuqqqhgfzsmxe"),
        "password": os.environ.get("DB_PASSWORD", "gUpmS3uGgNEfymaQ"),
        "host":     os.environ.get("DB_HOST",     "aws-0-eu-west-1.pooler.supabase.com"),
        "port":     5432,   # Session Pooler — PAS 6543 (Transaction Pooler/PgBouncer)
        "dbname":   os.environ.get("DB_NAME",     "postgres"),
        "sslmode":  "require",
    }

# ── CONNEXION DIRECTE (pas de pool — connexion par opération) ────────────────
def get_conn():
    """
    Ouvre une connexion psycopg2 fraîche via Session Pooler (port 5432).
    Pas de ThreadedConnectionPool : Supabase gère le pooling côté serveur.
    """
    c = _get_creds()
    try:
        return psycopg2.connect(
            host=c["host"],
            port=c["port"],
            dbname=c["dbname"],
            user=c["user"],
            password=c["password"],
            sslmode=c["sslmode"],
            connect_timeout=15,
        )
    except psycopg2.OperationalError as e:
        msg = str(e)
        if "Circuit breaker" in msg:
            st.error(
                "🔴 Supabase a temporairement bloqué les connexions (trop de tentatives échouées).\n\n"
                "⏳ **Attendez 5-10 minutes** puis rechargez la page. Le disjoncteur se réinitialise automatiquement."
            )
        else:
            st.error(f"🔴 Erreur connexion Supabase : {e}")
            st.info(f"Paramètres : host={c['host']} port={c['port']} user={c['user']} db={c['dbname']}")
        raise

def release_conn(conn):
    """Ferme la connexion proprement."""
    try:
        conn.close()
    except Exception:
        pass

# ── FONCTIONS D'ACCÈS AUX DONNÉES ───────────────────────────────────────────
def qdf(sql: str, p=None) -> pd.DataFrame:
    """Exécute une SELECT et retourne un DataFrame."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, p or ())
            rows = cur.fetchall()
        return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    except Exception as e:
        print(f"[qdf] {e}\n{sql}")
        try: conn.rollback()
        except Exception: pass
        return pd.DataFrame()
    finally:
        release_conn(conn)

def exsql(sql: str, p=None):
    """Exécute INSERT/UPDATE/DELETE. Retourne l'id si INSERT."""
    is_insert = sql.strip().upper().startswith("INSERT")
    full_sql = sql if not is_insert or "RETURNING" in sql.upper() else sql + " RETURNING id"
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(full_sql, p or ())
            conn.commit()
            if is_insert:
                row = cur.fetchone()
                return row[0] if row else None
        return None
    except Exception as e:
        print(f"[exsql] {e}\n{sql}")
        try: conn.rollback()
        except Exception: pass
        raise
    finally:
        release_conn(conn)

def exmany(sql: str, rows):
    """Exécute une requête sur plusieurs lignes."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()
    except Exception as e:
        print(f"[exmany] {e}")
        try: conn.rollback()
        except Exception: pass
        raise
    finally:
        release_conn(conn)

# ── INIT BASE DE DONNÉES ─────────────────────────────────────────────────────
def init_db():
    """Crée toutes les tables si elles n'existent pas."""
    ddl_statements = [
        "CREATE TABLE IF NOT EXISTS dossiers (id SERIAL PRIMARY KEY, nom TEXT NOT NULL UNIQUE, description TEXT DEFAULT '', client TEXT DEFAULT '', date_creation TEXT DEFAULT '', statut TEXT DEFAULT 'En cours', observation TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS rues (id SERIAL PRIMARY KEY, dossier_id INTEGER REFERENCES dossiers(id) ON DELETE SET NULL, nom TEXT NOT NULL, zone TEXT DEFAULT '', longueur_m REAL DEFAULT 0, largeur_m REAL DEFAULT 0, observation TEXT DEFAULT '', numero_marche TEXT DEFAULT '', objet_marche TEXT DEFAULT '', maitre_ouvrage TEXT DEFAULT '', maitre_ouvrage_delegue TEXT DEFAULT '', entreprise TEXT DEFAULT '', bureau_controle TEXT DEFAULT '', labo TEXT DEFAULT '', coordinateur_securite TEXT DEFAULT '', date_notification TEXT DEFAULT '', date_demarrage TEXT DEFAULT '', delai_jours REAL DEFAULT 0, delai_mois REAL DEFAULT 0, statut_chantier TEXT DEFAULT 'En cours')",
        "CREATE TABLE IF NOT EXISTS livrables (id SERIAL PRIMARY KEY, chantier_id INTEGER NOT NULL REFERENCES rues(id) ON DELETE CASCADE, nom TEXT NOT NULL, type_livrable TEXT DEFAULT 'Rue', description TEXT DEFAULT '', longueur_m REAL DEFAULT 0, largeur_m REAL DEFAULT 0, observation TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS devis_rue (id SERIAL PRIMARY KEY, rue_id INTEGER NOT NULL REFERENCES rues(id) ON DELETE CASCADE, livrable_id INTEGER REFERENCES livrables(id) ON DELETE SET NULL, code_poste TEXT DEFAULT '', designation TEXT NOT NULL, unite TEXT NOT NULL, quantite_marche REAL DEFAULT 0, prix_unitaire REAL DEFAULT 0, observation TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS realisations_journalieres (id SERIAL PRIMARY KEY, date_suivi TEXT NOT NULL, rue_id INTEGER NOT NULL REFERENCES rues(id), devis_id INTEGER NOT NULL REFERENCES devis_rue(id), quantite_jour REAL DEFAULT 0, observation TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS sous_traitants (id SERIAL PRIMARY KEY, nom TEXT NOT NULL UNIQUE, specialite TEXT DEFAULT '', responsable TEXT DEFAULT '', telephone TEXT DEFAULT '', email TEXT DEFAULT '', montant_contrat REAL DEFAULT 0, date_debut TEXT DEFAULT '', date_fin TEXT DEFAULT '', statut TEXT DEFAULT 'Actif', rue_id INTEGER REFERENCES rues(id), observation TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS devis_st (id SERIAL PRIMARY KEY, st_id INTEGER NOT NULL REFERENCES sous_traitants(id) ON DELETE CASCADE, rue_id INTEGER REFERENCES rues(id), code_poste TEXT DEFAULT '', designation TEXT NOT NULL, unite TEXT NOT NULL, quantite REAL DEFAULT 0, prix_unitaire REAL DEFAULT 0, observation TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS decomptes_st (id SERIAL PRIMARY KEY, st_id INTEGER NOT NULL REFERENCES sous_traitants(id), rue_id INTEGER REFERENCES rues(id), numero_decompte INTEGER DEFAULT 1, date_decompte TEXT NOT NULL, devis_st_id INTEGER REFERENCES devis_st(id), quantite_executee REAL DEFAULT 0, montant REAL DEFAULT 0, observation TEXT DEFAULT '', valide INTEGER DEFAULT 0)",
        "CREATE TABLE IF NOT EXISTS paiements_st (id SERIAL PRIMARY KEY, st_id INTEGER NOT NULL REFERENCES sous_traitants(id), rue_id INTEGER REFERENCES rues(id), date_paiement TEXT NOT NULL, montant REAL NOT NULL, reference TEXT DEFAULT '', mode_paiement TEXT DEFAULT 'Virement', observation TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS personnel (id SERIAL PRIMARY KEY, nom TEXT NOT NULL, prenom TEXT DEFAULT '', categorie TEXT NOT NULL, poste TEXT DEFAULT '', salaire_journalier REAL DEFAULT 0, telephone TEXT DEFAULT '', date_entree TEXT DEFAULT '', actif INTEGER DEFAULT 1, rue_id INTEGER REFERENCES rues(id), observation TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS pointage (id SERIAL PRIMARY KEY, date_pointage TEXT NOT NULL, personnel_id INTEGER NOT NULL REFERENCES personnel(id), rue_id INTEGER REFERENCES rues(id), statut TEXT NOT NULL DEFAULT 'Présent', heures_travaillees REAL DEFAULT 8, heures_normales REAL DEFAULT 8, heures_sup REAL DEFAULT 0, tache TEXT DEFAULT '', observation TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS materiaux (id SERIAL PRIMARY KEY, nom TEXT NOT NULL UNIQUE, unite TEXT NOT NULL, categorie TEXT DEFAULT 'Matériau', stock_initial REAL DEFAULT 0, seuil_alerte REAL DEFAULT 0, prix_unitaire REAL DEFAULT 0)",
        "CREATE TABLE IF NOT EXISTS approvisionnements (id SERIAL PRIMARY KEY, date_besoin TEXT NOT NULL, rue_id INTEGER REFERENCES rues(id), materiau_id INTEGER REFERENCES materiaux(id), designation TEXT NOT NULL, unite TEXT NOT NULL, quantite_demandee REAL DEFAULT 0, prix_unitaire_estime REAL DEFAULT 0, demandeur TEXT DEFAULT '', motif TEXT DEFAULT '', statut TEXT DEFAULT 'Besoin exprimé', date_validation_cc TEXT DEFAULT '', validateur_cc TEXT DEFAULT '', numero_bc TEXT DEFAULT '', date_bc TEXT DEFAULT '', fournisseur TEXT DEFAULT '', date_reception TEXT DEFAULT '', quantite_recue REAL DEFAULT 0, bon_livraison TEXT DEFAULT '', date_mise_stock TEXT DEFAULT '', quantite_mise_stock REAL DEFAULT 0, prix_unitaire_reel REAL DEFAULT 0, observation TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS mouvements_materiaux (id SERIAL PRIMARY KEY, date_mvt TEXT NOT NULL, rue_id INTEGER REFERENCES rues(id), materiau_id INTEGER NOT NULL REFERENCES materiaux(id), type_mvt TEXT NOT NULL, quantite REAL NOT NULL, prix_unitaire REAL DEFAULT 0, fournisseur TEXT DEFAULT '', bon_livraison TEXT DEFAULT '', appro_id INTEGER, observation TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS materiels (id SERIAL PRIMARY KEY, nom TEXT NOT NULL UNIQUE, type_materiel TEXT DEFAULT '', immatriculation TEXT DEFAULT '', marque TEXT DEFAULT '', annee INTEGER DEFAULT 0, cout_horaire REAL DEFAULT 0, cout_journalier REAL DEFAULT 0, statut TEXT DEFAULT 'Disponible', etat TEXT DEFAULT 'Opérationnel', rue_id_affectation INTEGER REFERENCES rues(id), date_acquisition TEXT DEFAULT '', date_derniere_maintenance TEXT DEFAULT '', prochain_entretien_heures REAL DEFAULT 0, heures_totales REAL DEFAULT 0, heure_compteur REAL DEFAULT 0, observation TEXT DEFAULT '', observations TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS suivi_materiels (id SERIAL PRIMARY KEY, date_suivi TEXT NOT NULL, rue_id INTEGER REFERENCES rues(id), materiel_id INTEGER NOT NULL REFERENCES materiels(id), heures_marche REAL DEFAULT 0, heures_arret REAL DEFAULT 0, heures_travail REAL DEFAULT 0, carburant_materiau_id INTEGER REFERENCES materiaux(id), carburant_l REAL DEFAULT 0, carburant_consomme REAL DEFAULT 0, cout_carburant REAL DEFAULT 0, chauffeur TEXT DEFAULT '', kilometre_debut REAL DEFAULT 0, kilometre_fin REAL DEFAULT 0, panne TEXT DEFAULT '', observation TEXT DEFAULT '', observations TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS maintenance_materiels (id SERIAL PRIMARY KEY, materiel_id INTEGER NOT NULL REFERENCES materiels(id), date_maintenance TEXT NOT NULL, type_maintenance TEXT DEFAULT 'Préventive', description TEXT NOT NULL, cout REAL DEFAULT 0, prestataire TEXT DEFAULT '', pieces_changees TEXT DEFAULT '', heures_compteur REAL DEFAULT 0, prochain_entretien_h REAL DEFAULT 0, observation TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS journal_chantier (id SERIAL PRIMARY KEY, date_journal TEXT NOT NULL, rue_id INTEGER REFERENCES rues(id), meteo TEXT DEFAULT '', temperature REAL DEFAULT 20, nb_ouvriers_presents INTEGER DEFAULT 0, nb_ouvriers INTEGER DEFAULT 0, nb_encadrants INTEGER DEFAULT 0, travaux_executes TEXT DEFAULT '', travaux_realises TEXT DEFAULT '', problemes TEXT DEFAULT '', decisions TEXT DEFAULT '', visiteurs TEXT DEFAULT '', redacteur TEXT DEFAULT '', observation TEXT DEFAULT '', observations TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS caisse_chantier (id SERIAL PRIMARY KEY, date_op TEXT NOT NULL, categorie TEXT DEFAULT 'CHANTIER', rue_id INTEGER REFERENCES rues(id), type_op TEXT NOT NULL, rubrique TEXT NOT NULL, montant REAL NOT NULL, beneficiaire TEXT DEFAULT '', reference_piece TEXT DEFAULT '', mode_paiement TEXT DEFAULT 'Espèces', valide INTEGER DEFAULT 0, observation TEXT DEFAULT '', observations TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS courriers (id SERIAL PRIMARY KEY, date_courrier TEXT NOT NULL, rue_id INTEGER REFERENCES rues(id), type_courrier TEXT NOT NULL DEFAULT 'Entrant', sens TEXT DEFAULT '', reference TEXT DEFAULT '', objet TEXT NOT NULL, expediteur_destinataire TEXT DEFAULT '', expediteur TEXT DEFAULT '', destinataire TEXT DEFAULT '', priorite TEXT DEFAULT 'Normale', resume TEXT DEFAULT '', actions_requises TEXT DEFAULT '', statut TEXT DEFAULT 'En cours', observation TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS incidents (id SERIAL PRIMARY KEY, date_incident TEXT NOT NULL, rue_id INTEGER REFERENCES rues(id), type_incident TEXT NOT NULL, gravite TEXT DEFAULT 'Mineur', description TEXT NOT NULL, personne_concernee TEXT DEFAULT '', mesures_prises TEXT DEFAULT '', nb_victimes INTEGER DEFAULT 0, cout_estime REAL DEFAULT 0, actions_correctives TEXT DEFAULT '', statut TEXT DEFAULT 'Ouvert', cloture INTEGER DEFAULT 0, observation TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS audit_trail (id SERIAL PRIMARY KEY, date_action TEXT NOT NULL, table_concernee TEXT NOT NULL, action TEXT NOT NULL, enregistrement_id INTEGER, details TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS audit_log (id SERIAL PRIMARY KEY, timestamp TEXT NOT NULL, table_name TEXT NOT NULL, action TEXT NOT NULL, record_id INTEGER, details TEXT DEFAULT '')",
    ]
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for ddl in ddl_statements:
                cur.execute(ddl)
        conn.commit()
        print("[init_db] ✅ Tables OK")
    except Exception as e:
        st.error(f"🔴 init_db erreur : {e}")
        try: conn.rollback()
        except Exception: pass
        raise
    finally:
        release_conn(conn)
