# -*- coding: utf-8 -*-
"""
ODIFSALAM - Couche acces donnees via Supabase REST API (supabase-py)
Utilise HTTPS au lieu de TCP/SSL (psycopg2).
Python 3.12 requis.
"""

import os
import pandas as pd
import streamlit as st
from supabase import create_client, Client

# Valeurs par defaut (fallback si secrets non disponibles)
_DEFAULT_URL = "https://dimjiazzuqqqhgfzsmxe.supabase.co"
_DEFAULT_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRpbWppYXp6dXFxcWhnZnpzbXhlIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NTE2NzI1MCwiZXhwIjoyMDkwNzQzMjUwfQ.eyL7zAenxCOsBTILQxdZfXcoZsHEBGT4aXJAYTa75is"

@st.cache_resource
def _get_client() -> Client:
    try:
        url = st.secrets.get("SUPABASE_URL", _DEFAULT_URL)
        key = st.secrets.get("SUPABASE_SERVICE_KEY", _DEFAULT_KEY)
    except Exception:
        url = os.environ.get("SUPABASE_URL", _DEFAULT_URL)
        key = os.environ.get("SUPABASE_SERVICE_KEY", _DEFAULT_KEY)
    return create_client(url, key)

def _quote(val) -> str:
    """Formate une valeur Python pour SQL."""
    if val is None:
        return 'NULL'
    if isinstance(val, bool):
        return 'TRUE' if val else 'FALSE'
    if isinstance(val, (int, float)):
        return str(val)
    return "'" + str(val).replace("'", "''") + "'"

def _fmt(sql: str, params) -> str:
    """Remplace les ? ou %s par les valeurs correctement echappees."""
    if not params:
        return sql
    result = []
    param_iter = iter(params)
    i = 0
    while i < len(sql):
        if sql[i] == '?' :
            try:
                result.append(_quote(next(param_iter)))
            except StopIteration:
                result.append('?')
            i += 1
        elif sql[i] == '%' and i + 1 < len(sql) and sql[i+1] == 's':
            try:
                result.append(_quote(next(param_iter)))
            except StopIteration:
                result.append('%s')
            i += 2
        else:
            result.append(sql[i])
            i += 1
    return ''.join(result)

def qdf(sql: str, p=None) -> pd.DataFrame:
    """Execute une SELECT et retourne un DataFrame."""
    client = _get_client()
    try:
        formatted = _fmt(sql, p)
        result = client.rpc("odifsalam_query", {"q": formatted}).execute()
        data = result.data
        if data and isinstance(data, list):
            return pd.DataFrame(data)
        return pd.DataFrame()
    except Exception as e:
        print(f"[qdf] {e}")
        return pd.DataFrame()

def exsql(sql: str, p=None):
    """Execute INSERT/UPDATE/DELETE. Retourne l'id si INSERT."""
    client = _get_client()
    is_insert = sql.strip().upper().startswith("INSERT")
    if is_insert and "RETURNING" not in sql.upper():
        sql = sql + " RETURNING id"
    try:
        formatted = _fmt(sql, p)
        result = client.rpc("odifsalam_exec", {"q": formatted}).execute()
        if is_insert and result.data:
            data = result.data
            if isinstance(data, list) and len(data) > 0:
                return data[0].get('id')
            elif isinstance(data, dict):
                return data.get('id')
        return None
    except Exception as e:
        print(f"[exsql] {e}")
        raise

def exmany(sql: str, rows):
    """Execute une requete sur plusieurs lignes."""
    client = _get_client()
    try:
        for row in rows:
            formatted = _fmt(sql, row)
            client.rpc("odifsalam_exec", {"q": formatted}).execute()
    except Exception as e:
        print(f"[exmany] {e}")
        raise

def get_conn():
    raise NotImplementedError("Utilisez qdf/exsql/exmany avec supabase-py")

def release_conn(conn):
    pass

def init_db():
    """Cree les tables si elles n'existent pas et verifie la connexion."""
    client = _get_client()
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
        "CREATE TABLE IF NOT EXISTS pointage (id SERIAL PRIMARY KEY, date_pointage TEXT NOT NULL, personnel_id INTEGER NOT NULL REFERENCES personnel(id), rue_id INTEGER REFERENCES rues(id), statut TEXT NOT NULL DEFAULT 'Present', heures_travaillees REAL DEFAULT 8, heures_normales REAL DEFAULT 8, heures_sup REAL DEFAULT 0, tache TEXT DEFAULT '', observation TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS materiaux (id SERIAL PRIMARY KEY, nom TEXT NOT NULL UNIQUE, unite TEXT NOT NULL, categorie TEXT DEFAULT 'Materiau', stock_initial REAL DEFAULT 0, seuil_alerte REAL DEFAULT 0, prix_unitaire REAL DEFAULT 0)",
        "CREATE TABLE IF NOT EXISTS approvisionnements (id SERIAL PRIMARY KEY, date_besoin TEXT NOT NULL, rue_id INTEGER REFERENCES rues(id), materiau_id INTEGER REFERENCES materiaux(id), designation TEXT NOT NULL, unite TEXT NOT NULL, quantite_demandee REAL DEFAULT 0, prix_unitaire_estime REAL DEFAULT 0, demandeur TEXT DEFAULT '', motif TEXT DEFAULT '', statut TEXT DEFAULT 'Besoin exprime', date_validation_cc TEXT DEFAULT '', validateur_cc TEXT DEFAULT '', numero_bc TEXT DEFAULT '', date_bc TEXT DEFAULT '', fournisseur TEXT DEFAULT '', date_reception TEXT DEFAULT '', quantite_recue REAL DEFAULT 0, bon_livraison TEXT DEFAULT '', date_mise_stock TEXT DEFAULT '', quantite_mise_stock REAL DEFAULT 0, prix_unitaire_reel REAL DEFAULT 0, observation TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS mouvements_materiaux (id SERIAL PRIMARY KEY, date_mvt TEXT NOT NULL, rue_id INTEGER REFERENCES rues(id), materiau_id INTEGER NOT NULL REFERENCES materiaux(id), type_mvt TEXT NOT NULL, quantite REAL NOT NULL, prix_unitaire REAL DEFAULT 0, fournisseur TEXT DEFAULT '', bon_livraison TEXT DEFAULT '', appro_id INTEGER, observation TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS materiels (id SERIAL PRIMARY KEY, nom TEXT NOT NULL UNIQUE, type_materiel TEXT DEFAULT '', immatriculation TEXT DEFAULT '', marque TEXT DEFAULT '', annee INTEGER DEFAULT 0, cout_horaire REAL DEFAULT 0, cout_journalier REAL DEFAULT 0, statut TEXT DEFAULT 'Disponible', etat TEXT DEFAULT 'Operationnel', rue_id_affectation INTEGER REFERENCES rues(id), date_acquisition TEXT DEFAULT '', date_derniere_maintenance TEXT DEFAULT '', prochain_entretien_heures REAL DEFAULT 0, heures_totales REAL DEFAULT 0, heure_compteur REAL DEFAULT 0, observation TEXT DEFAULT '', observations TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS suivi_materiels (id SERIAL PRIMARY KEY, date_suivi TEXT NOT NULL, rue_id INTEGER REFERENCES rues(id), materiel_id INTEGER NOT NULL REFERENCES materiels(id), heures_marche REAL DEFAULT 0, heures_arret REAL DEFAULT 0, heures_travail REAL DEFAULT 0, carburant_materiau_id INTEGER REFERENCES materiaux(id), carburant_l REAL DEFAULT 0, carburant_consomme REAL DEFAULT 0, cout_carburant REAL DEFAULT 0, chauffeur TEXT DEFAULT '', kilometre_debut REAL DEFAULT 0, kilometre_fin REAL DEFAULT 0, panne TEXT DEFAULT '', observation TEXT DEFAULT '', observations TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS maintenance_materiels (id SERIAL PRIMARY KEY, materiel_id INTEGER NOT NULL REFERENCES materiels(id), date_maintenance TEXT NOT NULL, type_maintenance TEXT DEFAULT 'Preventive', description TEXT NOT NULL, cout REAL DEFAULT 0, prestataire TEXT DEFAULT '', pieces_changees TEXT DEFAULT '', heures_compteur REAL DEFAULT 0, prochain_entretien_h REAL DEFAULT 0, observation TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS journal_chantier (id SERIAL PRIMARY KEY, date_journal TEXT NOT NULL, rue_id INTEGER REFERENCES rues(id), meteo TEXT DEFAULT '', temperature REAL DEFAULT 20, nb_ouvriers_presents INTEGER DEFAULT 0, nb_ouvriers INTEGER DEFAULT 0, nb_encadrants INTEGER DEFAULT 0, travaux_executes TEXT DEFAULT '', travaux_realises TEXT DEFAULT '', problemes TEXT DEFAULT '', decisions TEXT DEFAULT '', visiteurs TEXT DEFAULT '', redacteur TEXT DEFAULT '', observation TEXT DEFAULT '', observations TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS caisse_chantier (id SERIAL PRIMARY KEY, date_op TEXT NOT NULL, categorie TEXT DEFAULT 'CHANTIER', rue_id INTEGER REFERENCES rues(id), type_op TEXT NOT NULL, rubrique TEXT NOT NULL, montant REAL NOT NULL, beneficiaire TEXT DEFAULT '', reference_piece TEXT DEFAULT '', mode_paiement TEXT DEFAULT 'Especes', valide INTEGER DEFAULT 0, observation TEXT DEFAULT '', observations TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS courriers (id SERIAL PRIMARY KEY, date_courrier TEXT NOT NULL, rue_id INTEGER REFERENCES rues(id), type_courrier TEXT NOT NULL DEFAULT 'Entrant', sens TEXT DEFAULT '', reference TEXT DEFAULT '', objet TEXT NOT NULL, expediteur_destinataire TEXT DEFAULT '', expediteur TEXT DEFAULT '', destinataire TEXT DEFAULT '', priorite TEXT DEFAULT 'Normale', resume TEXT DEFAULT '', actions_requises TEXT DEFAULT '', statut TEXT DEFAULT 'En cours', observation TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS incidents (id SERIAL PRIMARY KEY, date_incident TEXT NOT NULL, rue_id INTEGER REFERENCES rues(id), type_incident TEXT NOT NULL, gravite TEXT DEFAULT 'Mineur', description TEXT NOT NULL, personne_concernee TEXT DEFAULT '', mesures_prises TEXT DEFAULT '', nb_victimes INTEGER DEFAULT 0, cout_estime REAL DEFAULT 0, actions_correctives TEXT DEFAULT '', statut TEXT DEFAULT 'Ouvert', cloture INTEGER DEFAULT 0, observation TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS audit_trail (id SERIAL PRIMARY KEY, date_action TEXT NOT NULL, table_concernee TEXT NOT NULL, action TEXT NOT NULL, enregistrement_id INTEGER, details TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS audit_log (id SERIAL PRIMARY KEY, timestamp TEXT NOT NULL, table_name TEXT NOT NULL, action TEXT NOT NULL, record_id INTEGER, details TEXT DEFAULT '')",
    ]
    try:
        for ddl in ddl_statements:
            client.rpc("odifsalam_exec", {"q": ddl}).execute()
        # Verification connexion
        client.rpc("odifsalam_query", {"q": "SELECT 1 as ok"}).execute()
        print("[init_db] OK - Supabase REST API connecte")
    except Exception as e:
        st.error(f"Erreur init_db : {e}")
        raise
