# -*- coding: utf-8 -*-
"""ODIFSALAM v3.0 — Structure Dossiers → Chantiers → Livrables/Rues
   Migration PostgreSQL/Supabase — données permanentes
"""
from datetime import date, datetime, timedelta
from io import BytesIO
import pandas as pd
import streamlit as st

# ── BASE DE DONNÉES PostgreSQL (Supabase) ─────────────────────────────────
from database import init_db, qdf, exsql, exmany, get_conn, release_conn

st.set_page_config(page_title="ODIFSALAM - Gestion de Projets", layout="wide", page_icon="🏗️", initial_sidebar_state="expanded")

CURRENCIES = {"GNF":"GNF","FCFA":"FCFA","USD":"$","EUR":"€"}
CATEGORIES_PERSONNEL = ["Directeur de projet","Chef de chantier","Conducteur de travaux","Ingénieur","Technicien","Chef d'équipe","Ouvrier qualifié","Ouvrier","Manœuvre","Conducteur d'engins","Chauffeur","Gardien","Autre"]

# ── Unités de mesure BTP ───────────────────────────────────────
UNITES_MESURE = [
    # Longueur / linéaire
    "ml","m","km",
    # Surface
    "m²","m2","ha","km²",
    # Volume
    "m³","m3","L","l","litre",
    # Masse
    "kg","T","tonne","quintal",
    # Quantité / forfait
    "U","unité","Ens","ensemble","Ft","forfait","Lot","lot",
    # Temps
    "h","j","mois","semaine",
    # Matériaux spécifiques
    "sac","palette","rouleau","ml²","Ml","barre",
    # Autre
    "Autre...",
]

def unite_selectbox(label, key, default=""):
    """Selectbox d'unité + champ de saisie libre si 'Autre...' est sélectionné."""
    opts=UNITES_MESURE
    if default and default not in opts:
        opts=[default]+[o for o in UNITES_MESURE if o!="Autre..."]+["Autre..."]
    idx=opts.index(default) if default in opts else 0
    sel=st.selectbox(label,opts,index=idx,key=key+"_sel")
    if sel=="Autre...":
        custom=st.text_input("Préciser l'unité",value=default if default not in UNITES_MESURE else "",key=key+"_custom",placeholder="ex: pl, dose, barre...")
        return custom.strip() if custom.strip() else "?"
    return sel

STATUT_PERSONNEL = ["Présent","Absent","Congé","Maladie","Mission"]
TYPES_MATERIEL = ["Engin de terrassement","Camion","Grue","Compacteur","Centrale à béton","Groupe électrogène","Pompe","Outillage","Autre"]
STATUTS_APPRO = ["Besoin exprimé","Validé chef chantier","Bon de commande émis","Réceptionné","Mis en stock"]
GRAVITE_INC = ["Mineur","Modéré","Grave","Très grave"]

for k,v in [("currency_code","FCFA"),("current_page","dashboard")]:
    if k not in st.session_state: st.session_state[k]=v

st.markdown("""<style>
div[data-testid="metric-container"]{background:#f8f9fa;border:1px solid #dee2e6;border-radius:8px;padding:8px;}
.section-header{background:linear-gradient(90deg,#1f4e79,#2980b9);color:white;padding:8px 16px;border-radius:6px;margin:10px 0 6px 0;font-weight:bold;}
.orgchart-box{background:#e8f4fd;border:2px solid #2980b9;border-radius:8px;padding:10px;text-align:center;margin:4px;display:inline-block;min-width:140px;}
.orgchart-box-green{background:#e8fdf0;border-color:#27ae60;}
.orgchart-box-orange{background:#fef9e7;border-color:#f39c12;}
</style>""", unsafe_allow_html=True)

# ── INITIALISATION BASE DE DONNÉES ────────────────────────────
try:
    init_db()
except Exception as _db_err:
    _msg = str(_db_err)
    if "Circuit breaker" in _msg:
        st.error("🔴 Supabase a temporairement bloqué les connexions (trop de tentatives échouées).")
        st.warning("⏳ **Attendez 2-3 minutes** puis cliquez sur le bouton ci-dessous pour réessayer.")
    else:
        st.error(f"🔴 Impossible de se connecter à la base de données : {_msg}")
        st.warning("⏳ Cliquez sur le bouton ci-dessous pour réessayer dans quelques instants.")
    if st.button("🔄 Réessayer la connexion"):
        st.cache_resource.clear()
        st.rerun()
    st.stop()

# ── UTILS ─────────────────────────────────────────────────────
def audit(table,action,rid=None,det=""):
    try: exsql("INSERT INTO audit_log(timestamp,table_name,action,record_id,details)VALUES(?,?,?,?,?)",[datetime.now().strftime("%Y-%m-%d %H:%M:%S"),table,action,rid,det])
    except Exception: pass

def cur_sym(): return CURRENCIES.get(st.session_state.get("currency_code","FCFA"),"FCFA")
def fmt(x):
    try: return f"{float(x):,.0f} {cur_sym()}".replace(",", " ")
    except: return "0"
def fpct(x):
    try: return f"{float(x):.1f} %"
    except: return "0.0 %"

def to_xl(d):
    out=BytesIO()
    with pd.ExcelWriter(out,engine="openpyxl") as w:
        for n,df in d.items(): df.to_excel(w,sheet_name=str(n)[:31],index=False)
    out.seek(0); return out.getvalue()

def _v(df, col, default=0):
    """Accès sécurisé à df.iloc[0][col] — retourne default si DataFrame vide."""
    if df is None or df.empty or col not in df.columns:
        return default
    val = df.iloc[0][col]
    return val if val is not None else default

def get_dos(): return qdf("SELECT * FROM dossiers ORDER BY nom")
def get_rues(did=None):
    if did: return qdf("SELECT * FROM rues WHERE dossier_id=? ORDER BY nom",[did])
    return qdf("SELECT * FROM rues ORDER BY nom")
def get_livs(cid): return qdf("SELECT * FROM livrables WHERE chantier_id=? ORDER BY nom",[cid])
def get_mats(cat=None):
    if cat: return qdf("SELECT * FROM materiaux WHERE categorie=? ORDER BY nom",[cat])
    return qdf("SELECT * FROM materiaux ORDER BY nom")
def get_carbs(): return qdf("SELECT * FROM materiaux WHERE categorie='Carburant' ORDER BY nom")
def get_engs(): return qdf("SELECT * FROM materiels ORDER BY nom")
def get_pers(a=True):
    if a: return qdf("SELECT * FROM personnel WHERE actif=1 ORDER BY categorie,nom")
    return qdf("SELECT * FROM personnel ORDER BY categorie,nom")
def get_sts(): return qdf("SELECT * FROM sous_traitants ORDER BY nom")
def get_devis(rid,lvid=None):
    if lvid: return qdf("SELECT * FROM devis_rue WHERE rue_id=? AND livrable_id=? ORDER BY id",[rid,lvid])
    return qdf("SELECT * FROM devis_rue WHERE rue_id=? ORDER BY id",[rid])

def stock_mat(mid):
    r=qdf("SELECT m.stock_initial+COALESCE(SUM(CASE WHEN mm.type_mvt='ENTREE' THEN mm.quantite ELSE 0 END),0)-COALESCE(SUM(CASE WHEN mm.type_mvt='SORTIE' THEN mm.quantite ELSE 0 END),0) AS s FROM materiaux m LEFT JOIN mouvements_materiaux mm ON mm.materiau_id=m.id WHERE m.id=?",[mid])
    return float(r.iloc[0]["s"] or 0) if not r.empty else 0

def delai_cons(dd_str,dj):
    try:
        if not dd_str or not dj: return 0,int(dj or 0),0
        dd=datetime.strptime(str(dd_str),"%Y-%m-%d").date()
        cons=(date.today()-dd).days; rest=int(dj)-cons
        pct=min(100,round(cons/dj*100,1)) if dj else 0
        return max(0,cons),rest,pct
    except: return 0,int(dj or 0),0

def ch_label_map():
    df=qdf("SELECT r.id,r.nom,COALESCE(d.nom,'—') AS dos FROM rues r LEFT JOIN dossiers d ON d.id=r.dossier_id ORDER BY d.nom,r.nom")
    if df.empty: return [],{}
    labs=[f"[{row['dos']}] {row['nom']}" for _,row in df.iterrows()]
    return labs,{labs[i]:int(df.iloc[i]["id"]) for i in range(len(df))}

def _norm_str(c):
    """Normalise une chaîne de caractères pour la comparaison de colonnes."""
    c=str(c).strip().lower()
    for src,dst in [("é","e"),("è","e"),("ê","e"),("ë","e"),("à","a"),("â","a"),("ù","u"),("û","u"),("ô","o"),("î","i"),("ï","i"),("ç","c"),("œ","oe"),("æ","ae"),(" ","_"),("/","_"),("-","_"),(".","_"),("(",""),(")",""),("°","")]:
        c=c.replace(src,dst)
    while "__" in c: c=c.replace("__","_")
    return c.strip("_")

def norm_cols(df):
    df.columns=[_norm_str(c) for c in df.columns]
    return df

def _detect_header_row(filepath_or_buffer, sheet=0, max_scan=15):
    """
    Scanne les premières lignes du fichier Excel pour trouver la ligne
    qui contient les vrais en-têtes (ex: Désignation, Unité, etc.).
    Retourne le numéro de ligne (0-indexé) à passer à header= de read_excel.
    """
    keywords={"designation","libelle","intitule","description","poste","ouvrage","travaux","prestation","nature","article",
               "unite","unit","unites","mesure","quantite","qte","prix","pu","code","reference","num"}
    # Lire sans header pour avoir les vraies valeurs de chaque cellule
    try:
        raw=pd.read_excel(filepath_or_buffer, header=None, nrows=max_scan)
    except Exception:
        return 0
    best_row=0; best_score=0
    for i,row in raw.iterrows():
        score=0
        for cell in row:
            normalized=_norm_str(str(cell))
            if any(kw in normalized for kw in keywords):
                score+=1
        if score>best_score:
            best_score=score; best_row=i
    # Si aucun mot-clé trouvé, on suppose row 0
    return best_row if best_score>0 else 0

def read_excel_smart(filepath_or_buffer):
    """Lit un fichier Excel en détectant automatiquement la ligne d'en-tête."""
    import io
    # On a besoin de lire deux fois : une pour détecter, une pour charger
    # Si c'est un UploadedFile Streamlit, on lit les bytes d'abord
    if hasattr(filepath_or_buffer,'read'):
        data=filepath_or_buffer.read()
        header_row=_detect_header_row(io.BytesIO(data))
        df=pd.read_excel(io.BytesIO(data), header=header_row)
    else:
        header_row=_detect_header_row(filepath_or_buffer)
        df=pd.read_excel(filepath_or_buffer, header=header_row)
    # Supprimer les colonnes et lignes totalement vides
    df=df.dropna(how="all",axis=1).dropna(how="all",axis=0).reset_index(drop=True)
    return norm_cols(df)

def find_col(cols,aliases):
    # 1. correspondance exacte
    for a in aliases:
        if a in cols: return a
    # 2. correspondance partielle (l'alias est contenu dans le nom de colonne)
    for a in aliases:
        for c in cols:
            if a in c: return c
    # 3. correspondance partielle inverse (le nom de colonne est contenu dans l'alias)
    for a in aliases:
        for c in cols:
            if c in a and len(c)>=3: return c
    return None

# ── SIDEBAR ───────────────────────────────────────────────────
st.sidebar.title("🏗️ ODIFSALAM"); st.sidebar.caption("v3.0 — Multi-Projets")
st.session_state["currency_code"]=st.sidebar.selectbox("💱 Monnaie",list(CURRENCIES.keys()),index=list(CURRENCIES.keys()).index(st.session_state.get("currency_code","FCFA")),key="sb_cur")
st.sidebar.markdown("---")
MENU=[("📊","Tableau de bord","dashboard"),("─── ORGANISATION ───",None,None),("📁","Dossiers / Projets","dossiers"),("🗺️","Chantiers","chantiers"),("📐","Livrables / Rues","livrables"),("📋","Fiche de Chantier","fiche_chantier"),("🏛️","Organigramme","organigramme"),("─── MARCHÉ ───",None,None),("📄","Devis du marché","devis"),("📊","Décompte travaux","decompte"),("─── SOUS-TRAITANTS ───",None,None),("🤝","Sous-traitants","sts"),("📝","Devis ST","devis_st"),("🧾","Décompte ST","decompte_st"),("─── RESSOURCES ───",None,None),("👷","Personnel","pers"),("✅","Pointage","pointage"),("─── APPROVISIONNEMENT ───",None,None),("🔄","Circuit Appro","appro"),("📦","Stock matériaux","stock"),("─── PARC ENGINS ───",None,None),("🚧","Matériels & Engins","engins"),("🔧","Maintenance","maint"),("📅","Suivi journalier engins","suivi_eng"),("─── CHANTIER ───",None,None),("📔","Journal de chantier","journal"),("💰","Caisse chantier","caisse"),("⚠️","Sécurité & Incidents","incidents"),("📬","Courriers","courriers"),("─── RAPPORTS ───",None,None),("📈","Rapports & Exports","rapports"),("🔍","Audit Trail","audit")]
for item in MENU:
    if item[1] is None: st.sidebar.markdown(f"<small style='color:#888'>{item[0]}</small>",unsafe_allow_html=True)
    else:
        icon,label,key=item
        if st.sidebar.button(f"{icon} {label}",key=f"nav_{key}",use_container_width=True): st.session_state["current_page"]=key
page=st.session_state["current_page"]

# ── TABLEAU DE BORD ───────────────────────────────────────────
if page=="dashboard":
    st.title("📊 Tableau de Bord — ODIFSALAM"); st.caption(f"Au {date.today().strftime('%d/%m/%Y')}")

    # ── Sélecteur de chantier ─────────────────────────────────────
    _labs_db, _idmap_db = ch_label_map()
    _db_opts = ["🌐 Tous les chantiers"] + _labs_db
    db_ch_sel = st.selectbox("🗺️ Filtrer par chantier", _db_opts, key="dash_ch_sel")
    rid_dash = _idmap_db.get(db_ch_sel) if db_ch_sel != "🌐 Tous les chantiers" else None
    if rid_dash:
        st.info(f"📍 Affichage : **{db_ch_sel}**")
    st.markdown("---")

    df_dos=get_dos(); df_rues=get_rues(); today=str(date.today())

    # ── KPI filtrés par chantier ──────────────────────────────────
    _w_rid  = " AND rue_id=?"    # clause WHERE additionnelle
    _p_rid  = [rid_dash]         # paramètre correspondant

    if rid_dash:
        nb_pres = int(_v(qdf(
            f"SELECT COUNT(*) AS n FROM pointage WHERE date_pointage=? AND statut='Présent'{_w_rid}",
            [today]+_p_rid), "n"))
        caisse = qdf(
            f"SELECT COALESCE(SUM(CASE WHEN type_op='Recette' THEN montant ELSE 0 END),0) AS rec,"
            f"COALESCE(SUM(CASE WHEN type_op='Dépense' THEN montant ELSE 0 END),0) AS dep,"
            f"COALESCE(SUM(CASE WHEN type_op='Avance' THEN montant ELSE 0 END),0) AS avance "
            f"FROM caisse_chantier WHERE rue_id=?", _p_rid)
        glo = qdf(
            "SELECT COALESCE(SUM(d.quantite_marche*d.prix_unitaire),0) AS mm,"
            "COALESCE(SUM(COALESCE(q.qe,0)*d.prix_unitaire),0) AS me "
            "FROM devis_rue d LEFT JOIN(SELECT devis_id,SUM(quantite_jour) AS qe "
            "FROM realisations_journalieres GROUP BY devis_id)q ON q.devis_id=d.id "
            "WHERE d.rue_id=?", _p_rid)
        nb_inc = int(_v(qdf(
            f"SELECT COUNT(*) AS n FROM incidents WHERE cloture=0{_w_rid}",
            _p_rid), "n"))
    else:
        nb_pres = int(_v(qdf(
            "SELECT COUNT(*) AS n FROM pointage WHERE date_pointage=? AND statut='Présent'",
            [today]), "n"))
        caisse = qdf(
            "SELECT COALESCE(SUM(CASE WHEN type_op='Recette' THEN montant ELSE 0 END),0) AS rec,"
            "COALESCE(SUM(CASE WHEN type_op='Dépense' THEN montant ELSE 0 END),0) AS dep,"
            "COALESCE(SUM(CASE WHEN type_op='Avance' THEN montant ELSE 0 END),0) AS avance "
            "FROM caisse_chantier")
        glo = qdf(
            "SELECT COALESCE(SUM(d.quantite_marche*d.prix_unitaire),0) AS mm,"
            "COALESCE(SUM(COALESCE(q.qe,0)*d.prix_unitaire),0) AS me "
            "FROM devis_rue d LEFT JOIN(SELECT devis_id,SUM(quantite_jour) AS qe "
            "FROM realisations_journalieres GROUP BY devis_id)q ON q.devis_id=d.id")
        nb_inc = int(_v(qdf("SELECT COUNT(*) AS n FROM incidents WHERE cloture=0"), "n"))

    _rec=float(_v(caisse, "rec"))
    _dep=float(_v(caisse, "dep"))
    _avance=float(_v(caisse, "avance"))
    solde = _rec - _dep - _avance
    mm=float(_v(glo, "mm")); me=float(_v(glo, "me"))
    nb_app=int(_v(qdf("SELECT COUNT(*) AS n FROM approvisionnements WHERE statut!='Mis en stock'"), "n"))

    c1,c2,c3,c4,c5,c6=st.columns(6)
    c1.metric("📁 Dossiers",len(df_dos)); c2.metric("🗺️ Chantiers",len(df_rues)); c3.metric("👷 Présents",nb_pres)
    c4.metric("📈 Exécution",fpct(me/mm*100 if mm else 0)); c5.metric("💰 Solde caisse",fmt(solde)); c6.metric("⚠️ Incidents",nb_inc)
    st.markdown("---")
    cl,cr=st.columns(2)
    with cl:
        st.subheader("📊 Avancement par chantier")
        av=qdf("SELECT r.nom AS Chantier,COALESCE(d.nom,'—') AS Dossier,ROUND(COALESCE(SUM(dv.quantite_marche*dv.prix_unitaire),0),0) AS Marché,ROUND(COALESCE(SUM(COALESCE(q.qe,0)*dv.prix_unitaire),0),0) AS Exécuté,CASE WHEN SUM(dv.quantite_marche*dv.prix_unitaire)>0 THEN ROUND(SUM(COALESCE(q.qe,0)*dv.prix_unitaire)/SUM(dv.quantite_marche*dv.prix_unitaire)*100,1) ELSE 0 END AS \"Taux %\" FROM rues r LEFT JOIN dossiers d ON d.id=r.dossier_id LEFT JOIN devis_rue dv ON dv.rue_id=r.id LEFT JOIN(SELECT devis_id,SUM(quantite_jour) AS qe FROM realisations_journalieres GROUP BY devis_id)q ON q.devis_id=dv.id GROUP BY r.id ORDER BY r.nom")
        st.dataframe(av,use_container_width=True)
        st.subheader("⏱️ Délais")
        drows=[]
        for _,r in df_rues.iterrows():
            cons,rest,pct=delai_cons(r.get("date_demarrage"),r.get("delai_jours",0))
            drows.append({"Chantier":r["nom"],"Délai(j)":r.get("delai_jours",0),"Consommé":cons,"Restant":rest,"% cons.":pct})
        if drows: st.dataframe(pd.DataFrame(drows),use_container_width=True)
    with cr:
        st.subheader("🦺 Alertes stocks")
        al=qdf("SELECT m.nom AS Matériau,m.unite AS Unité,ROUND(m.stock_initial+COALESCE(SUM(CASE WHEN mm.type_mvt='ENTREE' THEN mm.quantite ELSE 0 END),0)-COALESCE(SUM(CASE WHEN mm.type_mvt='SORTIE' THEN mm.quantite ELSE 0 END),0),2) AS Stock,m.seuil_alerte AS Seuil FROM materiaux m LEFT JOIN mouvements_materiaux mm ON mm.materiau_id=m.id GROUP BY m.id HAVING Stock<=m.seuil_alerte AND m.seuil_alerte>0")
        if not al.empty: st.error(f"⚠️ {len(al)} article(s) sous le seuil !"); st.dataframe(al,use_container_width=True)
        else: st.success("✅ Tous les stocks OK")
        st.subheader("🔄 Appros en attente")
        app=qdf("SELECT a.date_besoin AS Date,COALESCE(r.nom,'—') AS Chantier,a.designation AS Article,a.statut AS Statut FROM approvisionnements a LEFT JOIN rues r ON r.id=a.rue_id WHERE a.statut!='Mis en stock' ORDER BY a.date_besoin DESC LIMIT 8")
        if not app.empty: st.dataframe(app,use_container_width=True)
        else: st.info("Aucun approvisionnement en attente.")

# ── DOSSIERS ──────────────────────────────────────────────────
elif page=="dossiers":
    st.title("📁 Dossiers / Projets")
    t1,t2,t3=st.tabs(["➕ Nouveau","✏️ Modifier","📋 Liste"])
    with t1:
        with st.form("f_dos_add"):
            c1,c2=st.columns(2); nom_d=c1.text_input("Nom du dossier *"); client=c2.text_input("Client / Maître d'ouvrage")
            desc=st.text_area("Description",height=60)
            s_opts=["En cours","Terminé","Suspendu","En préparation"]
            s_d=st.selectbox("Statut",s_opts); dc=st.text_input("Date création (YYYY-MM-DD)",value=str(date.today()))
            if st.form_submit_button("💾 Créer"):
                if nom_d.strip():
                    try:
                        did=exsql("INSERT INTO dossiers(nom,description,client,date_creation,statut)VALUES(?,?,?,?,?)",[nom_d.strip(),desc.strip(),client.strip(),dc.strip(),s_d])
                        audit("dossiers","CREATE",did,f"Nouveau dossier: {nom_d}"); st.success("✅ Dossier créé."); st.rerun()
                    except Exception as e: st.error(f"Erreur: {e}")
                else: st.error("Nom obligatoire.")
    with t2:
        df_d=get_dos()
        if df_d.empty: st.info("Aucun dossier.")
        else:
            sel=st.selectbox("Dossier à modifier",df_d["nom"].tolist(),key="dos_edit_sel"); row=df_d[df_d["nom"]==sel].iloc[0]
            with st.form("f_dos_edit"):
                c1,c2=st.columns(2); en=c1.text_input("Nom *",value=str(row["nom"])); ec=c2.text_input("Client",value=str(row.get("client") or ""))
                ed=st.text_area("Description",value=str(row.get("description") or ""),height=60)
                s_opts=["En cours","Terminé","Suspendu","En préparation"]; cs=str(row.get("statut") or "En cours")
                es=st.selectbox("Statut",s_opts,index=s_opts.index(cs) if cs in s_opts else 0)
                ca,cb=st.columns(2); sv=ca.form_submit_button("✅ Enregistrer"); dl=cb.form_submit_button("🗑️ Supprimer")
            if sv:
                exsql("UPDATE dossiers SET nom=?,description=?,client=?,statut=? WHERE id=?",[en,ed,ec,es,int(row["id"])])
                audit("dossiers","UPDATE",int(row["id"]),f"Modif dossier {en}"); st.success("✅ Modifié."); st.rerun()
            if dl:
                exsql("DELETE FROM dossiers WHERE id=?",[int(row["id"])]); st.success("Supprimé."); st.rerun()
    with t3:
        df_d=get_dos()
        if not df_d.empty:
            for _,r in df_d.iterrows():
                nb_ch=int(_v(qdf("SELECT COUNT(*) AS n FROM rues WHERE dossier_id=?",[int(r["id"])]), "n"))
                st.markdown(f"**📁 {r['nom']}** — {r.get('client','—')} | {r.get('statut','—')} | {nb_ch} chantier(s)")
            st.dataframe(df_d,use_container_width=True)

# ── CHANTIERS ─────────────────────────────────────────────────
elif page=="chantiers":
    st.title("🗺️ Chantiers")
    df_dos=get_dos()
    dos_opts=["(Sans dossier)"]+df_dos["nom"].tolist() if not df_dos.empty else ["(Sans dossier)"]
    t1,t2,t3=st.tabs(["➕ Nouveau","✏️ Modifier","📋 Liste"])
    with t1:
        with st.form("f_ch_add"):
            c1,c2=st.columns(2); nom_c=c1.text_input("Nom du chantier *"); dos_sel=c2.selectbox("Dossier parent",dos_opts,key="ch_add_dos")
            c3,c4=st.columns(2); num_m=c3.text_input("N° Marché"); st_opts=["En cours","Terminé","Suspendu","Non démarré"]; st_c=c4.selectbox("Statut",st_opts)
            obj=st.text_area("Objet du marché",height=60); obs=st.text_area("Observation",height=60)
            if st.form_submit_button("💾 Créer"):
                if nom_c.strip():
                    did=int(df_dos[df_dos["nom"]==dos_sel].iloc[0]["id"]) if dos_sel!="(Sans dossier)" and not df_dos.empty else None
                    rid=exsql("INSERT INTO rues(nom,dossier_id,numero_marche,objet_marche,statut_chantier,observation)VALUES(?,?,?,?,?,?)",[nom_c.strip(),did,num_m.strip(),obj.strip(),st_c,obs.strip()])
                    audit("rues","CREATE",rid,f"Chantier: {nom_c}"); st.success("✅ Chantier créé."); st.rerun()
                else: st.error("Nom obligatoire.")
    with t2:
        df_r=get_rues()
        if df_r.empty: st.info("Aucun chantier.")
        else:
            ch_labels=qdf("SELECT r.id,r.nom,COALESCE(d.nom,'—') AS dos FROM rues r LEFT JOIN dossiers d ON d.id=r.dossier_id ORDER BY d.nom,r.nom")
            labels=[f"[{row['dos']}] {row['nom']}" for _,row in ch_labels.iterrows()]
            id_map={labels[i]:int(ch_labels.iloc[i]["id"]) for i in range(len(ch_labels))}
            sel=st.selectbox("Chantier",labels,key="ch_edit_sel"); rid=id_map.get(sel)
            if rid:
                row=df_r[df_r["id"]==rid].iloc[0]
                with st.form("f_ch_edit"):
                    c1,c2=st.columns(2); en=c1.text_input("Nom *",value=str(row["nom"])); enum=c2.text_input("N° Marché",value=str(row.get("numero_marche") or ""))
                    st_opts=["En cours","Terminé","Suspendu","Non démarré"]; cs=str(row.get("statut_chantier") or "En cours")
                    est=st.selectbox("Statut",st_opts,index=st_opts.index(cs) if cs in st_opts else 0)
                    eobj=st.text_area("Objet",value=str(row.get("objet_marche") or ""),height=60)
                    eobs=st.text_area("Observation",value=str(row.get("observation") or ""),height=60)
                    ca,cb=st.columns(2); sv=ca.form_submit_button("✅ Enregistrer"); dl=cb.form_submit_button("🗑️ Supprimer")
                if sv:
                    exsql("UPDATE rues SET nom=?,numero_marche=?,objet_marche=?,statut_chantier=?,observation=? WHERE id=?",[en,enum,eobj,est,eobs,rid])
                    audit("rues","UPDATE",rid,f"Modif chantier {en}"); st.success("✅ Modifié."); st.rerun()
                if dl:
                    # Suppression en cascade : supprimer d'abord tous les enregistrements liés
                    tables_liees=[
                        "livrables","devis_rue","realisations_journalieres",
                        "mouvements_materiaux","suivi_materiels","caisse_chantier",
                        "journal_chantier","incidents","courriers","besoins_appro",
                        "devis_st","decomptes_st","paiements_st","sous_traitants",
                        "pointage","personnel",
                    ]
                    for tbl in tables_liees:
                        try: exsql(f"DELETE FROM {tbl} WHERE rue_id=?",[rid])
                        except: pass
                    exsql("DELETE FROM rues WHERE id=?",[rid])
                    audit("rues","DELETE",rid,f"Suppression chantier {rid} + données liées")
                    st.success("✅ Chantier et toutes ses données associées supprimés."); st.rerun()
    with t3:
        df_list=qdf("SELECT COALESCE(d.nom,'—') AS Dossier,r.nom AS Chantier,r.numero_marche AS \"N° Marché\",r.statut_chantier AS Statut,r.date_demarrage AS Démarrage,r.delai_jours AS \"Délai(j)\" FROM rues r LEFT JOIN dossiers d ON d.id=r.dossier_id ORDER BY d.nom,r.nom")
        if not df_list.empty: st.dataframe(df_list,use_container_width=True); st.download_button("📥 Export",to_xl({"Chantiers":df_list}),"chantiers.xlsx","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ── LIVRABLES ─────────────────────────────────────────────────
elif page=="livrables":
    st.title("📐 Livrables / Rues")
    labs,id_map=ch_label_map()
    if not labs: st.warning("Créez d'abord un chantier."); st.stop()
    sel_ch=st.selectbox("Chantier",labs,key="liv_ch")
    rid=id_map.get(sel_ch)
    t1,t2,t3=st.tabs(["➕ Nouveau livrable","✏️ Modifier","📋 Liste"])
    with t1:
        with st.form("f_liv_add"):
            c1,c2=st.columns(2); nom_l=c1.text_input("Nom du livrable / rue *"); tl=c2.selectbox("Type",["Rue","Voirie","Bâtiment","Ouvrage d'art","Lot","Autre"])
            c3,c4=st.columns(2); long=c3.number_input("Longueur (m)",min_value=0.0); larg=c4.number_input("Largeur (m)",min_value=0.0)
            desc=st.text_area("Description",height=60)
            if st.form_submit_button("💾 Ajouter"):
                if nom_l.strip():
                    lid=exsql("INSERT INTO livrables(chantier_id,nom,type_livrable,description,longueur_m,largeur_m)VALUES(?,?,?,?,?,?)",[rid,nom_l.strip(),tl,desc.strip(),long,larg])
                    audit("livrables","CREATE",lid,f"Livrable: {nom_l}"); st.success("✅ Ajouté."); st.rerun()
                else: st.error("Nom obligatoire.")
    with t2:
        df_l=get_livs(rid)
        if df_l.empty: st.info("Aucun livrable pour ce chantier.")
        else:
            sel_l=st.selectbox("Livrable",df_l["nom"].tolist(),key="liv_edit_sel"); rowl=df_l[df_l["nom"]==sel_l].iloc[0]
            with st.form("f_liv_edit"):
                c1,c2=st.columns(2); en=c1.text_input("Nom *",value=str(rowl["nom"])); tl_opts=["Rue","Voirie","Bâtiment","Ouvrage d'art","Lot","Autre"]; ct=str(rowl.get("type_livrable") or "Rue")
                et=c2.selectbox("Type",tl_opts,index=tl_opts.index(ct) if ct in tl_opts else 0)
                c3,c4=st.columns(2); elong=c3.number_input("Longueur",min_value=0.0,value=float(rowl.get("longueur_m") or 0)); elarg=c4.number_input("Largeur",min_value=0.0,value=float(rowl.get("largeur_m") or 0))
                edesc=st.text_area("Description",value=str(rowl.get("description") or ""),height=60)
                ca,cb=st.columns(2); sv=ca.form_submit_button("✅ Enregistrer"); dl=cb.form_submit_button("🗑️ Supprimer")
            if sv:
                exsql("UPDATE livrables SET nom=?,type_livrable=?,description=?,longueur_m=?,largeur_m=? WHERE id=?",[en,et,edesc,elong,elarg,int(rowl["id"])])
                st.success("✅ Modifié."); st.rerun()
            if dl:
                exsql("DELETE FROM livrables WHERE id=?",[int(rowl["id"])]); st.success("Supprimé."); st.rerun()
    with t3:
        df_l=get_livs(rid)
        if not df_l.empty: st.dataframe(df_l[["nom","type_livrable","longueur_m","largeur_m","description"]],use_container_width=True)
        else: st.info("Aucun livrable.")

# ── FICHE CHANTIER ────────────────────────────────────────────
elif page=="fiche_chantier":
    st.title("📋 Fiche de Chantier")
    labs,id_map=ch_label_map()
    if not labs: st.warning("Aucun chantier."); st.stop()
    sel=st.selectbox("Chantier",labs,key="fiche_sel"); rid=id_map.get(sel)
    row=qdf("SELECT * FROM rues WHERE id=?",[rid])
    if row.empty: st.stop()
    row=row.iloc[0]

    # ── Calcul délai ──────────────────────────────────────────────
    delai_j=int(row.get("delai_jours") or 0)
    dd_str=str(row.get("date_demarrage") or "")
    dn_str=str(row.get("date_notification") or "")
    cons,rest,pct=delai_cons(dd_str,delai_j)

    # Indicateurs délai
    st.markdown("### ⏱️ Situation du délai")
    m1,m2,m3,m4=st.columns(4)
    m1.metric("📅 Délai contractuel", f"{delai_j} j" if delai_j else "—")
    m2.metric("⏱️ Délai consommé", f"{cons} j",
              delta=f"{pct:.1f}% du délai" if delai_j else None,
              delta_color="inverse")
    if rest >= 0:
        m3.metric("🕐 Délai restant", f"{rest} j", delta="En cours ✅", delta_color="normal")
    else:
        m3.metric("🕐 Délai restant", f"{abs(rest)} j de dépassement",
                  delta="⚠️ Délai dépassé", delta_color="inverse")
    # Date de fin prévue
    if dd_str:
        try:
            dd_date=datetime.strptime(dd_str,"%Y-%m-%d").date()
            fin_prevue=dd_date+timedelta(days=delai_j)
            m4.metric("📆 Fin prévue", fin_prevue.strftime("%d/%m/%Y"))
        except: m4.metric("📆 Fin prévue","—")
    else:
        m4.metric("📆 Fin prévue","—")

    # Barre de progression
    if delai_j>0:
        color="🟢" if pct<75 else ("🟠" if pct<100 else "🔴")
        st.markdown(f"{color} **Avancement délai : {pct:.1f}%**")
        st.progress(min(pct/100, 1.0))
    st.markdown("---")

    # ── Formulaire de modification ─────────────────────────────────
    st.markdown("### ✏️ Modifier la fiche")

    # Dates avec calendrier (hors form pour permettre st.date_input conditionnel)
    st.markdown("#### 📅 Dates & Délai")
    fc1,fc2,fc3=st.columns(3)
    # Date notification
    try: v_dn=date.fromisoformat(dn_str) if dn_str else date.today()
    except: v_dn=date.today()
    dn_d=fc1.date_input("📬 Date de notification du marché", value=v_dn, key="fiche_dn")
    # Date démarrage
    try: v_dd=date.fromisoformat(dd_str) if dd_str else date.today()
    except: v_dd=date.today()
    dd_d=fc2.date_input("🚀 Date de démarrage des travaux", value=v_dd, key="fiche_dd")
    # Délai
    dj_new=fc3.number_input("⏳ Délai contractuel (jours)", min_value=0,
                             value=delai_j, step=1, key="fiche_dj")
    # Info délai en mois
    if dj_new > 0:
        fc3.caption(f"≈ {dj_new/30:.1f} mois")

    st.markdown("#### 👥 Intervenants")
    with st.form("f_fiche"):
        c1,c2=st.columns(2)
        mo=c1.text_input("Maître d'ouvrage", value=str(row.get("maitre_ouvrage") or ""))
        mod=c2.text_input("MOA délégué", value=str(row.get("maitre_ouvrage_delegue") or ""))
        c3,c4=st.columns(2)
        ent=c3.text_input("Entreprise", value=str(row.get("entreprise") or ""))
        bc=c4.text_input("Bureau de contrôle", value=str(row.get("bureau_controle") or ""))
        c5,c6=st.columns(2)
        lab=c5.text_input("Laboratoire", value=str(row.get("labo") or ""))
        cs=c6.text_input("Coordinateur sécurité", value=str(row.get("coordinateur_securite") or ""))
        if st.form_submit_button("💾 Enregistrer la fiche"):
            exsql("UPDATE rues SET maitre_ouvrage=?,maitre_ouvrage_delegue=?,entreprise=?,bureau_controle=?,labo=?,coordinateur_securite=?,date_notification=?,date_demarrage=?,delai_jours=? WHERE id=?",
                  [mo, mod, ent, bc, lab, cs, str(dn_d), str(dd_d), dj_new, rid])
            audit("rues","UPDATE",rid,"Fiche de chantier mise à jour")
            st.success("✅ Fiche enregistrée."); st.rerun()

# ── ORGANIGRAMME ──────────────────────────────────────────────
elif page=="organigramme":
    st.title("🏛️ Organigramme")
    labs,id_map=ch_label_map()
    if not labs: st.warning("Aucun chantier."); st.stop()
    sel=st.selectbox("Chantier",labs,key="org_sel"); rid=id_map.get(sel)
    row=qdf("SELECT * FROM rues WHERE id=?",[rid])
    if row.empty: st.stop()
    row=row.iloc[0]
    def box(l,v,cls=""):
        v=str(v) if v else "—"
        return f'<div class="orgchart-box {cls}" style="min-width:150px;"><b style="font-size:11px;color:#555">{l}</b><br><span style="font-size:13px;font-weight:bold">{v}</span></div>'
    st.markdown("##### Maîtrise d'ouvrage")
    box_mo=box("Maître d'ouvrage",row.get("maitre_ouvrage"))
    box_moad=box("MOA Délégué",row.get("maitre_ouvrage_delegue"))
    st.markdown(f'<div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px">{box_mo}{box_moad}</div>',unsafe_allow_html=True)
    st.markdown("▼")
    st.markdown("##### Contrôle")
    box_bc=box("Bureau de contrôle",row.get("bureau_controle"),"orgchart-box-orange")
    box_labo=box("Laboratoire",row.get("labo"),"orgchart-box-orange")
    box_csec=box("Coord. sécurité",row.get("coordinateur_securite"),"orgchart-box-orange")
    st.markdown(f'<div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px">{box_bc}{box_labo}{box_csec}</div>',unsafe_allow_html=True)
    st.markdown("▼")
    st.markdown("##### Entreprise principale")
    st.markdown(box("Entreprise",row.get("entreprise"),"orgchart-box-green"),unsafe_allow_html=True)
    df_st=qdf("SELECT nom,specialite,responsable FROM sous_traitants WHERE rue_id=? ORDER BY nom",[rid])
    if not df_st.empty:
        st.markdown("▼"); st.markdown("##### Sous-traitants")
        bs="".join([box(f"ST: {r['nom']}",f"{r.get('specialite','')}") for _,r in df_st.iterrows()])
        st.markdown(f'<div style="display:flex;gap:10px;flex-wrap:wrap">{bs}</div>',unsafe_allow_html=True)
    df_eff=qdf("SELECT categorie,COUNT(*) AS nb FROM personnel WHERE rue_id=? AND actif=1 GROUP BY categorie",[rid])
    if not df_eff.empty:
        st.markdown("---"); st.subheader("👷 Effectifs"); st.dataframe(df_eff,use_container_width=True,hide_index=True)
    df_livs=get_livs(rid)
    if not df_livs.empty:
        st.markdown("---"); st.subheader("📐 Livrables / Rues")
        st.dataframe(df_livs[["nom","type_livrable","longueur_m","largeur_m"]],use_container_width=True,hide_index=True)

# ── DEVIS DU MARCHÉ ───────────────────────────────────────────
elif page=="devis":
    st.title("📄 Devis du Marché")
    labs,id_map=ch_label_map()
    if not labs: st.warning("Aucun chantier."); st.stop()
    sel=st.selectbox("Chantier",labs,key="dev_ch"); rid=id_map.get(sel)
    df_livs=get_livs(rid)
    liv_opts=["(Tous livrables)"]+df_livs["nom"].tolist() if not df_livs.empty else ["(Tous livrables)"]
    sel_liv=st.selectbox("Livrable (optionnel)",liv_opts,key="dev_liv")
    lvid=None
    if sel_liv!="(Tous livrables)" and not df_livs.empty: lvid=int(df_livs[df_livs["nom"]==sel_liv].iloc[0]["id"])
    tv,ta,te,ti=st.tabs(["👁️ Visualiser","➕ Ajouter","✏️ Modifier","📥 Import Excel"])
    with tv:
        # ── Mode récapitulatif : tous les livrables ──────────────────
        if sel_liv=="(Tous livrables)" and not df_livs.empty:
            st.subheader("📊 Tableau récapitulatif du marché — tous livrables")
            # Charger tous les postes avec leur livrable
            df_all=qdf("""
                SELECT dr.code_poste, dr.designation, dr.unite, dr.prix_unitaire,
                       dr.quantite_marche,
                       COALESCE(l.nom,'Sans livrable') AS livrable
                FROM devis_rue dr
                LEFT JOIN livrables l ON l.id=dr.livrable_id
                WHERE dr.rue_id=?
                ORDER BY dr.id
            """,[rid])
            if df_all.empty:
                st.info("Aucun poste. Ajoutez des postes par livrable ou importez depuis Excel.")
            else:
                # Pivot : une colonne de quantité par livrable
                livrables_noms=df_all["livrable"].unique().tolist()

                # ── Clé de regroupement ────────────────────────────────
                # Priorité 1 : code_poste non vide  → clé = code_poste normalisé
                # Priorité 2 : designation + unite normalisés (on ignore le PU
                #              car il peut légèrement différer entre livrables)
                def _make_key(row):
                    code=str(row["code_poste"]).strip()
                    if code and code not in ("","nan","None"):
                        return code.lower()
                    desig=str(row["designation"]).strip().lower()
                    unite=str(row["unite"]).strip().lower()
                    return desig+"|"+unite

                df_all["_key"]=df_all.apply(_make_key,axis=1)

                # Pour chaque clé, on prend la première valeur de code/desig/unite/PU
                # (on agrège par clé pour éviter les doublons dans le même livrable)
                agg_meta=df_all.groupby("_key",sort=False).agg(
                    code_poste=("code_poste","first"),
                    designation=("designation","first"),
                    unite=("unite","first"),
                    prix_unitaire=("prix_unitaire","first")
                ).reset_index()

                recap=agg_meta.copy()

                for lnom in livrables_noms:
                    sub=(df_all[df_all["livrable"]==lnom]
                         .groupby("_key",sort=False)["quantite_marche"]
                         .sum()
                         .reset_index()
                         .rename(columns={"quantite_marche":lnom}))
                    recap=recap.merge(sub,on="_key",how="left")
                    recap[lnom]=recap[lnom].fillna(0)

                recap["Qté_TOTAL"]=recap[[l for l in livrables_noms]].sum(axis=1)
                recap["Montant_TOTAL"]=recap["Qté_TOTAL"]*recap["prix_unitaire"]
                recap=recap.drop(columns=["_key"])
                recap=recap.rename(columns={"code_poste":"Code","designation":"Désignation","unite":"Unité","prix_unitaire":"PU"})

                # Ligne de total
                total_row={"Code":"","Désignation":"TOTAL MARCHÉ","Unité":"","PU":""}
                for lnom in livrables_noms: total_row[lnom]=""
                total_row["Qté_TOTAL"]=""
                total_row["Montant_TOTAL"]=recap["Montant_TOTAL"].sum()
                recap_display=pd.concat([recap,pd.DataFrame([total_row])],ignore_index=True)
                # Affichage
                col_a,col_b=st.columns(2)
                col_a.metric("💰 Montant total marché",fmt(recap["Montant_TOTAL"].sum()))
                col_b.metric("📋 Nombre de postes",len(recap))
                st.dataframe(recap_display,use_container_width=True,hide_index=True)
                # Export
                st.download_button("📥 Export Excel — Récapitulatif",
                    to_xl({"Récapitulatif":recap_display}),
                    f"recap_marche_{sel.replace('[','').replace(']','').replace(' ','_')}.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="dev_export_recap")

        else:
            # ── Mode livrable unique ──────────────────────────────────
            df_d=get_devis(rid,lvid)
            if df_d.empty: st.info("Aucun poste. Ajoutez des postes ou importez depuis Excel.")
            else:
                df_d["Montant"]=df_d["quantite_marche"]*df_d["prix_unitaire"]
                st.metric("💰 Montant livrable",fmt(df_d["Montant"].sum()))
                st.dataframe(df_d[["code_poste","designation","unite","quantite_marche","prix_unitaire","Montant"]].rename(columns={"code_poste":"Code","designation":"Désignation","unite":"Unité","quantite_marche":"Qté","prix_unitaire":"PU","Montant":"Montant"}),use_container_width=True)
                st.download_button("📥 Export Excel",to_xl({"Devis":df_d}),f"devis_{sel_liv.replace(' ','_')}.xlsx","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",key="dev_export_liv")
    with ta:
        c1,c2=st.columns([1,3]); code=c1.text_input("Code",key="dev_add_code"); desig=c2.text_input("Désignation *",key="dev_add_desig")
        c3,c4,c5=st.columns(3)
        un=c3.selectbox("Unité *",UNITES_MESURE,key="dev_add_un_sel")
        if un=="Autre...": un=st.text_input("Préciser l'unité",key="dev_add_un_custom",placeholder="ex: pl, barre...")
        q=c4.number_input("Qté",min_value=0.0,key="dev_add_q"); pu=c5.number_input("PU",min_value=0.0,key="dev_add_pu")
        obs=st.text_input("Observation",key="dev_add_obs")
        if st.button("💾 Ajouter",key="dev_add_btn"):
            if desig.strip() and un and un not in ("","Autre..."):
                pid=exsql("INSERT INTO devis_rue(rue_id,livrable_id,code_poste,designation,unite,quantite_marche,prix_unitaire,observation)VALUES(?,?,?,?,?,?,?,?)",[rid,lvid,code.strip(),desig.strip(),un.strip(),q,pu,obs.strip()])
                audit("devis_rue","CREATE",pid,f"Ajout: {desig}"); st.success("✅ Poste ajouté."); st.rerun()
            else: st.error("Désignation et Unité obligatoires.")
    with te:
        df_d=get_devis(rid,lvid)
        if df_d.empty: st.info("Aucun poste.")
        else:
            labs_d=df_d.apply(lambda r:f"{r.get('code_poste','')} — {r['designation']}",axis=1).tolist()
            sel_p=st.selectbox("Poste",labs_d,key="dev_edit_p"); idx=labs_d.index(sel_p); p=df_d.iloc[idx]
            c1,c2=st.columns([1,3]); ec=c1.text_input("Code",value=str(p.get("code_poste") or ""),key="dev_e_code"); ed=c2.text_input("Désignation",value=str(p["designation"]),key="dev_e_desig")
            c3,c4,c5=st.columns(3)
            cur_un=str(p["unite"]); eu_opts=UNITES_MESURE if cur_un in UNITES_MESURE else [cur_un]+UNITES_MESURE
            eu_sel=c3.selectbox("Unité",eu_opts,index=eu_opts.index(cur_un) if cur_un in eu_opts else 0,key="dev_e_un_sel")
            if eu_sel=="Autre...": eu=st.text_input("Préciser l'unité",value=cur_un if cur_un not in UNITES_MESURE else "",key="dev_e_un_custom")
            else: eu=eu_sel
            eq=c4.number_input("Qté",min_value=0.0,value=float(p["quantite_marche"] or 0),key="dev_e_q")
            ep=c5.number_input("PU",min_value=0.0,value=float(p["prix_unitaire"] or 0),key="dev_e_pu")
            eobs=st.text_input("Observation",value=str(p.get("observation") or ""),key="dev_e_obs")
            ca,cb=st.columns(2); sv=ca.button("✅ Enregistrer",key="dev_e_sv"); dl=cb.button("🗑️ Supprimer",key="dev_e_dl")
            if sv:
                exsql("UPDATE devis_rue SET code_poste=?,designation=?,unite=?,quantite_marche=?,prix_unitaire=?,observation=? WHERE id=?",[ec,ed,eu,eq,ep,eobs,int(p["id"])])
                audit("devis_rue","UPDATE",int(p["id"]),f"Modif: {ed}"); st.success("✅ Modifié."); st.rerun()
            if dl:
                exsql("DELETE FROM devis_rue WHERE id=?",[int(p["id"])]); st.success("Supprimé."); st.rerun()
    with ti:
        # ── Deux modes d'import selon le contexte ──────────────────
        if sel_liv=="(Tous livrables)":
            # ════════════════════════════════════════════════════════
            # MODE GLOBAL : import du devis récapitulatif du marché
            # ════════════════════════════════════════════════════════
            st.info("ℹ️ **Import du devis global du marché.** Deux formats acceptés :\n\n"
                    "**Format A — Simple** : `Code | Désignation | Unité | Quantité | Prix Unitaire`  \n"
                    "→ Lignes importées au niveau chantier (sans livrable)\n\n"
                    "**Format B — Multi-livrables** : `Code | Désignation | Unité | Prix Unitaire | Livrable1 | Livrable2 | …`  \n"
                    "→ Une colonne par livrable avec ses quantités, import automatique par livrable")
            up_g=st.file_uploader("Fichier Excel du devis global",type=["xlsx","xls"],key="dev_global_import")
            rep_g=st.checkbox("Remplacer le devis existant du chantier",key="dev_global_rep")
            if up_g:
                try:
                    df_g=read_excel_smart(up_g)
                    col_map_g={
                        "code":["code","code_poste","n_","num","item","ref","reference","no","numero"],
                        "desig":["designation","libelle","intitule","description","poste","ouvrage","travaux","prestation","nature","article","tache","objet","libelle_travaux"],
                        "unite":["unite","unit","u","unites","mesure","un"],
                        "qte":["quantite_marche","quantite","qte","q","qt","volume","quantites","total","qte_total"],
                        "pu":["prix_unitaire","pu","prix","cout_unitaire","tarif","montant_unitaire","pu_ht"]
                    }
                    res_g={}
                    for k,als in col_map_g.items(): res_g[k]=find_col(set(df_g.columns),als)
                    if not res_g["desig"] or not res_g["unite"]:
                        st.error(f"❌ Colonnes Désignation/Unité introuvables.\n\nColonnes détectées : `{', '.join(sorted(df_g.columns.tolist()))}`")
                        st.stop()

                    # ── Détecter si le fichier est multi-livrables ────────
                    # Les colonnes "livrable" sont celles qui ne correspondent
                    # à aucun champ standard (code/desig/unite/qte/pu/montant)
                    cols_standard={res_g[k] for k in res_g if res_g[k]}
                    cols_standard.update(["montant","total","montant_total","montant_ht"])
                    livs_cols_excel=[c for c in df_g.columns if c not in cols_standard and c not in ("nan","none","")]

                    # Récupérer les livrables existants du chantier
                    df_livs_ex=get_livs(rid)
                    liv_name_id={str(r["nom"]).strip().lower():int(r["id"]) for _,r in df_livs_ex.iterrows()} if not df_livs_ex.empty else {}

                    # Chercher des correspondances entre colonnes Excel et livrables existants
                    matched_livs={}  # col_excel → livrable_id
                    for col in livs_cols_excel:
                        col_norm=_norm_str(col)
                        for lnom,lid in liv_name_id.items():
                            if col_norm in _norm_str(lnom) or _norm_str(lnom) in col_norm:
                                matched_livs[col]=lid; break

                    is_multi=len(matched_livs)>0

                    if is_multi:
                        st.success(f"✅ Format **multi-livrables** détecté — {len(matched_livs)} livrable(s) reconnu(s) : **{', '.join(matched_livs.keys())}**")
                        # Aperçu
                        cols_show=["code_poste" if res_g["code"] else None, res_g["desig"], res_g["unite"]]
                        if res_g["pu"]: cols_show.append(res_g["pu"])
                        cols_show=[c for c in cols_show if c]+list(matched_livs.keys())
                        st.dataframe(df_g[cols_show].dropna(subset=[res_g["desig"]]).head(20),use_container_width=True)
                        st.caption(f"Aperçu limité à 20 lignes — {len(df_g)} lignes détectées au total")
                        if st.button("✅ Confirmer l'import multi-livrables",key="dev_multi_ok"):
                            if rep_g: exsql("DELETE FROM devis_rue WHERE rue_id=?",[rid])
                            total_rows=0
                            for col_excel,liv_id in matched_livs.items():
                                rows_lv=[]
                                for _,r in df_g.iterrows():
                                    desig_val=str(r[res_g["desig"]]).strip()
                                    if not desig_val or desig_val in ("nan","None",""): continue
                                    code_val=str(r[res_g["code"]]).strip() if res_g["code"] else ""
                                    unite_val=str(r[res_g["unite"]]).strip() if res_g["unite"] else ""
                                    pu_val=float(pd.to_numeric(r[res_g["pu"]],errors="coerce") or 0) if res_g["pu"] else 0.0
                                    qte_val=float(pd.to_numeric(r[col_excel],errors="coerce") or 0)
                                    if qte_val==0: continue  # ne pas importer les lignes à zéro
                                    rows_lv.append((rid,liv_id,code_val,desig_val,unite_val,qte_val,pu_val,""))
                                if rows_lv:
                                    exmany("INSERT INTO devis_rue(rue_id,livrable_id,code_poste,designation,unite,quantite_marche,prix_unitaire,observation)VALUES(?,?,?,?,?,?,?,?)",rows_lv)
                                    total_rows+=len(rows_lv)
                            audit("devis_rue","IMPORT_MULTI",rid,f"{total_rows} postes / {len(matched_livs)} livrables")
                            st.success(f"✅ {total_rows} postes importés sur {len(matched_livs)} livrables."); st.rerun()
                    else:
                        # Format simple → import au niveau chantier (livrable_id=NULL)
                        st.info("📋 Format **simple** détecté — import au niveau chantier (sans livrable).")
                        prev_g=pd.DataFrame({
                            "Code": df_g[res_g["code"]] if res_g["code"] else "",
                            "Désignation": df_g[res_g["desig"]],
                            "Unité": df_g[res_g["unite"]],
                            "Qté": pd.to_numeric(df_g[res_g["qte"]] if res_g["qte"] else 0,errors="coerce").fillna(0),
                            "PU": pd.to_numeric(df_g[res_g["pu"]] if res_g["pu"] else 0,errors="coerce").fillna(0)
                        }).dropna(subset=["Désignation"])
                        prev_g=prev_g[prev_g["Désignation"].astype(str).str.strip().isin(["","nan"])==False]
                        prev_g["Montant"]=prev_g["Qté"]*prev_g["PU"]
                        st.dataframe(prev_g,use_container_width=True)
                        st.metric("💰 Total",fmt(prev_g["Montant"].sum()))
                        if st.button("✅ Confirmer l'import global",key="dev_glob_ok"):
                            if rep_g: exsql("DELETE FROM devis_rue WHERE rue_id=?",[rid])
                            rows_g=[(rid,None,str(r["Code"]).strip(),str(r["Désignation"]).strip(),str(r["Unité"]).strip(),float(r["Qté"]),float(r["PU"]),"") for _,r in prev_g.iterrows() if str(r["Désignation"]).strip()]
                            exmany("INSERT INTO devis_rue(rue_id,livrable_id,code_poste,designation,unite,quantite_marche,prix_unitaire,observation)VALUES(?,?,?,?,?,?,?,?)",rows_g)
                            audit("devis_rue","IMPORT_GLOBAL",rid,f"{len(rows_g)} postes")
                            st.success(f"✅ {len(rows_g)} postes importés au niveau chantier."); st.rerun()
                except Exception as e: st.error(f"Erreur lors de la lecture du fichier : {e}")
        else:
            # ════════════════════════════════════════════════════════
            # MODE LIVRABLE : import pour un livrable spécifique
            # ════════════════════════════════════════════════════════
            st.markdown(f"**Import pour le livrable : {sel_liv}**")
            st.caption("Format attendu : Code | Désignation | Unité | Quantité | Prix Unitaire")
            up=st.file_uploader("Fichier Excel",type=["xlsx","xls"],key="dev_import")
            rep=st.checkbox("Remplacer le devis de ce livrable",key="dev_liv_rep")
            if up:
                try:
                    df_i=read_excel_smart(up)
                    col_map={
                        "code":["code","code_poste","n_","num","item","ref","reference","no","numero"],
                        "desig":["designation","libelle","intitule","description","poste","ouvrage","travaux","prestation","nature","article","tache","objet","libelle_travaux"],
                        "unite":["unite","unit","u","unites","mesure","un"],
                        "qte":["quantite_marche","quantite","qte","q","qt","volume","quantites"],
                        "pu":["prix_unitaire","pu","prix","cout_unitaire","tarif","montant_unitaire","pu_ht"]
                    }
                    res={}
                    for k,als in col_map.items(): res[k]=find_col(set(df_i.columns),als)
                    if not res["desig"] or not res["unite"]:
                        cols_trouves=", ".join(sorted(df_i.columns.tolist()))
                        manquantes=[]
                        if not res["desig"]: manquantes.append("Désignation")
                        if not res["unite"]: manquantes.append("Unité")
                        st.error(f"❌ Colonnes introuvables : **{', '.join(manquantes)}**\n\n"
                                 f"Colonnes détectées : `{cols_trouves}`\n\n"
                                 f"Renommez avec : **Code | Désignation | Unité | Quantité | Prix Unitaire**")
                        st.stop()
                    prev=pd.DataFrame({"Code":df_i[res["code"]] if res["code"] else "","Désignation":df_i[res["desig"]],"Unité":df_i[res["unite"]],"Qté":pd.to_numeric(df_i[res["qte"]] if res["qte"] else 0,errors="coerce").fillna(0),"PU":pd.to_numeric(df_i[res["pu"]] if res["pu"] else 0,errors="coerce").fillna(0)}).dropna(subset=["Désignation"])
                    prev["Montant"]=prev["Qté"]*prev["PU"]
                    prev=prev[prev["Désignation"].astype(str).str.strip().isin(["","nan"])==False]
                    st.dataframe(prev,use_container_width=True); st.metric("Total",fmt(prev["Montant"].sum()))
                    if st.button("✅ Confirmer l'import",key="dev_imp_ok"):
                        if rep: exsql("DELETE FROM devis_rue WHERE rue_id=? AND livrable_id=?",[rid,lvid])
                        rows=[(rid,lvid,str(r["Code"]).strip(),str(r["Désignation"]).strip(),str(r["Unité"]).strip(),float(r["Qté"]),float(r["PU"]),"") for _,r in prev.iterrows() if str(r["Désignation"]).strip()]
                        exmany("INSERT OR REPLACE INTO devis_rue(rue_id,livrable_id,code_poste,designation,unite,quantite_marche,prix_unitaire,observation)VALUES(?,?,?,?,?,?,?,?)",rows)
                        audit("devis_rue","IMPORT",rid,f"{len(rows)} postes livrable {sel_liv}"); st.success(f"✅ {len(rows)} postes importés."); st.rerun()
                except Exception as e: st.error(f"Erreur: {e}")

# ── DÉCOMPTE TRAVAUX ──────────────────────────────────────────
elif page=="decompte":
    st.title("📊 Décompte des Travaux")

    # ─── Niveau 1 : Marché / Dossier ───────────────────────────────
    df_dos = get_dos()
    dos_opts = ["(Tous les marchés)"] + df_dos["nom"].tolist() if not df_dos.empty else ["(Tous les marchés)"]
    sel_dos = st.selectbox("📁 Marché / Dossier", dos_opts, key="dec_dos")
    did_filter = int(df_dos[df_dos["nom"]==sel_dos].iloc[0]["id"]) if sel_dos != "(Tous les marchés)" and not df_dos.empty else None

    df_rues_dec = get_rues(did_filter)
    if df_rues_dec.empty:
        st.warning("Aucune rue/chantier dans ce marché."); st.stop()

    tab_saisie, tab_rue, tab_global = st.tabs(["📝 Saisie par rue / livrable","📊 Décompte par rue","🏗️ Décompte global marché"])

    # ─── Onglet Saisie réalisations ────────────────────────────────
    with tab_saisie:
        # Niveau 2 : Rue / Chantier
        rue_opts = df_rues_dec["nom"].tolist()
        sel_rue_s = st.selectbox("🛣️ Rue / Chantier", rue_opts, key="dec_rue_saisie")
        rid_s = int(df_rues_dec[df_rues_dec["nom"]==sel_rue_s].iloc[0]["id"])

        # Niveau 3 : Livrable (optionnel)
        df_livs_s = get_livs(rid_s)
        lvid_s = None
        if not df_livs_s.empty:
            liv_opts_s = ["(Tous les livrables)"] + df_livs_s["nom"].tolist()
            sel_liv_s = st.selectbox("📐 Livrable (optionnel)", liv_opts_s, key="dec_liv_saisie")
            if sel_liv_s != "(Tous les livrables)":
                lvid_s = int(df_livs_s[df_livs_s["nom"]==sel_liv_s].iloc[0]["id"])

        dd = st.date_input("Date", value=date.today(), key="dec_date")
        df_d = get_devis(rid_s, lvid_s)
        if df_d.empty:
            msg = f"Aucun poste de devis pour **{sel_rue_s}**"
            if lvid_s: msg += f" / livrable **{sel_liv_s}**"
            st.info(msg + ". Créez le cadre de devis d'abord (menu Devis du marché).")
        else:
            caption = f"Cadre de devis — **{sel_rue_s}**"
            if lvid_s: caption += f" › {sel_liv_s}"
            caption += f" : {len(df_d)} postes"
            st.caption(caption)
            with st.form("f_dec"):
                qts = {}
                for _, p in df_d.iterrows():
                    qts[int(p["id"])] = st.number_input(
                        f"{p.get('code_poste','')} — {p['designation']} ({p['unite']})",
                        min_value=0.0, key=f"dq_{p['id']}"
                    )
                obs_d = st.text_input("Observation", key="dec_obs")
                if st.form_submit_button("💾 Enregistrer"):
                    for did, q in qts.items():
                        if q > 0:
                            exsql("INSERT INTO realisations_journalieres(date_suivi,rue_id,devis_id,quantite_jour,observation)VALUES(?,?,?,?,?)",
                                  [str(dd), rid_s, did, q, obs_d])
                    audit("realisations_journalieres", "CREATE", rid_s, f"Réalisations {dd}")
                    st.success("✅ Enregistré."); st.rerun()

    # ─── Onglet Décompte d'une rue ─────────────────────────────────
    with tab_rue:
        rue_opts2 = df_rues_dec["nom"].tolist()
        sel_rue_r = st.selectbox("🛣️ Rue / Chantier", rue_opts2, key="dec_rue_recap")
        rid_r = int(df_rues_dec[df_rues_dec["nom"]==sel_rue_r].iloc[0]["id"])

        # Filtre livrable
        df_livs_r = get_livs(rid_r)
        lvid_r = None
        if not df_livs_r.empty:
            liv_opts_r = ["(Tous les livrables)"] + df_livs_r["nom"].tolist()
            sel_liv_r = st.selectbox("📐 Livrable (optionnel)", liv_opts_r, key="dec_liv_recap")
            if sel_liv_r != "(Tous les livrables)":
                lvid_r = int(df_livs_r[df_livs_r["nom"]==sel_liv_r].iloc[0]["id"])

        df_d2 = get_devis(rid_r, lvid_r)
        if df_d2.empty:
            st.info(f"Aucun poste de devis pour **{sel_rue_r}**.")
        else:
            rows = []
            for _, p in df_d2.iterrows():
                r2 = qdf("SELECT COALESCE(SUM(quantite_jour),0) AS qe FROM realisations_journalieres WHERE devis_id=?", [int(p["id"])])
                qe = float(_v(r2, "qe")); qm = float(p["quantite_marche"] or 0); pu = float(p["prix_unitaire"] or 0)
                rows.append({
                    "Code": p.get("code_poste",""), "Désignation": p["designation"],
                    "Unité": p["unite"], "Qté marché": qm, "Qté exécutée": qe,
                    "Reste": max(0, qm-qe), "Taux %": round(qe/qm*100, 1) if qm else 0,
                    "Montant marché": qm*pu, "Montant exécuté": qe*pu
                })
            df_rec = pd.DataFrame(rows); tm = df_rec["Montant marché"].sum(); te = df_rec["Montant exécuté"].sum()
            c1, c2, c3 = st.columns(3)
            c1.metric("Marché", fmt(tm)); c2.metric("Exécuté", fmt(te)); c3.metric("Taux", fpct(te/tm*100 if tm else 0))
            st.progress(min(te/tm, 1.0) if tm else 0)
            st.dataframe(df_rec, use_container_width=True)
            export_name = f"{sel_rue_r}" + (f"_{sel_liv_r}" if lvid_r else "")
            st.download_button("📥 Export", to_xl({f"Décompte": df_rec}),
                               f"decompte_{export_name.replace(' ','_')}.xlsx",
                               "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # ─── Onglet Décompte global (cumul toutes rues du marché) ───────
    with tab_global:
        st.subheader("🏗️ Récapitulatif global du marché")
        rows_glob = []; tm_total = 0.0; te_total = 0.0
        for _, rue_row in df_rues_dec.iterrows():
            r_id = int(rue_row["id"])
            df_dv = get_devis(r_id)
            if df_dv.empty: continue
            tm_r = float((df_dv["quantite_marche"] * df_dv["prix_unitaire"]).sum())
            te_r = 0.0
            for _, p in df_dv.iterrows():
                r2 = qdf("SELECT COALESCE(SUM(quantite_jour),0) AS qe FROM realisations_journalieres WHERE devis_id=?", [int(p["id"])])
                te_r += float(_v(r2, "qe")) * float(p["prix_unitaire"] or 0)
            tm_total += tm_r; te_total += te_r
            rows_glob.append({
                "Rue / Chantier": rue_row["nom"],
                "Montant marché": tm_r,
                "Montant exécuté": te_r,
                "Reste à exécuter": max(0, tm_r - te_r),
                "Taux %": round(te_r/tm_r*100, 1) if tm_r else 0
            })
        if rows_glob:
            c1, c2, c3 = st.columns(3)
            c1.metric("Total marché", fmt(tm_total))
            c2.metric("Total exécuté", fmt(te_total))
            c3.metric("Taux global", fpct(te_total/tm_total*100 if tm_total else 0))
            st.progress(min(te_total/tm_total, 1.0) if tm_total else 0)
            df_glob = pd.DataFrame(rows_glob)
            st.dataframe(df_glob, use_container_width=True)
            st.download_button("📥 Export global", to_xl({"Décompte global": df_glob}),
                               "decompte_global.xlsx",
                               "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            st.info("Aucun poste de devis trouvé dans ce marché. Créez d'abord les cadres de devis.")


# ── SOUS-TRAITANTS ────────────────────────────────────────────
elif page=="sts":
    st.title("🤝 Sous-traitants")
    labs,id_map=ch_label_map()
    t1,t2,t3=st.tabs(["➕ Nouveau","✏️ Modifier","📋 Liste"])
    with t1:
        with st.form("f_st_add"):
            c1,c2=st.columns(2); nom_s=c1.text_input("Nom *"); spec=c2.text_input("Spécialité")
            c3,c4=st.columns(2); resp=c3.text_input("Responsable"); tel=c4.text_input("Téléphone")
            email=st.text_input("Email")
            c5,c6=st.columns(2); mont=c5.number_input("Montant contrat",min_value=0.0); st_s=c6.selectbox("Statut",["Actif","Terminé","Suspendu"])
            c7,c8=st.columns(2)
            deb_s=c7.date_input("Date début",value=date.today(),key="st_deb")
            fin_s=c8.date_input("Date fin",value=date.today(),key="st_fin")
            ch_st=st.selectbox("Chantier affecté",["(Non affecté)"]+labs,key="st_ch_add")
            obs_s=st.text_area("Observation",height=60)
            if st.form_submit_button("💾 Enregistrer"):
                if nom_s.strip():
                    rid_s=id_map.get(ch_st) if ch_st!="(Non affecté)" else None
                    try:
                        sid=exsql("INSERT INTO sous_traitants(nom,specialite,responsable,telephone,email,montant_contrat,date_debut,date_fin,statut,rue_id,observation)VALUES(?,?,?,?,?,?,?,?,?,?,?)",[nom_s.strip(),spec.strip(),resp.strip(),tel.strip(),email.strip(),mont,str(deb_s),str(fin_s),st_s,rid_s,obs_s.strip()])
                        audit("sous_traitants","CREATE",sid,f"Nouveau ST: {nom_s}"); st.success("✅ Enregistré."); st.rerun()
                    except Exception as e: st.error(f"Erreur: {e}")
                else: st.error("Nom obligatoire.")
    with t2:
        df_st=get_sts()
        if df_st.empty: st.info("Aucun sous-traitant.")
        else:
            sel_s=st.selectbox("Sous-traitant à modifier",df_st["nom"].tolist(),key="st_edit_sel"); rs=df_st[df_st["nom"]==sel_s].iloc[0]
            with st.form("f_st_edit"):
                c1,c2=st.columns(2); en=c1.text_input("Nom *",value=str(rs["nom"])); es=c2.text_input("Spécialité",value=str(rs.get("specialite") or ""))
                c3,c4=st.columns(2); er=c3.text_input("Responsable",value=str(rs.get("responsable") or "")); et=c4.text_input("Téléphone",value=str(rs.get("telephone") or ""))
                ee=st.text_input("Email",value=str(rs.get("email") or ""))
                c5,c6=st.columns(2); em=c5.number_input("Montant contrat",min_value=0.0,value=float(rs.get("montant_contrat") or 0))
                st_opts=["Actif","Terminé","Suspendu"]; cs=str(rs.get("statut") or "Actif")
                est=c6.selectbox("Statut",st_opts,index=st_opts.index(cs) if cs in st_opts else 0)
                c7,c8=st.columns(2)
                try: dv_deb=datetime.strptime(str(rs.get("date_debut") or date.today()),"%Y-%m-%d").date()
                except: dv_deb=date.today()
                try: dv_fin=datetime.strptime(str(rs.get("date_fin") or date.today()),"%Y-%m-%d").date()
                except: dv_fin=date.today()
                ed=c7.date_input("Date début",value=dv_deb,key="st_edit_deb"); ef=c8.date_input("Date fin",value=dv_fin,key="st_edit_fin")
                eobs=st.text_area("Observation",value=str(rs.get("observation") or ""),height=60)
                ca,cb=st.columns(2); sv=ca.form_submit_button("✅ Enregistrer"); dl=cb.form_submit_button("🗑️ Supprimer")
            if sv:
                exsql("UPDATE sous_traitants SET nom=?,specialite=?,responsable=?,telephone=?,email=?,montant_contrat=?,date_debut=?,date_fin=?,statut=?,observation=? WHERE id=?",[en,es,er,et,ee,em,str(ed),str(ef),est,eobs,int(rs["id"])])
                audit("sous_traitants","UPDATE",int(rs["id"]),f"Modif ST {en}"); st.success("✅ Modifié."); st.rerun()
            if dl:
                exsql("DELETE FROM sous_traitants WHERE id=?",[int(rs["id"])]); st.success("Supprimé."); st.rerun()
    with t3:
        df_list_s=qdf("SELECT s.nom AS ST,s.specialite AS Spécialité,COALESCE(r.nom,'—') AS Chantier,s.montant_contrat AS Contrat,COALESCE(SUM(p.montant),0) AS Payé,s.montant_contrat-COALESCE(SUM(p.montant),0) AS Solde,s.statut AS Statut FROM sous_traitants s LEFT JOIN rues r ON r.id=s.rue_id LEFT JOIN paiements_st p ON p.st_id=s.id GROUP BY s.id ORDER BY s.nom")
        if not df_list_s.empty: st.dataframe(df_list_s,use_container_width=True); st.download_button("📥 Export",to_xl({"ST":df_list_s}),"sous_traitants.xlsx","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ── DEVIS ST — import Excel + modification complète ───────────
elif page=="devis_st":
    st.title("📝 Devis Sous-traitant")
    df_st=get_sts()
    if df_st.empty: st.warning("Créez d'abord un sous-traitant."); st.stop()
    sel_s=st.selectbox("Sous-traitant",df_st["nom"].tolist(),key="dst_sel")
    st_id=int(df_st[df_st["nom"]==sel_s].iloc[0]["id"])
    tv,ta,te,ti=st.tabs(["👁️ Visualiser","➕ Ajouter","✏️ Modifier / Supprimer","📥 Import Excel"])
    with tv:
        df_d=qdf("SELECT * FROM devis_st WHERE st_id=? ORDER BY id",[st_id])
        if df_d.empty: st.info("Aucun poste. Ajoutez des postes ou importez depuis Excel.")
        else:
            df_d["Montant"]=df_d["quantite"]*df_d["prix_unitaire"]
            st.metric("💰 Total devis ST",fmt(df_d["Montant"].sum()))
            st.dataframe(df_d[["code_poste","designation","unite","quantite","prix_unitaire","Montant"]].rename(columns={"code_poste":"Code","designation":"Désignation","unite":"Unité","quantite":"Qté","prix_unitaire":"PU"}),use_container_width=True)
            st.download_button("📥 Export Excel",to_xl({"Devis ST":df_d}),f"devis_st_{sel_s}.xlsx","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    with ta:
        c1,c2=st.columns([1,3]); code=c1.text_input("Code",key="dst_add_code"); desig=c2.text_input("Désignation *",key="dst_add_desig")
        c3,c4,c5=st.columns(3)
        un_dst=c3.selectbox("Unité *",UNITES_MESURE,key="dst_add_un_sel")
        if un_dst=="Autre...": un_dst=st.text_input("Préciser l'unité",key="dst_add_un_custom",placeholder="ex: pl, barre...")
        q=c4.number_input("Quantité",min_value=0.0,key="dst_add_q"); pu=c5.number_input("Prix unitaire",min_value=0.0,key="dst_add_pu")
        obs=st.text_input("Observation",key="dst_add_obs")
        if st.button("💾 Ajouter",key="dst_add_btn"):
                if desig.strip() and un_dst and un_dst not in ("","Autre..."):
                    exsql("INSERT INTO devis_st(st_id,code_poste,designation,unite,quantite,prix_unitaire,observation)VALUES(?,?,?,?,?,?,?)",[st_id,code.strip(),desig.strip(),un_dst.strip(),q,pu,obs.strip()])
                    audit("devis_st","CREATE",st_id,f"Poste: {desig}"); st.success("✅ Ajouté."); st.rerun()
                else: st.error("Désignation et Unité obligatoires.")
    with te:
        df_d=qdf("SELECT * FROM devis_st WHERE st_id=? ORDER BY id",[st_id])
        if df_d.empty: st.info("Aucun poste à modifier.")
        else:
            labs_d=df_d.apply(lambda r:f"{r.get('code_poste','')} — {r['designation']}",axis=1).tolist()
            sel_p=st.selectbox("Poste à modifier",labs_d,key="dst_edit_p"); idx=labs_d.index(sel_p); p=df_d.iloc[idx]
            c1,c2=st.columns([1,3]); ec=c1.text_input("Code",value=str(p.get("code_poste") or ""),key="dst_e_code"); ed=c2.text_input("Désignation",value=str(p["designation"]),key="dst_e_desig")
            c3,c4,c5=st.columns(3)
            cur_un_dst=str(p["unite"]); eu_dst_opts=UNITES_MESURE if cur_un_dst in UNITES_MESURE else [cur_un_dst]+UNITES_MESURE
            eu_dst_sel=c3.selectbox("Unité",eu_dst_opts,index=eu_dst_opts.index(cur_un_dst) if cur_un_dst in eu_dst_opts else 0,key="dst_e_un_sel")
            if eu_dst_sel=="Autre...": eu_dst=st.text_input("Préciser l'unité",value=cur_un_dst if cur_un_dst not in UNITES_MESURE else "",key="dst_e_un_custom")
            else: eu_dst=eu_dst_sel
            eq=c4.number_input("Quantité",min_value=0.0,value=float(p["quantite"] or 0),key="dst_e_q")
            ep=c5.number_input("PU",min_value=0.0,value=float(p["prix_unitaire"] or 0),key="dst_e_pu")
            eobs=st.text_input("Observation",value=str(p.get("observation") or ""),key="dst_e_obs")
            ca,cb=st.columns(2); sv=ca.button("✅ Enregistrer",key="dst_e_sv"); dl=cb.button("🗑️ Supprimer",key="dst_e_dl")
            if sv:
                exsql("UPDATE devis_st SET code_poste=?,designation=?,unite=?,quantite=?,prix_unitaire=?,observation=? WHERE id=?",[ec,ed,eu_dst,eq,ep,eobs,int(p["id"])])
                audit("devis_st","UPDATE",int(p["id"]),f"Modif: {ed}"); st.success("✅ Modifié."); st.rerun()
            if dl:
                exsql("DELETE FROM devis_st WHERE id=?",[int(p["id"])]); st.success("Supprimé."); st.rerun()
    with ti:
        st.markdown("**Format attendu :** Code | Désignation | Unité | Quantité | Prix Unitaire")
        up=st.file_uploader("Fichier Excel (.xlsx/.xls)",type=["xlsx","xls"],key="dst_import")
        rep=st.checkbox("Remplacer le devis existant de ce ST",key="dst_rep")
        if up:
            try:
                df_i=read_excel_smart(up)
                cm={
                    "code":["code","code_poste","n_","num","item","ref","reference","no","numero"],
                    "desig":["designation","libelle","intitule","description","poste","ouvrage","travaux","prestation","nature","article","tache","objet","libelle_travaux"],
                    "unite":["unite","unit","u","unites","mesure","un"],
                    "qte":["quantite","qte","q","qt","volume","quantite_marche","quantites"],
                    "pu":["prix_unitaire","pu","prix","cout_unitaire","tarif","montant_unitaire","pu_ht"]
                }
                res={}
                for k,als in cm.items(): res[k]=find_col(set(df_i.columns),als)
                if not res["desig"] or not res["unite"]:
                    cols_trouves=", ".join(sorted(df_i.columns.tolist()))
                    manquantes=[]
                    if not res["desig"]: manquantes.append("Désignation")
                    if not res["unite"]: manquantes.append("Unité")
                    st.error(f"❌ Colonnes introuvables : **{', '.join(manquantes)}**\n\n"
                             f"Colonnes détectées dans votre fichier : `{cols_trouves}`\n\n"
                             f"Renommez vos colonnes avec : **Code | Désignation | Unité | Quantité | Prix Unitaire**")
                    st.stop()
                prev=pd.DataFrame({"Code":df_i[res["code"]] if res["code"] else "","Désignation":df_i[res["desig"]],"Unité":df_i[res["unite"]],"Qté":pd.to_numeric(df_i[res["qte"]] if res["qte"] else 0,errors="coerce").fillna(0),"PU":pd.to_numeric(df_i[res["pu"]] if res["pu"] else 0,errors="coerce").fillna(0)}).dropna(subset=["Désignation"])
                prev=prev[prev["Désignation"].astype(str).str.strip()!=""]
                prev["Montant"]=prev["Qté"]*prev["PU"]
                st.dataframe(prev,use_container_width=True); st.metric("Total",fmt(prev["Montant"].sum()))
                if st.button("✅ Confirmer l'import",key="dst_ok"):
                    if rep: exsql("DELETE FROM devis_st WHERE st_id=?",[st_id])
                    rows=[(st_id,str(r["Code"]).strip(),str(r["Désignation"]).strip(),str(r["Unité"]).strip(),float(r["Qté"]),float(r["PU"]),"") for _,r in prev.iterrows() if str(r["Désignation"]).strip()]
                    exmany("INSERT INTO devis_st(st_id,code_poste,designation,unite,quantite,prix_unitaire,observation)VALUES(?,?,?,?,?,?,?)",rows)
                    audit("devis_st","IMPORT",st_id,f"{len(rows)} postes importés"); st.success(f"✅ {len(rows)} postes importés."); st.rerun()
            except Exception as e: st.error(f"Erreur lecture fichier: {e}")

# ── DÉCOMPTE ST ───────────────────────────────────────────────
elif page=="decompte_st":
    st.title("🧾 Décompte Sous-traitant")
    df_st=get_sts()
    if df_st.empty: st.warning("Aucun sous-traitant."); st.stop()
    sel_s=st.selectbox("Sous-traitant",df_st["nom"].tolist(),key="dcst_sel"); rs=df_st[df_st["nom"]==sel_s].iloc[0]; st_id=int(rs["id"])

    # ── Sélection du cadre de devis ST (avec filtre par rue si plusieurs)
    df_dst_all=qdf("SELECT ds.*,r.nom AS rue_nom FROM devis_st ds LEFT JOIN rues r ON r.id=ds.rue_id WHERE ds.st_id=? ORDER BY ds.id",[st_id])
    rues_st=["(Toutes les rues)"]+[n for n in df_dst_all["rue_nom"].dropna().unique().tolist() if n] if not df_dst_all.empty else ["(Toutes les rues)"]

    if len(rues_st)>1:
        sel_rue_dst=st.selectbox("🛣️ Filtrer le cadre de devis ST par rue",rues_st,key="dcst_rue_filter")
        if sel_rue_dst!="(Toutes les rues)":
            df_dst_filt=df_dst_all[df_dst_all["rue_nom"]==sel_rue_dst].copy()
        else:
            df_dst_filt=df_dst_all.copy()
    else:
        df_dst_filt=df_dst_all.copy()

    if not df_dst_filt.empty:
        st.caption(f"Cadre de devis ST — **{sel_s}** : {len(df_dst_filt)} postes")

    ts,tr,tp=st.tabs(["📝 Saisir situation","📊 Récapitulatif","💳 Paiements"])
    with ts:
        df_d=df_dst_filt
        if df_d.empty: st.warning("Aucun poste de devis ST. Ajoutez d'abord des postes via le menu Devis ST.")
        else:
            with st.form("f_dcst"):
                c1,c2=st.columns(2); num=c1.number_input("N° situation",min_value=1,value=1,step=1); dt=c2.date_input("Date",value=date.today(),key="dcst_date")
                st.markdown("**Quantités exécutées :**"); qts={}
                for _,p in df_d.iterrows():
                    lbl=f"{p.get('code_poste','')} — {p['designation']} ({p['unite']}) | PU: {fmt(p['prix_unitaire'])}"
                    if pd.notna(p.get("rue_nom")) and str(p.get("rue_nom",""))!="": lbl+=f"  [Rue: {p['rue_nom']}]"
                    qts[int(p["id"])]=st.number_input(lbl,min_value=0.0,key=f"qst_{p['id']}")
                obs=st.text_input("Observation",key="dcst_obs")
                if st.form_submit_button("💾 Enregistrer"):
                    for did,q in qts.items():
                        if q>0:
                            p2=df_d[df_d["id"]==did].iloc[0]; mont=q*float(p2["prix_unitaire"] or 0)
                            exsql("INSERT INTO decomptes_st(st_id,numero_decompte,date_decompte,devis_st_id,quantite_executee,montant,observation)VALUES(?,?,?,?,?,?,?)",[st_id,num,str(dt),did,q,mont,obs])
                    audit("decomptes_st","CREATE",st_id,f"Situation n°{num}"); st.success(f"✅ Situation n°{num} enregistrée."); st.rerun()
    with tr:
        df_d=df_dst_filt
        if not df_d.empty:
            rows=[]
            for _,p in df_d.iterrows():
                ex=qdf("SELECT COALESCE(SUM(quantite_executee),0) AS qe,COALESCE(SUM(montant),0) AS mt FROM decomptes_st WHERE devis_st_id=?",[int(p["id"])])
                qe=float(ex.iloc[0]["qe"] or 0); mt=float(ex.iloc[0]["mt"] or 0); qm=float(p["quantite"] or 0)
                row_r={"Code":p.get("code_poste",""),"Désignation":p["designation"],"Unité":p["unite"],"Qté devis":qm,"Qté exécutée":qe,"Taux %":round(qe/qm*100,1) if qm else 0,"Montant devis":qm*float(p["prix_unitaire"] or 0),"Montant exécuté":mt}
                if "rue_nom" in p and pd.notna(p.get("rue_nom")) and str(p.get("rue_nom",""))!="": row_r["Rue"]=p["rue_nom"]
                rows.append(row_r)
            df_r2=pd.DataFrame(rows); td_val=df_r2["Montant devis"].sum(); te=df_r2["Montant exécuté"].sum()
            tp_val=float(_v(qdf("SELECT COALESCE(SUM(montant),0) AS s FROM paiements_st WHERE st_id=?",[st_id]), "s"))
            c1,c2,c3=st.columns(3); c1.metric("Montant devis ST",fmt(td_val)); c2.metric("Montant exécuté",fmt(te)); c3.metric("Solde à payer",fmt(te-tp_val))
            st.dataframe(df_r2,use_container_width=True)
            st.download_button("📥 Export",to_xl({"Décompte ST":df_r2}),f"dcst_{sel_s}.xlsx","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            st.info("Aucun poste de devis ST.")
    with tp:
        st.subheader("💳 Enregistrer un paiement")
        with st.form("f_pst"):
            c1,c2=st.columns(2); dt_p=c1.date_input("Date paiement",value=date.today(),key="pst_date"); mont_p=c2.number_input("Montant",min_value=0.0)
            c3,c4=st.columns(2); ref=c3.text_input("Référence"); mode=c4.selectbox("Mode",["Virement","Chèque","Espèces","Mobile Money"])
            obs_p=st.text_input("Observation")
            if st.form_submit_button("💾 Enregistrer"):
                if mont_p>0: exsql("INSERT INTO paiements_st(st_id,date_paiement,montant,reference,mode_paiement,observation)VALUES(?,?,?,?,?,?)",[st_id,str(dt_p),mont_p,ref,mode,obs_p]); st.success("✅ Paiement enregistré."); st.rerun()
        df_pay=qdf("SELECT date_paiement AS Date,montant AS Montant,reference AS Réf,mode_paiement AS Mode,observation AS Obs FROM paiements_st WHERE st_id=? ORDER BY date_paiement DESC",[st_id])
        if not df_pay.empty: st.metric("Total payé",fmt(df_pay["Montant"].sum())); st.dataframe(df_pay,use_container_width=True)


# ── PERSONNEL ─────────────────────────────────────────────────
elif page=="pers":
    st.title("👷 Gestion du Personnel")
    labs,id_map=ch_label_map()
    t1,t2,t3=st.tabs(["➕ Nouveau","✏️ Modifier","📋 Effectifs"])
    with t1:
        with st.form("f_pers_add"):
            c1,c2,c3=st.columns(3); nom_p=c1.text_input("Nom *"); prenom_p=c2.text_input("Prénom"); cat_p=c3.selectbox("Catégorie *",CATEGORIES_PERSONNEL,key="pers_cat_add")
            c4,c5=st.columns(2); poste_p=c4.text_input("Poste"); sal_p=c5.number_input("Salaire journalier",min_value=0.0)
            c6,c7=st.columns(2); tel_p=c6.text_input("Téléphone"); date_e=c7.date_input("Date d'entrée",value=date.today(),key="pers_date_add")
            ch_p=st.selectbox("Chantier affecté",["(Non affecté)"]+labs,key="pers_ch_add"); obs_p=st.text_area("Observation",height=60)
            if st.form_submit_button("💾 Enregistrer"):
                if nom_p.strip():
                    rid_p=id_map.get(ch_p) if ch_p!="(Non affecté)" else None
                    pid=exsql("INSERT INTO personnel(nom,prenom,categorie,poste,salaire_journalier,telephone,date_entree,rue_id,observation)VALUES(?,?,?,?,?,?,?,?,?)",[nom_p.strip(),prenom_p.strip(),cat_p,poste_p.strip(),sal_p,tel_p.strip(),str(date_e),rid_p,obs_p.strip()])
                    audit("personnel","CREATE",pid,f"Nouveau: {nom_p}"); st.success("✅ Enregistré."); st.rerun()
                else: st.error("Nom obligatoire.")
    with t2:
        df_p=get_pers(False)
        if df_p.empty: st.info("Aucun personnel.")
        else:
            df_p["label"]=df_p.apply(lambda r:f"{r['nom']} {r.get('prenom','')} — {r['categorie']}",axis=1)
            sel_p=st.selectbox("Personnel",df_p["label"].tolist(),key="pers_edit_sel"); rp=df_p[df_p["label"]==sel_p].iloc[0]
            with st.form("f_pers_edit"):
                c1,c2,c3=st.columns(3); en=c1.text_input("Nom *",value=str(rp["nom"])); ep=c2.text_input("Prénom",value=str(rp.get("prenom") or ""))
                cats=CATEGORIES_PERSONNEL; cc=str(rp.get("categorie") or "Autre")
                ec=c3.selectbox("Catégorie",cats,index=cats.index(cc) if cc in cats else 0,key="pers_cat_edit")
                c4,c5=st.columns(2); epos=c4.text_input("Poste",value=str(rp.get("poste") or "")); esal=c5.number_input("Salaire journalier",min_value=0.0,value=float(rp.get("salaire_journalier") or 0))
                etel=st.text_input("Téléphone",value=str(rp.get("telephone") or ""))
                ea_opts=["Actif","Inactif"]; ea=st.selectbox("Statut",ea_opts,index=0 if int(rp.get("actif") or 1) else 1,key="pers_actif_edit")
                eobs=st.text_area("Observation",value=str(rp.get("observation") or ""),height=60)
                ca,cb=st.columns(2); sv=ca.form_submit_button("✅ Enregistrer"); dl=cb.form_submit_button("🗑️ Supprimer")
            if sv:
                exsql("UPDATE personnel SET nom=?,prenom=?,categorie=?,poste=?,salaire_journalier=?,telephone=?,actif=?,observation=? WHERE id=?",[en,ep,ec,epos,esal,etel,1 if ea=="Actif" else 0,eobs,int(rp["id"])])
                audit("personnel","UPDATE",int(rp["id"]),f"Modif {en}"); st.success("✅ Modifié."); st.rerun()
            if dl: exsql("DELETE FROM personnel WHERE id=?",[int(rp["id"])]); st.success("Supprimé."); st.rerun()
    with t3:
        df_p=get_pers(False)
        if not df_p.empty:
            cf=st.selectbox("Filtrer catégorie",["Toutes"]+CATEGORIES_PERSONNEL,key="pers_cat_filt")
            df_s=df_p if cf=="Toutes" else df_p[df_p["categorie"]==cf]
            c1,c2=st.columns(2); c1.metric("Effectif",len(df_s[df_s["actif"]==1])); c2.metric("Masse salariale/j",fmt(df_s[df_s["actif"]==1]["salaire_journalier"].sum()))
            st.dataframe(df_s[["nom","prenom","categorie","poste","salaire_journalier","actif"]],use_container_width=True)
            st.download_button("📥 Export",to_xl({"Personnel":df_s}),"personnel.xlsx","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ── POINTAGE ─────────────────────────────────────────────────
elif page=="pointage":
    st.title("✅ Pointage du Personnel")
    df_p=get_pers(); labs,id_map=ch_label_map()
    ts,th=st.tabs(["📝 Saisie","📋 Historique"])
    with ts:
        if df_p.empty: st.warning("Aucun personnel actif.")
        else:
            c1,c2=st.columns(2); date_pt=c1.date_input("Date",value=date.today(),key="pt_date"); ch_pt=c2.selectbox("Chantier",["(Général)"]+labs,key="pt_ch")
            rid_pt=id_map.get(ch_pt) if ch_pt!="(Général)" else None
            st.markdown("---")
            rows_pt=[]
            for _,p in df_p.iterrows():
                with st.expander(f"👷 {p['nom']} {p.get('prenom','')} — {p['categorie']}"):
                    c1p,c2p,c3p=st.columns(3)
                    stat=c1p.selectbox("Statut",STATUT_PERSONNEL,key=f"ps_{p['id']}")
                    hres=c2p.number_input("Heures",min_value=0.0,max_value=24.0,value=8.0,key=f"ph_{p['id']}")
                    tache=c3p.text_input("Tâche",key=f"pt_{p['id']}")
                    rows_pt.append((str(date_pt),int(p["id"]),rid_pt,stat,hres,tache,""))
            if st.button("💾 Enregistrer le pointage",key="pt_save"):
                exmany("INSERT OR REPLACE INTO pointage(date_pointage,personnel_id,rue_id,statut,heures_travaillees,tache,observation)VALUES(?,?,?,?,?,?,?)",rows_pt)
                audit("pointage","CREATE",None,f"Pointage {date_pt} — {len(rows_pt)} agents"); st.success(f"✅ Pointage enregistré ({len(rows_pt)} agents)."); st.rerun()
    with th:
        df_h=qdf("SELECT pt.date_pointage AS Date,p.nom AS Nom,p.prenom AS Prénom,p.categorie AS Catégorie,pt.statut AS Statut,pt.heures_travaillees AS Heures,pt.tache AS Tâche,COALESCE(r.nom,'—') AS Chantier FROM pointage pt JOIN personnel p ON p.id=pt.personnel_id LEFT JOIN rues r ON r.id=pt.rue_id ORDER BY pt.date_pointage DESC,p.nom LIMIT 300")
        if not df_h.empty:
            df_filt=df_h[df_h["Date"]==str(date.today())]
            st.metric("Présents aujourd'hui",len(df_filt[df_filt["Statut"]=="Présent"]))
            st.dataframe(df_h,use_container_width=True)
            st.download_button("📥 Export",to_xl({"Pointage":df_h}),"pointage.xlsx","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else: st.info("Aucun pointage.")


# ── CIRCUIT APPRO (5 étapes) — avec modification ──────────────
elif page=="appro":
    st.title("🔄 Circuit d'Approvisionnement — 5 Étapes")
    st.caption("Besoin → Validation CC → Bon de commande → Réception → Mise en stock")
    labs,id_map=ch_label_map(); df_mats=get_mats()

    t1,t2,t3,t4,t5,t6,t7=st.tabs(["1️⃣ Besoin","2️⃣ Validation CC","3️⃣ Bon Commande","4️⃣ Réception","5️⃣ Mise en stock","✏️ Modifier saisie","📋 Suivi"])

    with t1:
        st.subheader("Expression du besoin")
        with st.form("f_appro_b"):
            c1,c2=st.columns(2); ch_a=c1.selectbox("Chantier",["(Général)"]+labs,key="appro_ch_b"); demandeur=c2.text_input("Demandeur")
            date_b=st.date_input("Date du besoin",value=date.today(),key="appro_date_b")
            mat_opts=["(Nouveau article)"]+df_mats["nom"].tolist() if not df_mats.empty else ["(Nouveau article)"]
            mat_sel=st.selectbox("Article du catalogue (ou nouveau)",mat_opts,key="appro_mat_sel")
            if mat_sel!="(Nouveau article)" and not df_mats.empty:
                rm=df_mats[df_mats["nom"]==mat_sel].iloc[0]; desig_a=mat_sel; unite_a=str(rm.get("unite",""))
                stk=stock_mat(int(rm["id"])); st.info(f"**Stock actuel :** {stk:.2f} {unite_a} | Seuil alerte: {rm.get('seuil_alerte',0)}")
            else: desig_a=""; unite_a=""
            c3,c4=st.columns(2)
            if mat_sel=="(Nouveau article)":
                desig_a=c3.text_input("Désignation *"); unite_a=c4.text_input("Unité *")
            c5,c6=st.columns(2); qte_d=c5.number_input("Quantité demandée *",min_value=0.01,value=1.0); pu_est=c6.number_input("Prix unitaire estimé",min_value=0.0)
            motif=st.text_area("Motif / Justification",height=60)
            if st.form_submit_button("📤 Soumettre le besoin"):
                rid_a=id_map.get(ch_a) if ch_a!="(Général)" else None
                mid_a=None
                if mat_sel!="(Nouveau article)" and not df_mats.empty:
                    rm2=df_mats[df_mats["nom"]==mat_sel].iloc[0]; mid_a=int(rm2["id"]); desig_a=mat_sel; unite_a=str(rm2.get("unite",""))
                if not desig_a.strip(): st.error("Désignation obligatoire."); st.stop()
                aid=exsql("INSERT INTO approvisionnements(date_besoin,rue_id,materiau_id,designation,unite,quantite_demandee,prix_unitaire_estime,demandeur,motif,statut)VALUES(?,?,?,?,?,?,?,?,?,?)",[str(date_b),rid_a,mid_a,desig_a.strip(),unite_a.strip(),qte_d,pu_est,demandeur.strip(),motif.strip(),"Besoin exprimé"])
                audit("approvisionnements","CREATE",aid,f"Besoin: {desig_a}"); st.success(f"✅ Besoin soumis (ID #{aid})."); st.rerun()

    with t2:
        st.subheader("Validation Chef de Chantier")
        df_v=qdf("SELECT a.*,COALESCE(r.nom,'Général') AS ch FROM approvisionnements a LEFT JOIN rues r ON r.id=a.rue_id WHERE a.statut='Besoin exprimé' ORDER BY a.date_besoin")
        if df_v.empty: st.success("✅ Aucun besoin en attente.")
        else:
            st.warning(f"⏳ {len(df_v)} besoin(s) en attente")
            for _,a in df_v.iterrows():
                with st.expander(f"#{int(a['id'])} — {a['designation']} × {a['quantite_demandee']} {a['unite']} | {a['ch']} | {a['date_besoin']}"):
                    st.text(f"Demandeur: {a.get('demandeur','—')} | Motif: {a.get('motif','—')}")
                    st.text(f"Estimation: {fmt(float(a.get('prix_unitaire_estime',0))*float(a.get('quantite_demandee',0)))}")
                    c1v,c2v,c3v=st.columns(3); val_cc=c1v.text_input("Validateur CC",key=f"vcc_{a['id']}")
                    if c2v.button("✅ Valider",key=f"val_{a['id']}"):
                        exsql("UPDATE approvisionnements SET statut='Validé chef chantier',date_validation_cc=?,validateur_cc=? WHERE id=?",[str(date.today()),val_cc,int(a["id"])])
                        audit("approvisionnements","UPDATE",int(a["id"]),"Validé CC"); st.success("Validé."); st.rerun()
                    if c3v.button("❌ Rejeter",key=f"rej_{a['id']}"):
                        exsql("UPDATE approvisionnements SET statut='Rejeté' WHERE id=?",[int(a["id"])]); st.warning("Rejeté."); st.rerun()

    with t3:
        st.subheader("Bon de Commande")
        df_bc=qdf("SELECT a.*,COALESCE(r.nom,'Général') AS ch FROM approvisionnements a LEFT JOIN rues r ON r.id=a.rue_id WHERE a.statut='Validé chef chantier' ORDER BY a.date_besoin")
        if df_bc.empty: st.success("✅ Aucun BC en attente.")
        else:
            for _,a in df_bc.iterrows():
                with st.expander(f"#{int(a['id'])} — {a['designation']} × {a['quantite_demandee']} {a['unite']} | {a['ch']}"):
                    c1b,c2b,c3b=st.columns(3); nbc=c1b.text_input("N° Bon de commande",key=f"nbc_{a['id']}"); fourn=c2b.text_input("Fournisseur",key=f"fourn_{a['id']}"); dbc=c3b.date_input("Date BC",value=date.today(),key=f"dbc_{a['id']}")
                    if st.button("📄 Émettre le BC",key=f"bc_{a['id']}"):
                        exsql("UPDATE approvisionnements SET statut='Bon de commande émis',numero_bc=?,fournisseur=?,date_bc=? WHERE id=?",[nbc,fourn,str(dbc),int(a["id"])])
                        audit("approvisionnements","UPDATE",int(a["id"]),f"BC: {nbc}"); st.success(f"BC {nbc} émis."); st.rerun()

    with t4:
        st.subheader("Réception de livraison")
        df_rec=qdf("SELECT a.*,COALESCE(r.nom,'Général') AS ch FROM approvisionnements a LEFT JOIN rues r ON r.id=a.rue_id WHERE a.statut='Bon de commande émis' ORDER BY a.date_besoin")
        if df_rec.empty: st.success("✅ Aucune livraison en attente.")
        else:
            for _,a in df_rec.iterrows():
                with st.expander(f"#{int(a['id'])} — {a['designation']} | Fourn: {a.get('fournisseur','—')} | BC: {a.get('numero_bc','—')}"):
                    c1r,c2r,c3r=st.columns(3); drec=c1r.date_input("Date réception",value=date.today(),key=f"drec_{a['id']}"); qrec=c2r.number_input("Quantité reçue",min_value=0.0,value=float(a.get("quantite_demandee",0)),key=f"qrec_{a['id']}"); bl=c3r.text_input("N° Bon de livraison",key=f"bl_{a['id']}")
                    if st.button("✅ Confirmer réception",key=f"rec_{a['id']}"):
                        exsql("UPDATE approvisionnements SET statut='Réceptionné',date_reception=?,quantite_recue=?,bon_livraison=? WHERE id=?",[str(drec),qrec,bl,int(a["id"])])
                        audit("approvisionnements","UPDATE",int(a["id"]),"Réceptionné"); st.success("✅ Réception confirmée."); st.rerun()

    with t5:
        st.subheader("Mise en stock")
        df_stk=qdf("SELECT a.*,COALESCE(r.nom,'Général') AS ch FROM approvisionnements a LEFT JOIN rues r ON r.id=a.rue_id WHERE a.statut='Réceptionné' ORDER BY a.date_besoin")
        if df_stk.empty: st.success("✅ Aucun article en attente de mise en stock.")
        else:
            for _,a in df_stk.iterrows():
                with st.expander(f"#{int(a['id'])} — {a['designation']} | Reçu: {a.get('quantite_recue','?')} {a['unite']} | BL: {a.get('bon_livraison','—')}"):
                    c1s,c2s,c3s=st.columns(3); dstk=c1s.date_input("Date mise en stock",value=date.today(),key=f"dstk_{a['id']}"); qstk=c2s.number_input("Quantité mise en stock",min_value=0.0,value=float(a.get("quantite_recue",0) or 0),key=f"qstk_{a['id']}"); pu_r=c3s.number_input("Prix unitaire réel",min_value=0.0,value=float(a.get("prix_unitaire_estime",0) or 0),key=f"pustk_{a['id']}")
                    if st.button("✅ Confirmer mise en stock",key=f"stk_{a['id']}"):
                        exsql("UPDATE approvisionnements SET statut='Mis en stock',date_mise_stock=?,quantite_mise_stock=?,prix_unitaire_reel=? WHERE id=?",[str(dstk),qstk,pu_r,int(a["id"])])
                        if a.get("materiau_id"):
                            exsql("INSERT INTO mouvements_materiaux(date_mvt,rue_id,materiau_id,type_mvt,quantite,prix_unitaire,fournisseur,bon_livraison,appro_id)VALUES(?,?,?,?,?,?,?,?,?)",[str(dstk),a.get("rue_id"),int(a["materiau_id"]),"ENTREE",qstk,pu_r,str(a.get("fournisseur","") or ""),str(a.get("bon_livraison","") or ""),int(a["id"])])
                        audit("approvisionnements","UPDATE",int(a["id"]),"Mis en stock"); st.success("✅ Mis en stock. Entrée stock créée."); st.rerun()

    with t6:
        st.subheader("✏️ Modifier une saisie d'approvisionnement")
        statuts_mod=["Besoin exprimé","Validé chef chantier","Bon de commande émis","Réceptionné","Mis en stock","Rejeté"]
        filt_st=st.selectbox("Filtrer par statut",["Tous"]+statuts_mod,key="appro_mod_filt")
        sql_mod="SELECT a.id,a.date_besoin,a.designation,a.statut,COALESCE(r.nom,'—') AS ch FROM approvisionnements a LEFT JOIN rues r ON r.id=a.rue_id"
        p_mod=[]
        if filt_st!="Tous": sql_mod+=" WHERE a.statut=?"; p_mod.append(filt_st)
        sql_mod+=" ORDER BY a.date_besoin DESC LIMIT 100"
        df_mod=qdf(sql_mod,p_mod)
        if df_mod.empty: st.info("Aucun enregistrement.")
        else:
            df_mod["label"]=df_mod.apply(lambda r:f"#{int(r['id'])} — {r['designation']} | {r['ch']} | {r['statut']} | {r['date_besoin']}",axis=1)
            sel_mod=st.selectbox("Sélectionner",df_mod["label"].tolist(),key="appro_mod_sel")
            aid_mod=int(df_mod[df_mod["label"]==sel_mod].iloc[0]["id"])
            ra=qdf("SELECT * FROM approvisionnements WHERE id=?",[aid_mod])
            if not ra.empty:
                ra=ra.iloc[0]
                with st.form("f_appro_edit"):
                    c1,c2=st.columns(2); ed=c1.text_input("Désignation",value=str(ra["designation"])); eu=c2.text_input("Unité",value=str(ra["unite"]))
                    c3,c4=st.columns(2); eq=c3.number_input("Quantité demandée",min_value=0.0,value=float(ra["quantite_demandee"] or 0)); epu=c4.number_input("PU estimé",min_value=0.0,value=float(ra["prix_unitaire_estime"] or 0))
                    c5,c6=st.columns(2); edem=c5.text_input("Demandeur",value=str(ra.get("demandeur") or "")); emot=c6.text_input("Motif",value=str(ra.get("motif") or ""))
                    st_opts2=statuts_mod; cst=str(ra.get("statut") or "Besoin exprimé")
                    est=st.selectbox("Statut",st_opts2,index=st_opts2.index(cst) if cst in st_opts2 else 0,key="appro_st_edit")
                    efourn=st.text_input("Fournisseur",value=str(ra.get("fournisseur") or "")); ebl=st.text_input("Bon de livraison",value=str(ra.get("bon_livraison") or ""))
                    eobs=st.text_area("Observation",value=str(ra.get("observation") or ""),height=60)
                    ca,cb=st.columns(2); sv=ca.form_submit_button("✅ Enregistrer"); dl=cb.form_submit_button("🗑️ Supprimer")
                if sv:
                    exsql("UPDATE approvisionnements SET designation=?,unite=?,quantite_demandee=?,prix_unitaire_estime=?,demandeur=?,motif=?,statut=?,fournisseur=?,bon_livraison=?,observation=? WHERE id=?",[ed,eu,eq,epu,edem,emot,est,efourn,ebl,eobs,aid_mod])
                    audit("approvisionnements","UPDATE",aid_mod,f"Modif: {ed}"); st.success("✅ Modifié."); st.rerun()
                if dl: exsql("DELETE FROM approvisionnements WHERE id=?",[aid_mod]); st.success("Supprimé."); st.rerun()

    with t7:
        st.subheader("📋 Suivi complet")
        fst=st.selectbox("Statut",["Tous"]+statuts_mod+["Rejeté"],key="appro_suivi_filt")
        df_all=qdf("SELECT a.id AS ID,a.date_besoin AS Date,COALESCE(r.nom,'—') AS Chantier,a.designation AS Article,a.unite AS Unité,a.quantite_demandee AS \"Qté dem.\",a.fournisseur AS Fournisseur,a.numero_bc AS BC,a.quantite_recue AS \"Qté reçue\",a.statut AS Statut FROM approvisionnements a LEFT JOIN rues r ON r.id=a.rue_id ORDER BY a.date_besoin DESC")
        if fst!="Tous": df_all=df_all[df_all["Statut"]==fst]
        if not df_all.empty: st.dataframe(df_all,use_container_width=True); st.download_button("📥 Export",to_xl({"Appros":df_all}),"appros.xlsx","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else: st.info("Aucun approvisionnement.")


# ── STOCK MATÉRIAUX ───────────────────────────────────────────
elif page=="stock":
    st.title("📦 Stock Matériaux")
    labs,id_map=ch_label_map()
    t1,t2,t3,t4,t5=st.tabs(["📋 Catalogue","📥 Entrée manuelle","📤 Sortie","✏️ Modifier mouvement","✏️ Modifier article"])
    with t1:
        df_m=get_mats()
        if not df_m.empty:
            rows_s=[]
            for _,m in df_m.iterrows():
                s=stock_mat(int(m["id"])); alerte="⚠️" if s<=float(m.get("seuil_alerte") or 0) and float(m.get("seuil_alerte") or 0)>0 else "✅"
                rows_s.append({"Matériau":m["nom"],"Catégorie":m.get("categorie",""),"Unité":m["unite"],"Stock":round(s,2),"Seuil":m.get("seuil_alerte",0),"PU":m.get("prix_unitaire",0),"Valeur":round(s*float(m.get("prix_unitaire",0) or 0),0),"":alerte})
            st.dataframe(pd.DataFrame(rows_s),use_container_width=True)
        st.subheader("➕ Ajouter article")
        # Unité choisie hors du form (le selectbox conditionnel ne fonctionne pas dans st.form)
        nm_add=st.text_input("Nom *",key="mat_add_nom")
        un_add=unite_selectbox("Unité *","mat_add_un")
        cat_m_add=st.selectbox("Catégorie",["Matériau","Carburant","Consommable","Fourniture","Autre"],key="mat_cat")
        c4,c5,c6=st.columns(3)
        si_add=c4.number_input("Stock initial",min_value=0.0,key="mat_add_si")
        seuil_add=c5.number_input("Seuil alerte",min_value=0.0,key="mat_add_seuil")
        pu_add=c6.number_input("Prix unitaire",min_value=0.0,key="mat_add_pu")
        if st.button("💾 Ajouter",key="mat_add_btn"):
            if nm_add.strip() and un_add and un_add!="?":
                try:
                    exsql("INSERT INTO materiaux(nom,unite,categorie,stock_initial,seuil_alerte,prix_unitaire)VALUES(?,?,?,?,?,?)",[nm_add.strip(),un_add,cat_m_add,si_add,seuil_add,pu_add])
                    st.success("✅ Ajouté."); st.rerun()
                except Exception as e: st.error(f"Erreur: {e}")
            else: st.error("Nom et Unité obligatoires.")
    with t2:
        df_m=get_mats()
        if df_m.empty: st.warning("Aucun article dans le catalogue.")
        else:
            with st.form("f_entree"):
                c1,c2=st.columns(2); ms=c1.selectbox("Article",df_m["nom"].tolist(),key="entree_art"); ch_e=c2.selectbox("Chantier",["(Général)"]+labs,key="entree_ch")
                c3,c4,c5=st.columns(3); de=c3.date_input("Date",value=date.today(),key="entree_date"); qe=c4.number_input("Quantité",min_value=0.01); pue=c5.number_input("Prix unitaire",min_value=0.0)
                c6,c7=st.columns(2); fne=c6.text_input("Fournisseur"); ble=c7.text_input("Bon de livraison"); obse=st.text_input("Observation")
                if st.form_submit_button("💾 Enregistrer entrée"):
                    mr=df_m[df_m["nom"]==ms].iloc[0]; rid_e=id_map.get(ch_e) if ch_e!="(Général)" else None
                    exsql("INSERT INTO mouvements_materiaux(date_mvt,rue_id,materiau_id,type_mvt,quantite,prix_unitaire,fournisseur,bon_livraison,observation)VALUES(?,?,?,?,?,?,?,?,?)",[str(de),rid_e,int(mr["id"]),"ENTREE",qe,pue,fne,ble,obse]); st.success("✅ Entrée enregistrée."); st.rerun()
    with t3:
        df_m=get_mats()
        if df_m.empty: st.warning("Aucun article.")
        else:
            with st.form("f_sortie"):
                c1,c2=st.columns(2); ms=c1.selectbox("Article",df_m["nom"].tolist(),key="sortie_art"); ch_s=c2.selectbox("Chantier",["(Général)"]+labs,key="sortie_ch")
                mr=df_m[df_m["nom"]==ms].iloc[0]; stk_d=stock_mat(int(mr["id"])); st.info(f"Stock disponible: **{stk_d:.2f} {mr['unite']}**")
                c3,c4=st.columns(2); ds=c3.date_input("Date sortie",value=date.today(),key="sortie_date"); qs=c4.number_input("Quantité à sortir",min_value=0.01,max_value=max(float(stk_d),0.01))
                obss=st.text_input("Observation / Destination")
                if st.form_submit_button("📤 Enregistrer sortie"):
                    rid_s2=id_map.get(ch_s) if ch_s!="(Général)" else None
                    exsql("INSERT INTO mouvements_materiaux(date_mvt,rue_id,materiau_id,type_mvt,quantite,observation)VALUES(?,?,?,?,?,?)",[str(ds),rid_s2,int(mr["id"]),"SORTIE",qs,obss]); st.success("✅ Sortie enregistrée."); st.rerun()
    with t4:
        st.subheader("✏️ Modifier / Supprimer un mouvement de stock")
        df_mvt=qdf("SELECT mm.id,mm.date_mvt AS Date,mm.type_mvt AS Type,m.nom AS Matériau,mm.quantite AS Quantité,mm.prix_unitaire AS PU,COALESCE(r.nom,'—') AS Chantier,mm.fournisseur AS Fournisseur,mm.observation AS Observation FROM mouvements_materiaux mm JOIN materiaux m ON m.id=mm.materiau_id LEFT JOIN rues r ON r.id=mm.rue_id ORDER BY mm.date_mvt DESC LIMIT 200")
        if df_mvt.empty: st.info("Aucun mouvement.")
        else:
            df_mvt["label"]=df_mvt.apply(lambda r:f"{r['Date']} | {r['Type']} | {r['Matériau']} × {r['Quantité']} | {r['Chantier']}",axis=1)
            sel_mv=st.selectbox("Mouvement à modifier",df_mvt["label"].tolist(),key="mvt_edit_sel"); mv=df_mvt[df_mvt["label"]==sel_mv].iloc[0]
            with st.form("f_mvt_edit"):
                c1,c2=st.columns(2); eq=c1.number_input("Quantité",min_value=0.01,value=float(mv["Quantité"] or 1)); epu=c2.number_input("Prix unitaire",min_value=0.0,value=float(mv["PU"] or 0))
                efn=st.text_input("Fournisseur",value=str(mv.get("Fournisseur") or ""))
                eobs=st.text_area("Observation",value=str(mv.get("Observation") or ""),height=60)
                ca,cb=st.columns(2); sv=ca.form_submit_button("✅ Enregistrer"); dl=cb.form_submit_button("🗑️ Supprimer")
            if sv:
                exsql("UPDATE mouvements_materiaux SET quantite=?,prix_unitaire=?,fournisseur=?,observation=? WHERE id=?",[eq,epu,efn,eobs,int(mv["id"])]); st.success("✅ Modifié."); st.rerun()
            if dl: exsql("DELETE FROM mouvements_materiaux WHERE id=?",[int(mv["id"])]); st.success("Supprimé."); st.rerun()
    with t5:
        df_m=get_mats()
        if not df_m.empty:
            sel_ma=st.selectbox("Article à modifier",df_m["nom"].tolist(),key="mat_edit_sel"); rm=df_m[df_m["nom"]==sel_ma].iloc[0]
            # Hors form pour permettre le selectbox conditionnel d'unité
            en=st.text_input("Nom",value=str(rm["nom"]),key="mat_e_nom")
            eu=unite_selectbox("Unité","mat_e_un",default=str(rm.get("unite") or ""))
            cats_m=["Matériau","Carburant","Consommable","Fourniture","Autre"]; cc=str(rm.get("categorie") or "Matériau")
            ec=st.selectbox("Catégorie",cats_m,index=cats_m.index(cc) if cc in cats_m else 0,key="mat_cat_edit")
            c3,c4,c5=st.columns(3)
            esi=c3.number_input("Stock initial",min_value=0.0,value=float(rm.get("stock_initial") or 0),key="mat_e_si")
            eseuil=c4.number_input("Seuil alerte",min_value=0.0,value=float(rm.get("seuil_alerte") or 0),key="mat_e_seuil")
            epu=c5.number_input("Prix unitaire",min_value=0.0,value=float(rm.get("prix_unitaire") or 0),key="mat_e_pu")
            ca,cb=st.columns(2); sv=ca.button("✅ Enregistrer",key="mat_e_sv"); dl=cb.button("🗑️ Supprimer",key="mat_e_dl")
            if sv:
                exsql("UPDATE materiaux SET nom=?,unite=?,categorie=?,stock_initial=?,seuil_alerte=?,prix_unitaire=? WHERE id=?",[en,eu,ec,esi,eseuil,epu,int(rm["id"])]); st.success("✅ Modifié."); st.rerun()
            if dl: exsql("DELETE FROM materiaux WHERE id=?",[int(rm["id"])]); st.success("Supprimé."); st.rerun()


# ── MATÉRIELS & ENGINS ───────────────────────────────────────────
elif page=="engins":
    st.header("🚧 Matériels & Engins")
    t1,t2,t3=st.tabs(["📋 Liste","➕ Ajouter / Modifier","🔧 Maintenance"])

    def get_engins():
        return qdf("SELECT * FROM materiels ORDER BY nom")

    with t1:
        df_e=get_engins()
        if df_e.empty: st.info("Aucun engin enregistré.")
        else: st.dataframe(df_e,use_container_width=True)

    with t2:
        st.subheader("➕ Ajouter un engin")
        with st.form("f_add_eng"):
            c1,c2=st.columns(2)
            nom_e=c1.text_input("Nom / Désignation",key="eng_nom")
            immat_e=c2.text_input("Immatriculation / N° série",key="eng_immat")
            c3,c4=st.columns(2)
            type_e=c3.selectbox("Type",["Engin TP","Véhicule","Groupe électrogène","Pompe","Autre"],key="eng_type")
            etat_e=c4.selectbox("État",["Opérationnel","En panne","En maintenance","Hors service"],key="eng_etat")
            c5,c6=st.columns(2)
            date_acq=c5.date_input("Date acquisition",value=date.today(),key="eng_date_acq")
            hrs_e=c6.number_input("Heures compteur actuel",min_value=0.0,key="eng_hrs")
            obs_e=st.text_area("Observations",height=60,key="eng_obs")
            if st.form_submit_button("💾 Enregistrer"):
                if not nom_e: st.error("Nom obligatoire.")
                else:
                    exsql("INSERT OR IGNORE INTO materiels(nom,immatriculation,type_materiel,etat,date_acquisition,heure_compteur,observations)VALUES(?,?,?,?,?,?,?)",[nom_e,immat_e,type_e,etat_e,str(date_acq),hrs_e,obs_e])
                    st.success("✅ Engin ajouté."); st.rerun()

        st.divider()
        st.subheader("✏️ Modifier un engin existant")
        df_e2=get_engins()
        if not df_e2.empty:
            sel_e=st.selectbox("Engin à modifier",df_e2["nom"].tolist(),key="eng_edit_sel")
            re=df_e2[df_e2["nom"]==sel_e].iloc[0]
            with st.form("f_edit_eng"):
                c1,c2=st.columns(2)
                en_nom=c1.text_input("Nom",value=str(re["nom"]),key="eng_e_nom")
                en_immat=c2.text_input("Immatriculation",value=str(re.get("immatriculation") or ""),key="eng_e_immat")
                types_e=["Engin TP","Véhicule","Groupe électrogène","Pompe","Autre"]
                etats_e=["Opérationnel","En panne","En maintenance","Hors service"]
                c3,c4=st.columns(2)
                en_type=c3.selectbox("Type",types_e,index=types_e.index(str(re.get("type_materiel") or "Autre")) if str(re.get("type_materiel") or "Autre") in types_e else 0,key="eng_e_type")
                en_etat=c4.selectbox("État",etats_e,index=etats_e.index(str(re.get("etat") or "Opérationnel")) if str(re.get("etat") or "Opérationnel") in etats_e else 0,key="eng_e_etat")
                c5,c6=st.columns(2)
                try: v_acq=date.fromisoformat(str(re.get("date_acquisition") or ""))
                except: v_acq=date.today()
                en_acq=c5.date_input("Date acquisition",value=v_acq,key="eng_e_acq")
                en_hrs=c6.number_input("Heures compteur",min_value=0.0,value=float(re.get("heure_compteur") or 0),key="eng_e_hrs")
                en_obs=st.text_area("Observations",value=str(re.get("observations") or ""),height=60,key="eng_e_obs")
                ca,cb=st.columns(2)
                sv=ca.form_submit_button("✅ Enregistrer")
                dl=cb.form_submit_button("🗑️ Supprimer")
            if sv:
                exsql("UPDATE materiels SET nom=?,immatriculation=?,type_materiel=?,etat=?,date_acquisition=?,heure_compteur=?,observations=? WHERE id=?",[en_nom,en_immat,en_type,en_etat,str(en_acq),en_hrs,en_obs,int(re["id"])])
                st.success("✅ Modifié."); st.rerun()
            if dl:
                exsql("DELETE FROM materiels WHERE id=?",[int(re["id"])]); st.success("Supprimé."); st.rerun()

    with t3:
        st.subheader("🔧 Ajouter une intervention maintenance")
        df_e3=get_engins()
        if df_e3.empty: st.warning("Aucun engin enregistré.")
        else:
            with st.form("f_maint"):
                c1,c2=st.columns(2)
                eng_m=c1.selectbox("Engin",df_e3["nom"].tolist(),key="maint_eng")
                dm=c2.date_input("Date intervention",value=date.today(),key="maint_date")
                c3,c4=st.columns(2)
                type_m=c3.selectbox("Type",["Préventive","Corrective","Vidange","Réparation","Autre"],key="maint_type")
                cout_m=c4.number_input("Coût (DH)",min_value=0.0,key="maint_cout")
                desc_m=st.text_area("Description de l'intervention",height=80,key="maint_desc")
                if st.form_submit_button("💾 Enregistrer maintenance"):
                    er=df_e3[df_e3["nom"]==eng_m].iloc[0]
                    exsql("INSERT INTO maintenance_materiels(materiel_id,date_maintenance,type_maintenance,description,cout)VALUES(?,?,?,?,?)",[int(er["id"]),str(dm),type_m,desc_m,cout_m])
                    st.success("✅ Maintenance enregistrée."); st.rerun()

            st.divider()
            st.subheader("📋 Historique des maintenances")
            df_hist=qdf("SELECT mm.date_maintenance AS Date,m.nom AS Engin,mm.type_maintenance AS Type,mm.description AS Description,mm.cout AS Coût FROM maintenance_materiels mm JOIN materiels m ON m.id=mm.materiel_id ORDER BY mm.date_maintenance DESC LIMIT 100")
            if df_hist.empty: st.info("Aucune maintenance enregistrée.")
            else: st.dataframe(df_hist,use_container_width=True)


# ── SUIVI JOURNALIER ENGINS ───────────────────────────────────────
elif page=="suivi_eng":
    st.header("⛽ Suivi Journalier des Engins")
    labs,id_map=ch_label_map()
    t1,t2,t3=st.tabs(["➕ Nouvelle saisie","📋 Consulter","✏️ Modifier saisie"])

    def get_carburants():
        return qdf("SELECT id,nom,unite FROM materiaux WHERE LOWER(categorie)='carburant' ORDER BY nom")

    with t1:
        st.subheader("➕ Saisie journalière")
        df_eng=qdf("SELECT id,nom FROM materiels WHERE etat='Opérationnel' ORDER BY nom")
        if df_eng.empty:
            st.warning("Aucun engin opérationnel. Ajoutez des engins dans Matériels & Engins.")
        elif not labs:
            st.warning("Aucun chantier. Créez d'abord un chantier.")
        else:
            df_carb=get_carburants()
            with st.form("f_suivi_eng"):
                c1,c2=st.columns(2)
                ch_se=c1.selectbox("Chantier",labs,key="se_ch")
                date_se=c2.date_input("Date",value=date.today(),key="se_date")
                c3,c4=st.columns(2)
                eng_se=c3.selectbox("Engin",df_eng["nom"].tolist(),key="se_eng")
                nb_hrs=c4.number_input("Heures de travail",min_value=0.0,step=0.5,key="se_hrs")
                st.markdown("**⛽ Carburant**")
                c5,c6,c7=st.columns(3)
                use_carb=c5.checkbox("Enregistrer carburant",value=True,key="se_use_carb")
                if not df_carb.empty:
                    carb_names=df_carb["nom"].tolist()
                    carb_sel=c6.selectbox("Type carburant",carb_names,key="se_carb_type")
                    cr=df_carb[df_carb["nom"]==carb_sel].iloc[0]
                    stk_carb=stock_mat(int(cr["id"]))
                    c7.metric("Stock disponible",f"{stk_carb:.1f} {cr['unite']}")
                    qte_carb=st.number_input(f"Quantité {cr['unite']} consommée",min_value=0.0,step=1.0,key="se_qte_carb")
                else:
                    carb_sel=None; cr=None; qte_carb=0.0
                    c6.warning("Aucun carburant dans le catalogue. Ajoutez un article de catégorie 'Carburant'.")
                obs_se=st.text_area("Observations",height=60,key="se_obs")
                if st.form_submit_button("💾 Enregistrer"):
                    rid_se=id_map.get(ch_se)
                    er=df_eng[df_eng["nom"]==eng_se].iloc[0]
                    carb_id=int(cr["id"]) if (use_carb and cr is not None) else None
                    qte_c=qte_carb if (use_carb and cr is not None and qte_carb>0) else 0
                    exsql("INSERT INTO suivi_materiels(date_suivi,rue_id,materiel_id,heures_travail,carburant_materiau_id,carburant_consomme,observations)VALUES(?,?,?,?,?,?,?)",
                          [str(date_se),rid_se,int(er["id"]),nb_hrs,carb_id,qte_c,obs_se])
                    # Déduire le carburant du stock si renseigné
                    if use_carb and carb_id and qte_c>0:
                        exsql("INSERT INTO mouvements_materiaux(date_mvt,rue_id,materiau_id,type_mvt,quantite,observation)VALUES(?,?,?,?,?,?)",
                              [str(date_se),rid_se,carb_id,"SORTIE",qte_c,f"Carburant engin {eng_se}"])
                        st.info(f"⛽ {qte_c:.1f} {cr['unite']} de {carb_sel} déduit(e) du stock.")
                    st.success("✅ Suivi enregistré."); st.rerun()

    with t2:
        st.subheader("📋 Historique du suivi engins")
        col1,col2=st.columns(2)
        if labs:
            ch_sv2=col1.selectbox("Filtrer par chantier",["Tous"]+labs,key="se_list_ch")
        else:
            ch_sv2="Tous"
        periode_se=col2.selectbox("Période",["7 jours","30 jours","90 jours","Tout"],key="se_periode")
        days_map={"7 jours":7,"30 jours":30,"90 jours":90,"Tout":9999}
        d_lim=(date.today()-timedelta(days=days_map[periode_se])).isoformat()
        if ch_sv2=="Tous" or not labs:
            df_sv=qdf(f"SELECT sm.date_suivi AS Date,COALESCE(r.nom,'—') AS Chantier,m.nom AS Engin,sm.heures_travail AS Heures,COALESCE(ma.nom,'—') AS Carburant,sm.carburant_consomme AS Qté_Carb,sm.observations AS Obs FROM suivi_materiels sm JOIN materiels m ON m.id=sm.materiel_id LEFT JOIN rues r ON r.id=sm.rue_id LEFT JOIN materiaux ma ON ma.id=sm.carburant_materiau_id WHERE sm.date_suivi>='{d_lim}' ORDER BY sm.date_suivi DESC LIMIT 200")
        else:
            rid_sv=id_map.get(ch_sv2)
            df_sv=qdf(f"SELECT sm.date_suivi AS Date,COALESCE(r.nom,'—') AS Chantier,m.nom AS Engin,sm.heures_travail AS Heures,COALESCE(ma.nom,'—') AS Carburant,sm.carburant_consomme AS Qté_Carb,sm.observations AS Obs FROM suivi_materiels sm JOIN materiels m ON m.id=sm.materiel_id LEFT JOIN rues r ON r.id=sm.rue_id LEFT JOIN materiaux ma ON ma.id=sm.carburant_materiau_id WHERE sm.rue_id=? AND sm.date_suivi>='{d_lim}' ORDER BY sm.date_suivi DESC LIMIT 200",[rid_sv])
        if df_sv.empty: st.info("Aucune donnée pour cette période.")
        else:
            st.dataframe(df_sv,use_container_width=True)
            col_m,col_h=st.columns(2)
            col_m.metric("Total heures",f"{df_sv['Heures'].sum():.1f} h")
            if "Qté_Carb" in df_sv.columns: col_h.metric("Total carburant",f"{df_sv['Qté_Carb'].sum():.1f} L")

    with t3:
        st.subheader("✏️ Modifier / Supprimer une saisie")
        df_sv_all=qdf("SELECT sm.id,sm.date_suivi,r.nom AS chantier,m.nom AS engin,sm.heures_travail,COALESCE(ma.nom,'—') AS carburant,sm.carburant_consomme,sm.carburant_materiau_id,sm.observations FROM suivi_materiels sm JOIN materiels m ON m.id=sm.materiel_id LEFT JOIN rues r ON r.id=sm.rue_id LEFT JOIN materiaux ma ON ma.id=sm.carburant_materiau_id ORDER BY sm.date_suivi DESC LIMIT 100")
        if df_sv_all.empty: st.info("Aucune saisie.")
        else:
            df_sv_all["label"]=df_sv_all.apply(lambda r:f"{r['date_suivi']} | {r['chantier']} | {r['engin']} | {r['heures_travail']}h",axis=1)
            sel_sv=st.selectbox("Saisie à modifier",df_sv_all["label"].tolist(),key="se_edit_sel")
            rs=df_sv_all[df_sv_all["label"]==sel_sv].iloc[0]
            df_eng2=qdf("SELECT id,nom FROM materiels ORDER BY nom")
            df_carb2=get_carburants()
            with st.form("f_sv_edit"):
                c1,c2=st.columns(2)
                try: v_dt=date.fromisoformat(str(rs["date_suivi"]))
                except: v_dt=date.today()
                ed_date=c1.date_input("Date",value=v_dt,key="se_e_date")
                ed_hrs=c2.number_input("Heures travail",min_value=0.0,value=float(rs["heures_travail"] or 0),step=0.5,key="se_e_hrs")
                st.markdown("**⛽ Carburant**")
                c3,c4=st.columns(2)
                if not df_carb2.empty:
                    carb_names2=df_carb2["nom"].tolist()
                    cur_carb_idx=0
                    if rs["carburant_materiau_id"]:
                        cur_cr=df_carb2[df_carb2["id"]==rs["carburant_materiau_id"]]
                        if not cur_cr.empty: cur_carb_idx=carb_names2.index(cur_cr.iloc[0]["nom"])
                    ed_carb=c3.selectbox("Type carburant",carb_names2,index=cur_carb_idx,key="se_e_carb")
                    ed_qte=c4.number_input("Quantité carburant",min_value=0.0,value=float(rs["carburant_consomme"] or 0),step=1.0,key="se_e_qte")
                    ed_carb_id=int(df_carb2[df_carb2["nom"]==ed_carb].iloc[0]["id"])
                else:
                    ed_carb=None; ed_qte=0.0; ed_carb_id=None
                ed_obs=st.text_area("Observations",value=str(rs.get("observations") or ""),height=60,key="se_e_obs")
                ca,cb=st.columns(2)
                sv_se=ca.form_submit_button("✅ Enregistrer")
                dl_se=cb.form_submit_button("🗑️ Supprimer")
            if sv_se:
                exsql("UPDATE suivi_materiels SET date_suivi=?,heures_travail=?,carburant_materiau_id=?,carburant_consomme=?,observations=? WHERE id=?",
                      [str(ed_date),ed_hrs,ed_carb_id,ed_qte,ed_obs,int(rs["id"])])
                st.success("✅ Saisie modifiée."); st.rerun()
            if dl_se:
                exsql("DELETE FROM suivi_materiels WHERE id=?",[int(rs["id"])]); st.success("Supprimé."); st.rerun()


# ── JOURNAL DE CHANTIER ───────────────────────────────────────────
elif page=="journal":
    st.header("📔 Journal de Chantier")
    labs,id_map=ch_label_map()
    t1,t2,t3,t4=st.tabs(["➕ Saisie quotidienne","📋 Consulter","📊 Rapport hebdo","✏️ Modifier saisie"])

    with t1:
        st.subheader("➕ Nouvelle entrée journal")
        if not labs:
            st.warning("Aucun chantier disponible.")
        else:
            with st.form("f_journal_saisie"):
                c1,c2=st.columns(2)
                ch_j1=c1.selectbox("Chantier",labs,key="jrn_saisie_ch")
                date_j1=c2.date_input("Date",value=date.today(),key="jrn_saisie_date")
                c3,c4=st.columns(2)
                meteo_j1=c3.selectbox("Météo",["Ensoleillé","Nuageux","Pluvieux","Vent","Brouillard","Neige"],key="jrn_saisie_meteo")
                temp_j1=c4.number_input("Température (°C)",value=20.0,step=1.0,key="jrn_saisie_temp")
                st.markdown("**Personnel présent**")
                c5,c6=st.columns(2)
                nb_ouvriers=c5.number_input("Ouvriers",min_value=0,step=1,key="jrn_saisie_ouv")
                nb_encad=c6.number_input("Encadrants",min_value=0,step=1,key="jrn_saisie_enc")
                travaux_j1=st.text_area("Travaux réalisés",height=100,key="jrn_saisie_trav")
                incidents_j1=st.text_area("Incidents / Observations sécurité",height=60,key="jrn_saisie_inc")
                livraisons_j1=st.text_area("Livraisons / Réceptions",height=60,key="jrn_saisie_liv")
                visites_j1=st.text_area("Visites / Réunions",height=60,key="jrn_saisie_vis")
                obs_j1=st.text_area("Observations générales",height=60,key="jrn_saisie_obs")
                if st.form_submit_button("💾 Enregistrer"):
                    rid_j1=id_map.get(ch_j1)
                    contenu=f"TRAVAUX: {travaux_j1}\nINCIDENTS: {incidents_j1}\nLIVRAISONS: {livraisons_j1}\nVISITES: {visites_j1}\nOBS: {obs_j1}"
                    exsql("INSERT INTO journal_chantier(date_journal,rue_id,meteo,temperature,nb_ouvriers,nb_encadrants,travaux_realises,observations)VALUES(?,?,?,?,?,?,?,?)",
                          [str(date_j1),rid_j1,meteo_j1,temp_j1,nb_ouvriers,nb_encad,travaux_j1,obs_j1])
                    st.success("✅ Entrée journal enregistrée."); st.rerun()

    with t2:
        st.subheader("📋 Consulter le journal")
        col1,col2=st.columns(2)
        if labs:
            ch_j2=col1.selectbox("Chantier",["Tous"]+labs,key="jrn_list_ch")
        else:
            ch_j2="Tous"
        periode_j=col2.selectbox("Période",["7 jours","30 jours","90 jours","Tout"],key="jrn_list_periode")
        days_j={"7 jours":7,"30 jours":30,"90 jours":90,"Tout":9999}
        d_lim_j=(date.today()-timedelta(days=days_j[periode_j])).isoformat()
        if ch_j2=="Tous" or not labs:
            df_jrn=qdf(f"SELECT jc.date_journal AS Date,COALESCE(r.nom,'Général') AS Chantier,jc.meteo AS Météo,jc.temperature AS Temp,jc.nb_ouvriers AS Ouvriers,jc.nb_encadrants AS Encadrants,jc.travaux_realises AS Travaux,jc.observations AS Observations FROM journal_chantier jc LEFT JOIN rues r ON r.id=jc.rue_id WHERE jc.date_journal>='{d_lim_j}' ORDER BY jc.date_journal DESC LIMIT 200")
        else:
            rid_j2=id_map.get(ch_j2)
            df_jrn=qdf(f"SELECT jc.date_journal AS Date,COALESCE(r.nom,'Général') AS Chantier,jc.meteo AS Météo,jc.temperature AS Temp,jc.nb_ouvriers AS Ouvriers,jc.nb_encadrants AS Encadrants,jc.travaux_realises AS Travaux,jc.observations AS Observations FROM journal_chantier jc LEFT JOIN rues r ON r.id=jc.rue_id WHERE jc.rue_id=? AND jc.date_journal>='{d_lim_j}' ORDER BY jc.date_journal DESC LIMIT 200",[rid_j2])
        if df_jrn.empty: st.info("Aucune entrée pour cette période.")
        else: st.dataframe(df_jrn,use_container_width=True)

    with t3:
        st.subheader("📊 Rapport hebdomadaire")
        col1h,col2h=st.columns(2)
        if labs:
            ch_j3=col1h.selectbox("Chantier",labs,key="jrn_hebdo_ch")
        else:
            st.warning("Aucun chantier disponible."); ch_j3=None
        date_hebdo=col2h.date_input("Semaine du lundi",value=date.today()-timedelta(days=date.today().weekday()),key="jrn_hebdo_date")
        if ch_j3 and labs:
            rid_j3=id_map.get(ch_j3)
            d_debut=date_hebdo.isoformat()
            d_fin=(date_hebdo+timedelta(days=6)).isoformat()
            df_hebdo=qdf(f"SELECT jc.date_journal AS Date,jc.meteo AS Météo,jc.nb_ouvriers AS Ouvriers,jc.nb_encadrants AS Encadrants,jc.travaux_realises AS Travaux FROM journal_chantier jc WHERE jc.rue_id=? AND jc.date_journal BETWEEN ? AND ? ORDER BY jc.date_journal",[rid_j3,d_debut,d_fin])
            st.markdown(f"**Semaine du {d_debut} au {d_fin}**")
            if df_hebdo.empty: st.info("Aucune entrée cette semaine.")
            else:
                st.dataframe(df_hebdo,use_container_width=True)
                col_a,col_b=st.columns(2)
                col_a.metric("Total ouvriers-jours",int(df_hebdo["Ouvriers"].sum()))
                col_b.metric("Total encadrants-jours",int(df_hebdo["Encadrants"].sum()))

    with t4:
        st.subheader("✏️ Modifier / Supprimer une entrée journal")
        df_jrn_all=qdf("SELECT jc.id,jc.date_journal,COALESCE(r.nom,'Général') AS chantier,jc.meteo,jc.temperature,jc.nb_ouvriers,jc.nb_encadrants,jc.travaux_realises,jc.observations FROM journal_chantier jc LEFT JOIN rues r ON r.id=jc.rue_id ORDER BY jc.date_journal DESC LIMIT 100")
        if df_jrn_all.empty: st.info("Aucune entrée.")
        else:
            df_jrn_all["label"]=df_jrn_all.apply(lambda r:f"{r['date_journal']} | {r['chantier']} | {str(r['travaux_realises'])[:40]}",axis=1)
            sel_jrn=st.selectbox("Entrée à modifier",df_jrn_all["label"].tolist(),key="jrn_edit_sel")
            rj=df_jrn_all[df_jrn_all["label"]==sel_jrn].iloc[0]
            with st.form("f_jrn_edit"):
                c1,c2=st.columns(2)
                try: v_dj=date.fromisoformat(str(rj["date_journal"]))
                except: v_dj=date.today()
                ej_date=c1.date_input("Date",value=v_dj,key="jrn_e_date")
                meteos=["Ensoleillé","Nuageux","Pluvieux","Vent","Brouillard","Neige"]
                cur_m=str(rj.get("meteo") or "Ensoleillé")
                ej_meteo=c2.selectbox("Météo",meteos,index=meteos.index(cur_m) if cur_m in meteos else 0,key="jrn_e_meteo")
                c3,c4=st.columns(2)
                ej_temp=c3.number_input("Température",value=float(rj.get("temperature") or 20),step=1.0,key="jrn_e_temp")
                ej_ouv=c4.number_input("Ouvriers",min_value=0,value=int(rj.get("nb_ouvriers") or 0),key="jrn_e_ouv")
                ej_enc=st.number_input("Encadrants",min_value=0,value=int(rj.get("nb_encadrants") or 0),key="jrn_e_enc")
                ej_trav=st.text_area("Travaux réalisés",value=str(rj.get("travaux_realises") or ""),height=100,key="jrn_e_trav")
                ej_obs=st.text_area("Observations",value=str(rj.get("observations") or ""),height=60,key="jrn_e_obs")
                ca,cb=st.columns(2)
                sv_jrn=ca.form_submit_button("✅ Enregistrer")
                dl_jrn=cb.form_submit_button("🗑️ Supprimer")
            if sv_jrn:
                exsql("UPDATE journal_chantier SET date_journal=?,meteo=?,temperature=?,nb_ouvriers=?,nb_encadrants=?,travaux_realises=?,observations=? WHERE id=?",
                      [str(ej_date),ej_meteo,ej_temp,ej_ouv,ej_enc,ej_trav,ej_obs,int(rj["id"])])
                st.success("✅ Entrée modifiée."); st.rerun()
            if dl_jrn:
                exsql("DELETE FROM journal_chantier WHERE id=?",[int(rj["id"])]); st.success("Supprimé."); st.rerun()


# ── CAISSE CHANTIER ───────────────────────────────────────────────
elif page=="caisse":
    st.header("💰 Caisse Chantier")
    labs,id_map=ch_label_map()
    t1,t2,t3=st.tabs(["➕ Saisie","📋 Consulter","✏️ Modifier"])

    with t1:
        st.subheader("➕ Nouvelle opération caisse")
        if not labs: st.warning("Aucun chantier disponible.")
        else:
            with st.form("f_caisse"):
                c1,c2=st.columns(2)
                ch_ca=c1.selectbox("Chantier",labs,key="ca_ch")
                date_ca=c2.date_input("Date opération",value=date.today(),key="ca_date")
                c3,c4=st.columns(2)
                type_ca=c3.selectbox("Type",["Dépense","Recette","Avance"],key="ca_type")
                montant_ca=c4.number_input("Montant (DH)",min_value=0.01,key="ca_montant")
                c5,c6=st.columns(2)
                categorie_ca=c5.selectbox("Catégorie",["Main d'œuvre","Matériaux","Transport","Sous-traitance","Divers"],key="ca_cat")
                beneficiaire_ca=c6.text_input("Bénéficiaire / Description",key="ca_benef")
                ref_ca=st.text_input("Référence pièce justificative",key="ca_ref")
                obs_ca=st.text_area("Observations",height=60,key="ca_obs")
                if st.form_submit_button("💾 Enregistrer"):
                    rid_ca=id_map.get(ch_ca)
                    exsql("INSERT INTO caisse_chantier(date_op,rue_id,type_op,rubrique,montant,categorie,beneficiaire,reference_piece,observation)VALUES(?,?,?,?,?,?,?,?,?)",
                          [str(date_ca),rid_ca,type_ca,categorie_ca,montant_ca,categorie_ca,beneficiaire_ca,ref_ca,obs_ca])
                    st.success("✅ Opération enregistrée."); st.rerun()

    with t2:
        st.subheader("📊 Tableau de bord caisse")
        col1,col2=st.columns(2)
        if labs:
            ch_ca2=col1.selectbox("Chantier",["Tous"]+labs,key="ca_list_ch")
        else:
            ch_ca2="Tous"
        mois_ca=col2.selectbox("Mois",["Tous"]+[f"{y}-{m:02d}" for y in range(2023,date.today().year+1) for m in range(1,13) if f"{y}-{m:02d}"<=date.today().strftime("%Y-%m")],key="ca_mois")
        if ch_ca2=="Tous" or not labs:
            base_q="SELECT cc.id,cc.date_op AS Date,COALESCE(r.nom,'—') AS Chantier,cc.type_op AS Type,cc.categorie AS Catégorie,cc.montant AS Montant,cc.beneficiaire AS Bénéficiaire,cc.reference_piece AS Référence FROM caisse_chantier cc LEFT JOIN rues r ON r.id=cc.rue_id"
            if mois_ca!="Tous":
                df_ca=qdf(f"{base_q} WHERE strftime('%Y-%m',cc.date_op)=? ORDER BY cc.date_op DESC LIMIT 200",[mois_ca])
            else:
                df_ca=qdf(f"{base_q} ORDER BY cc.date_op DESC LIMIT 200")
        else:
            rid_ca2=id_map.get(ch_ca2)
            base_q="SELECT cc.id,cc.date_op AS Date,COALESCE(r.nom,'—') AS Chantier,cc.type_op AS Type,cc.categorie AS Catégorie,cc.montant AS Montant,cc.beneficiaire AS Bénéficiaire,cc.reference_piece AS Référence FROM caisse_chantier cc LEFT JOIN rues r ON r.id=cc.rue_id WHERE cc.rue_id=?"
            if mois_ca!="Tous":
                df_ca=qdf(f"{base_q} AND strftime('%Y-%m',cc.date_op)=? ORDER BY cc.date_op DESC LIMIT 200",[rid_ca2,mois_ca])
            else:
                df_ca=qdf(f"{base_q} ORDER BY cc.date_op DESC LIMIT 200",[rid_ca2])
        if df_ca.empty: st.info("Aucune opération.")
        else:
            dep=df_ca[df_ca["Type"]=="Dépense"]["Montant"].sum()
            rec=df_ca[df_ca["Type"]=="Recette"]["Montant"].sum()
            avance=df_ca[df_ca["Type"]=="Avance"]["Montant"].sum()
            solde=rec - dep - avance
            col_d,col_r,col_av,col_s=st.columns(4)
            col_d.metric("💸 Total Dépenses",f"{dep:,.0f}",delta=None)
            col_r.metric("💵 Total Recettes",f"{rec:,.0f}",delta=None)
            col_av.metric("🔄 Total Avances",f"{avance:,.0f}",delta=None)
            col_s.metric("💰 Solde caisse",f"{solde:,.0f}",
                         delta="Excédent ✅" if solde>=0 else "Déficit ⚠️",
                         delta_color="normal" if solde>=0 else "inverse")
            st.markdown("---")

            # Tableau avec solde cumulatif ligne par ligne
            df_solde=df_ca.drop(columns=["id"],errors="ignore").copy()
            # Trier par date croissante pour calculer le solde cumulatif
            df_solde_sorted=df_solde.sort_values("Date").copy()
            def _impact(t,m):
                if t=="Recette": return float(m)
                elif t in ("Dépense","Avance"): return -float(m)
                return 0
            df_solde_sorted["Impact"]=df_solde_sorted.apply(lambda r:_impact(r["Type"],r["Montant"]),axis=1)
            df_solde_sorted["Solde cumulé"]=df_solde_sorted["Impact"].cumsum().round(0)
            df_solde_sorted=df_solde_sorted.drop(columns=["Impact"])
            # Réafficher du plus récent au plus ancien
            st.dataframe(df_solde_sorted.sort_values("Date",ascending=False),
                         use_container_width=True,hide_index=True)

    with t3:
        st.subheader("✏️ Modifier / Supprimer une opération")
        df_ca_all=qdf("SELECT cc.id,cc.date_op,COALESCE(r.nom,'—') AS chantier,cc.type_op,cc.montant,cc.categorie,cc.beneficiaire,cc.reference_piece,cc.observation FROM caisse_chantier cc LEFT JOIN rues r ON r.id=cc.rue_id ORDER BY cc.date_op DESC LIMIT 100")
        if df_ca_all.empty: st.info("Aucune opération.")
        else:
            df_ca_all["label"]=df_ca_all.apply(lambda r:f"{r['date_op']} | {r['chantier']} | {r['type_op']} | {r['montant']:.2f} DH",axis=1)
            sel_ca=st.selectbox("Opération à modifier",df_ca_all["label"].tolist(),key="ca_edit_sel")
            rca=df_ca_all[df_ca_all["label"]==sel_ca].iloc[0]
            with st.form("f_ca_edit"):
                c1,c2=st.columns(2)
                try: v_dc=date.fromisoformat(str(rca["date_op"]))
                except: v_dc=date.today()
                ec_date=c1.date_input("Date",value=v_dc,key="ca_e_date")
                ec_montant=c2.number_input("Montant",min_value=0.01,value=float(rca["montant"] or 1),key="ca_e_montant")
                types_ca=["Dépense","Recette","Avance"]
                cats_ca=["Main d'œuvre","Matériaux","Transport","Sous-traitance","Divers"]
                c3,c4=st.columns(2)
                cur_tc=str(rca.get("type_op") or "Dépense")
                ec_type=c3.selectbox("Type",types_ca,index=types_ca.index(cur_tc) if cur_tc in types_ca else 0,key="ca_e_type")
                cur_cat=str(rca.get("categorie") or "Divers")
                ec_cat=c4.selectbox("Catégorie",cats_ca,index=cats_ca.index(cur_cat) if cur_cat in cats_ca else 0,key="ca_e_cat")
                ec_benef=st.text_input("Bénéficiaire",value=str(rca.get("beneficiaire") or ""),key="ca_e_benef")
                ec_ref=st.text_input("Référence",value=str(rca.get("reference_piece") or ""),key="ca_e_ref")
                ec_obs=st.text_area("Observations",value=str(rca.get("observation") or ""),height=60,key="ca_e_obs")
                ca_a,ca_b=st.columns(2)
                sv_ca=ca_a.form_submit_button("✅ Enregistrer")
                dl_ca=ca_b.form_submit_button("🗑️ Supprimer")
            if sv_ca:
                exsql("UPDATE caisse_chantier SET date_op=?,type_op=?,rubrique=?,montant=?,categorie=?,beneficiaire=?,reference_piece=?,observation=? WHERE id=?",
                      [str(ec_date),ec_type,ec_cat,ec_montant,ec_cat,ec_benef,ec_ref,ec_obs,int(rca["id"])])
                st.success("✅ Modifié."); st.rerun()
            if dl_ca:
                exsql("DELETE FROM caisse_chantier WHERE id=?",[int(rca["id"])]); st.success("Supprimé."); st.rerun()


# ── SÉCURITÉ & INCIDENTS ──────────────────────────────────────────
elif page=="incidents":
    st.header("⚠️ Sécurité & Incidents")
    labs,id_map=ch_label_map()
    t1,t2,t3=st.tabs(["➕ Déclarer incident","📋 Liste","✏️ Modifier / Clôturer"])

    with t1:
        st.subheader("➕ Déclarer un incident")
        if not labs: st.warning("Aucun chantier disponible.")
        else:
            with st.form("f_inc"):
                c1,c2=st.columns(2)
                ch_inc=c1.selectbox("Chantier",labs,key="inc_ch")
                date_inc=c2.date_input("Date incident",value=date.today(),key="inc_date")
                c3,c4=st.columns(2)
                type_inc=c3.selectbox("Type",["Accident corporel","Accident matériel","Presqu'accident","Incident environnemental","Violation sécurité","Autre"],key="inc_type")
                gravite_inc=c4.selectbox("Gravité",["Mineur","Modéré","Grave","Critique"],key="inc_grav")
                desc_inc=st.text_area("Description de l'incident",height=120,key="inc_desc")
                c5,c6=st.columns(2)
                victimes_inc=c5.number_input("Nombre de victimes",min_value=0,step=1,key="inc_vict")
                cout_inc=c6.number_input("Coût estimé (DH)",min_value=0.0,key="inc_cout")
                actions_inc=st.text_area("Actions correctives prévues",height=80,key="inc_actions")
                if st.form_submit_button("💾 Déclarer"):
                    rid_inc=id_map.get(ch_inc)
                    exsql("INSERT INTO incidents(date_incident,rue_id,type_incident,gravite,description,nb_victimes,cout_estime,actions_correctives,statut)VALUES(?,?,?,?,?,?,?,?,?)",
                          [str(date_inc),rid_inc,type_inc,gravite_inc,desc_inc,victimes_inc,cout_inc,actions_inc,"Ouvert"])
                    st.success("✅ Incident déclaré."); st.rerun()

    with t2:
        st.subheader("📋 Liste des incidents")
        col1,col2=st.columns(2)
        if labs:
            ch_inc2=col1.selectbox("Chantier",["Tous"]+labs,key="inc_list_ch")
        else:
            ch_inc2="Tous"
        statut_inc=col2.selectbox("Statut",["Tous","Ouvert","En cours","Clôturé"],key="inc_statut")
        if ch_inc2=="Tous" or not labs:
            base_qi="SELECT i.id,i.date_incident AS Date,COALESCE(r.nom,'—') AS Chantier,i.type_incident AS Type,i.gravite AS Gravité,i.nb_victimes AS Victimes,i.statut AS Statut,i.description AS Description FROM incidents i LEFT JOIN rues r ON r.id=i.rue_id"
            if statut_inc!="Tous": df_inc=qdf(f"{base_qi} WHERE i.statut=? ORDER BY i.date_incident DESC LIMIT 200",[statut_inc])
            else: df_inc=qdf(f"{base_qi} ORDER BY i.date_incident DESC LIMIT 200")
        else:
            rid_inc2=id_map.get(ch_inc2)
            base_qi="SELECT i.id,i.date_incident AS Date,COALESCE(r.nom,'—') AS Chantier,i.type_incident AS Type,i.gravite AS Gravité,i.nb_victimes AS Victimes,i.statut AS Statut,i.description AS Description FROM incidents i LEFT JOIN rues r ON r.id=i.rue_id WHERE i.rue_id=?"
            if statut_inc!="Tous": df_inc=qdf(f"{base_qi} AND i.statut=? ORDER BY i.date_incident DESC LIMIT 200",[rid_inc2,statut_inc])
            else: df_inc=qdf(f"{base_qi} ORDER BY i.date_incident DESC LIMIT 200",[rid_inc2])
        if df_inc.empty: st.info("Aucun incident.")
        else:
            st.metric("Nombre d'incidents",len(df_inc))
            st.dataframe(df_inc.drop(columns=["id"],errors="ignore"),use_container_width=True)

    with t3:
        st.subheader("✏️ Modifier / Clôturer un incident")
        df_inc_all=qdf("SELECT i.id,i.date_incident,COALESCE(r.nom,'—') AS chantier,i.type_incident,i.gravite,i.nb_victimes,i.cout_estime,i.description,i.actions_correctives,i.statut FROM incidents i LEFT JOIN rues r ON r.id=i.rue_id ORDER BY i.date_incident DESC LIMIT 100")
        if df_inc_all.empty: st.info("Aucun incident.")
        else:
            df_inc_all["label"]=df_inc_all.apply(lambda r:f"{r['date_incident']} | {r['chantier']} | {r['type_incident']} | {r['statut']}",axis=1)
            sel_inc=st.selectbox("Incident à modifier",df_inc_all["label"].tolist(),key="inc_edit_sel")
            ri=df_inc_all[df_inc_all["label"]==sel_inc].iloc[0]
            with st.form("f_inc_edit"):
                c1,c2=st.columns(2)
                try: v_di=date.fromisoformat(str(ri["date_incident"]))
                except: v_di=date.today()
                ei_date=c1.date_input("Date",value=v_di,key="inc_e_date")
                gravs=["Mineur","Modéré","Grave","Critique"]
                cur_grav=str(ri.get("gravite") or "Mineur")
                ei_grav=c2.selectbox("Gravité",gravs,index=gravs.index(cur_grav) if cur_grav in gravs else 0,key="inc_e_grav")
                ei_vict=st.number_input("Victimes",min_value=0,value=int(ri.get("nb_victimes") or 0),key="inc_e_vict")
                ei_cout=st.number_input("Coût estimé",min_value=0.0,value=float(ri.get("cout_estime") or 0),key="inc_e_cout")
                ei_desc=st.text_area("Description",value=str(ri.get("description") or ""),height=100,key="inc_e_desc")
                ei_act=st.text_area("Actions correctives",value=str(ri.get("actions_correctives") or ""),height=80,key="inc_e_act")
                statuts=["Ouvert","En cours","Clôturé"]
                cur_st=str(ri.get("statut") or "Ouvert")
                ei_stat=st.selectbox("Statut",statuts,index=statuts.index(cur_st) if cur_st in statuts else 0,key="inc_e_stat")
                ca,cb=st.columns(2)
                sv_inc=ca.form_submit_button("✅ Enregistrer")
                dl_inc=cb.form_submit_button("🗑️ Supprimer")
            if sv_inc:
                exsql("UPDATE incidents SET date_incident=?,gravite=?,nb_victimes=?,cout_estime=?,description=?,actions_correctives=?,statut=? WHERE id=?",
                      [str(ei_date),ei_grav,ei_vict,ei_cout,ei_desc,ei_act,ei_stat,int(ri["id"])])
                st.success("✅ Incident mis à jour."); st.rerun()
            if dl_inc:
                exsql("DELETE FROM incidents WHERE id=?",[int(ri["id"])]); st.success("Supprimé."); st.rerun()


# ── COURRIERS ─────────────────────────────────────────────────────
elif page=="courriers":
    st.header("📬 Gestion des Courriers")
    labs,id_map=ch_label_map()
    t1,t2,t3=st.tabs(["➕ Nouveau courrier","📋 Liste","✏️ Modifier"])

    with t1:
        st.subheader("➕ Enregistrer un courrier")
        with st.form("f_courrier"):
            c1,c2=st.columns(2)
            date_cour=c1.date_input("Date",value=date.today(),key="cour_date")
            ref_cour=c2.text_input("Référence / N° courrier",key="cour_ref")
            c3,c4=st.columns(2)
            type_cour=c3.selectbox("Type",["Entrant","Sortant","Interne"],key="cour_type")
            priorite_cour=c4.selectbox("Priorité",["Normale","Urgente","Confidentielle"],key="cour_prio")
            c5,c6=st.columns(2)
            expediteur_cour=c5.text_input("Expéditeur",key="cour_exped")
            destinataire_cour=c6.text_input("Destinataire",key="cour_dest")
            if labs:
                ch_cour=st.selectbox("Chantier concerné",["(Général)"]+labs,key="cour_ch")
            else:
                ch_cour="(Général)"
            objet_cour=st.text_input("Objet",key="cour_objet")
            resume_cour=st.text_area("Résumé / Corps",height=100,key="cour_resume")
            actions_cour=st.text_area("Actions requises",height=60,key="cour_actions")
            if st.form_submit_button("💾 Enregistrer"):
                rid_cour=id_map.get(ch_cour) if ch_cour!="(Général)" else None
                exsql("INSERT INTO courriers(date_courrier,reference,type_courrier,priorite,expediteur,destinataire,rue_id,objet,resume,actions_requises,statut)VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                      [str(date_cour),ref_cour,type_cour,priorite_cour,expediteur_cour,destinataire_cour,rid_cour,objet_cour,resume_cour,actions_cour,"Nouveau"])
                st.success("✅ Courrier enregistré."); st.rerun()

    with t2:
        st.subheader("📋 Liste des courriers")
        col1,col2,col3=st.columns(3)
        type_f=col1.selectbox("Type",["Tous","Entrant","Sortant","Interne"],key="cour_f_type")
        prio_f=col2.selectbox("Priorité",["Tous","Normale","Urgente","Confidentielle"],key="cour_f_prio")
        stat_f=col3.selectbox("Statut",["Tous","Nouveau","En traitement","Clôturé"],key="cour_f_stat")
        conditions=[]; params=[]
        if type_f!="Tous": conditions.append("c.type_courrier=?"); params.append(type_f)
        if prio_f!="Tous": conditions.append("c.priorite=?"); params.append(prio_f)
        if stat_f!="Tous": conditions.append("c.statut=?"); params.append(stat_f)
        where_cl="WHERE "+" AND ".join(conditions) if conditions else ""
        df_cour=qdf(f"SELECT c.id,c.date_courrier AS Date,c.reference AS Réf,c.type_courrier AS Type,c.priorite AS Priorité,c.expediteur AS Expéditeur,c.destinataire AS Destinataire,COALESCE(r.nom,'Général') AS Chantier,c.objet AS Objet,c.statut AS Statut FROM courriers c LEFT JOIN rues r ON r.id=c.rue_id {where_cl} ORDER BY c.date_courrier DESC LIMIT 200",params if params else None)
        if df_cour.empty: st.info("Aucun courrier.")
        else: st.dataframe(df_cour.drop(columns=["id"],errors="ignore"),use_container_width=True)

    with t3:
        st.subheader("✏️ Modifier un courrier")
        df_cour_all=qdf("SELECT c.id,c.date_courrier,c.reference,c.type_courrier,c.priorite,c.expediteur,c.destinataire,c.objet,c.resume,c.actions_requises,c.statut,COALESCE(r.nom,'Général') AS chantier FROM courriers c LEFT JOIN rues r ON r.id=c.rue_id ORDER BY c.date_courrier DESC LIMIT 100")
        if df_cour_all.empty: st.info("Aucun courrier.")
        else:
            df_cour_all["label"]=df_cour_all.apply(lambda r:f"{r['date_courrier']} | {r['reference']} | {r['objet'][:40]}",axis=1)
            sel_cour=st.selectbox("Courrier à modifier",df_cour_all["label"].tolist(),key="cour_edit_sel")
            rc=df_cour_all[df_cour_all["label"]==sel_cour].iloc[0]
            with st.form("f_cour_edit"):
                c1,c2=st.columns(2)
                try: v_dc=date.fromisoformat(str(rc["date_courrier"]))
                except: v_dc=date.today()
                ec_date=c1.date_input("Date",value=v_dc,key="cour_e_date")
                ec_ref=c2.text_input("Référence",value=str(rc.get("reference") or ""),key="cour_e_ref")
                types_c=["Entrant","Sortant","Interne"]; prios_c=["Normale","Urgente","Confidentielle"]; stats_c=["Nouveau","En traitement","Clôturé"]
                c3,c4=st.columns(2)
                cur_tc=str(rc.get("type_courrier") or "Entrant")
                ec_type=c3.selectbox("Type",types_c,index=types_c.index(cur_tc) if cur_tc in types_c else 0,key="cour_e_type")
                cur_pc=str(rc.get("priorite") or "Normale")
                ec_prio=c4.selectbox("Priorité",prios_c,index=prios_c.index(cur_pc) if cur_pc in prios_c else 0,key="cour_e_prio")
                ec_exped=st.text_input("Expéditeur",value=str(rc.get("expediteur") or ""),key="cour_e_exped")
                ec_dest=st.text_input("Destinataire",value=str(rc.get("destinataire") or ""),key="cour_e_dest")
                ec_objet=st.text_input("Objet",value=str(rc.get("objet") or ""),key="cour_e_objet")
                ec_resume=st.text_area("Résumé",value=str(rc.get("resume") or ""),height=80,key="cour_e_resume")
                ec_actions=st.text_area("Actions",value=str(rc.get("actions_requises") or ""),height=60,key="cour_e_actions")
                cur_sc=str(rc.get("statut") or "Nouveau")
                ec_stat=st.selectbox("Statut",stats_c,index=stats_c.index(cur_sc) if cur_sc in stats_c else 0,key="cour_e_stat")
                ca,cb=st.columns(2)
                sv_cour=ca.form_submit_button("✅ Enregistrer")
                dl_cour=cb.form_submit_button("🗑️ Supprimer")
            if sv_cour:
                exsql("UPDATE courriers SET date_courrier=?,reference=?,type_courrier=?,priorite=?,expediteur=?,destinataire=?,objet=?,resume=?,actions_requises=?,statut=? WHERE id=?",
                      [str(ec_date),ec_ref,ec_type,ec_prio,ec_exped,ec_dest,ec_objet,ec_resume,ec_actions,ec_stat,int(rc["id"])])
                st.success("✅ Modifié."); st.rerun()
            if dl_cour:
                exsql("DELETE FROM courriers WHERE id=?",[int(rc["id"])]); st.success("Supprimé."); st.rerun()


# ── RAPPORTS ──────────────────────────────────────────────────────
elif page=="rapports":
    st.header("📈 Rapports & Exports")
    labs,id_map=ch_label_map()

    if not labs:
        st.warning("Aucun chantier disponible.")
    else:
        col1,col2=st.columns(2)
        ch_rap=col1.selectbox("Chantier",["Tous"]+labs,key="rap_ch")
        rid_rap=id_map.get(ch_rap) if ch_rap!="Tous" else None

        RAPPORTS=[
            "1. Suivi avancement décompte",
            "2. Situation sous-traitants",
            "3. État du personnel & pointage",
            "4. Stock matériaux & mouvements",
            "5. Suivi engins & carburant",
            "6. Journal de chantier",
            "7. Caisse chantier",
            "8. Incidents sécurité",
            "9. Devis marché vs réalisations",
            "10. Circuit approvisionnement",
            "11. Rapport global chantier",
        ]
        type_rap=col2.selectbox("Type de rapport",RAPPORTS,key="rap_type")
        date_deb=col1.date_input("Date début",value=date(date.today().year,1,1),key="rap_deb")
        date_fin=col2.date_input("Date fin",value=date.today(),key="rap_fin")
        d_deb=str(date_deb); d_fin=str(date_fin)

        # ── Fonction de construction des données ─────────────────────
        def build_report_sheets(type_rap,rid_rap,d_deb,d_fin):
            """Retourne un dict {nom_feuille: DataFrame} pour le rapport demandé."""
            sheets={}
            try:
                # ── 1. Avancement décompte ──────────────────────────
                if type_rap.startswith("1"):
                    q="""
                        SELECT COALESCE(r.nom,'—') AS Chantier,
                               COALESCE(l.nom,'Sans livrable') AS Livrable,
                               dr.code_poste AS Code,
                               dr.designation AS Désignation,
                               dr.unite AS Unité,
                               dr.quantite_marche AS Qté_Marché,
                               dr.prix_unitaire AS PU,
                               COALESCE(SUM(rj.quantite_jour),0) AS Qté_Réalisée
                        FROM devis_rue dr
                        LEFT JOIN rues r ON r.id=dr.rue_id
                        LEFT JOIN livrables l ON l.id=dr.livrable_id
                        LEFT JOIN realisations_journalieres rj ON rj.devis_id=dr.id
                    """
                    p=[]
                    if rid_rap: q+=" WHERE dr.rue_id=?"; p.append(rid_rap)
                    q+=" GROUP BY dr.id ORDER BY COALESCE(dr.livrable_id,0),dr.id"
                    df_r=qdf(q,p)
                    if not df_r.empty:
                        df_r["Montant_Marché"]=df_r["Qté_Marché"]*df_r["PU"]
                        df_r["Montant_Réalisé"]=df_r["Qté_Réalisée"]*df_r["PU"]
                        df_r["Reste"]=df_r["Montant_Marché"]-df_r["Montant_Réalisé"]
                        df_r["%_Avancement"]=(df_r["Qté_Réalisée"]/df_r["Qté_Marché"].replace(0,1)*100).round(1)

                        # ── Sous-totaux par livrable + Total Décompte Marché ──
                        _num=["Qté_Marché","Montant_Marché","Qté_Réalisée","Montant_Réalisé","Reste"]
                        _cols=df_r.columns.tolist()

                        # Groupement séquentiel (préserve l'ordre du tri)
                        _groups=[]; _cur_liv=None; _cur_rows=[]
                        for _,_row in df_r.iterrows():
                            _lv=_row["Livrable"]
                            if _lv!=_cur_liv:
                                if _cur_rows: _groups.append((_cur_liv,pd.DataFrame(_cur_rows)))
                                _cur_liv=_lv; _cur_rows=[]
                            _cur_rows.append(_row.to_dict())
                        if _cur_rows: _groups.append((_cur_liv,pd.DataFrame(_cur_rows)))

                        _pieces=[]; _grand={nc:0.0 for nc in _num}
                        for _liv_nom,_grp in _groups:
                            _pieces.append(_grp)
                            # Ligne sous-total livrable
                            _sub={c:"" for c in _cols}
                            _sub["Désignation"]=f"▶ TOTAL {str(_liv_nom).upper()}"
                            _sub["Livrable"]=str(_liv_nom)
                            for _nc in _num:
                                _v=pd.to_numeric(_grp[_nc],errors="coerce").sum()
                                _sub[_nc]=round(_v,2); _grand[_nc]+=_v
                            _smm=float(_sub.get("Montant_Marché") or 0)
                            _smr=float(_sub.get("Montant_Réalisé") or 0)
                            _sub["%_Avancement"]=round(_smr/_smm*100,1) if _smm else 0
                            _pieces.append(pd.DataFrame([_sub]))

                        # Ligne total général
                        _tot={c:"" for c in _cols}
                        _tot["Désignation"]="▶▶ TOTAL DÉCOMPTE MARCHÉ"
                        for _nc in _num: _tot[_nc]=round(_grand[_nc],2)
                        _gmm=_grand.get("Montant_Marché",0); _gmr=_grand.get("Montant_Réalisé",0)
                        _tot["%_Avancement"]=round(_gmr/_gmm*100,1) if _gmm else 0
                        _pieces.append(pd.DataFrame([_tot]))

                        df_r=pd.concat(_pieces,ignore_index=True)
                    sheets["Avancement"]=df_r

                # ── 2. Sous-traitants ───────────────────────────────
                elif type_rap.startswith("2"):
                    q="""
                        SELECT st.nom AS Sous_Traitant, st.specialite AS Spécialité,
                               st.montant_contrat AS Montant_Contrat,
                               COALESCE(SUM(dst.quantite*dst.prix_unitaire),0) AS Montant_Devis_ST,
                               COALESCE(SUM(dc.montant),0) AS Total_Décompté,
                               COALESCE(SUM(p.montant),0) AS Total_Payé
                        FROM sous_traitants st
                        LEFT JOIN devis_st dst ON dst.st_id=st.id
                        LEFT JOIN decomptes_st dc ON dc.st_id=st.id
                        LEFT JOIN paiements_st p ON p.st_id=st.id
                    """
                    p=[]
                    if rid_rap: q+=" WHERE st.rue_id=?"; p.append(rid_rap)
                    q+=" GROUP BY st.id ORDER BY st.nom"
                    df_r=qdf(q,p)
                    if not df_r.empty:
                        df_r["Solde_à_payer"]=df_r["Total_Décompté"]-df_r["Total_Payé"]
                    sheets["Sous-Traitants"]=df_r

                # ── 3. Personnel & pointage ─────────────────────────
                elif type_rap.startswith("3"):
                    q=f"""
                        SELECT p.nom||' '||COALESCE(p.prenom,'') AS Personnel,
                               p.categorie AS Catégorie, p.poste AS Poste,
                               pt.date_pointage AS Date, pt.statut AS Statut,
                               pt.heures_travaillees AS Heures
                        FROM personnel p
                        LEFT JOIN pointage pt ON pt.personnel_id=p.id
                        WHERE pt.date_pointage BETWEEN '{d_deb}' AND '{d_fin}'
                    """
                    if rid_rap: q+=f" AND pt.rue_id={rid_rap}"
                    sheets["Personnel"]=qdf(q+" ORDER BY pt.date_pointage,p.nom")

                # ── 4. Stock matériaux ──────────────────────────────
                elif type_rap.startswith("4"):
                    df_r=qdf("""
                        SELECT m.nom AS Matériau, m.unite AS Unité, m.categorie AS Catégorie,
                               m.stock_initial AS Stock_Initial,
                               COALESCE(SUM(CASE WHEN mm.type_mvt='ENTREE' THEN mm.quantite ELSE 0 END),0) AS Total_Entrées,
                               COALESCE(SUM(CASE WHEN mm.type_mvt='SORTIE' THEN mm.quantite ELSE 0 END),0) AS Total_Sorties
                        FROM materiaux m
                        LEFT JOIN mouvements_materiaux mm ON mm.materiau_id=m.id
                        GROUP BY m.id ORDER BY m.nom
                    """)
                    if not df_r.empty:
                        df_r["Stock_Actuel"]=df_r["Stock_Initial"]+df_r["Total_Entrées"]-df_r["Total_Sorties"]
                    sheets["Stock"]=df_r

                # ── 5. Suivi engins ─────────────────────────────────
                elif type_rap.startswith("5"):
                    q=f"""
                        SELECT m.nom AS Engin, m.immatriculation AS Immat,
                               COALESCE(r.nom,'—') AS Chantier,
                               sm.date_suivi AS Date,
                               sm.heures_marche AS Hrs_Marche, sm.heures_arret AS Hrs_Arrêt,
                               sm.carburant_l AS Carburant_L, sm.cout_carburant AS Coût_Carb,
                               sm.chauffeur AS Chauffeur, sm.panne AS Panne
                        FROM suivi_materiels sm
                        JOIN materiels m ON m.id=sm.materiel_id
                        LEFT JOIN rues r ON r.id=sm.rue_id
                        WHERE sm.date_suivi BETWEEN '{d_deb}' AND '{d_fin}'
                    """
                    if rid_rap: q+=f" AND sm.rue_id={rid_rap}"
                    sheets["Engins"]=qdf(q+" ORDER BY sm.date_suivi,m.nom")

                # ── 6. Journal de chantier ──────────────────────────
                elif type_rap.startswith("6"):
                    q=f"""
                        SELECT jc.date_journal AS Date,
                               COALESCE(r.nom,'—') AS Chantier,
                               jc.meteo AS Météo,
                               jc.nb_ouvriers_presents AS Ouvriers,
                               jc.travaux_executes AS Travaux_Exécutés,
                               jc.problemes AS Problèmes,
                               jc.decisions AS Décisions,
                               jc.visiteurs AS Visiteurs,
                               jc.redacteur AS Rédacteur,
                               jc.observation AS Observation
                        FROM journal_chantier jc
                        LEFT JOIN rues r ON r.id=jc.rue_id
                        WHERE jc.date_journal BETWEEN '{d_deb}' AND '{d_fin}'
                    """
                    if rid_rap: q+=f" AND jc.rue_id={rid_rap}"
                    sheets["Journal"]=qdf(q+" ORDER BY jc.date_journal")

                # ── 7. Caisse chantier ──────────────────────────────
                elif type_rap.startswith("7"):
                    q=f"""
                        SELECT cc.date_op AS Date,
                               COALESCE(r.nom,'—') AS Chantier,
                               cc.type_op AS Type,
                               cc.rubrique AS Rubrique,
                               cc.montant AS Montant,
                               cc.beneficiaire AS Bénéficiaire,
                               cc.mode_paiement AS Mode_Paiement,
                               cc.reference_piece AS Référence
                        FROM caisse_chantier cc
                        LEFT JOIN rues r ON r.id=cc.rue_id
                        WHERE cc.date_op BETWEEN '{d_deb}' AND '{d_fin}'
                    """
                    if rid_rap: q+=f" AND cc.rue_id={rid_rap}"
                    sheets["Caisse"]=qdf(q+" ORDER BY cc.date_op")

                # ── 8. Incidents sécurité ───────────────────────────
                elif type_rap.startswith("8"):
                    q=f"""
                        SELECT i.date_incident AS Date,
                               COALESCE(r.nom,'—') AS Chantier,
                               i.type_incident AS Type,
                               i.gravite AS Gravité,
                               i.description AS Description,
                               i.personne_concernee AS Personne_Concernée,
                               i.mesures_prises AS Mesures_Prises,
                               CASE WHEN i.cloture=1 THEN 'Clôturé' ELSE 'Ouvert' END AS Statut
                        FROM incidents i
                        LEFT JOIN rues r ON r.id=i.rue_id
                        WHERE i.date_incident BETWEEN '{d_deb}' AND '{d_fin}'
                    """
                    if rid_rap: q+=f" AND i.rue_id={rid_rap}"
                    sheets["Incidents"]=qdf(q+" ORDER BY i.date_incident")

                # ── 9. Devis marché vs réalisations ────────────────
                elif type_rap.startswith("9"):
                    q="""
                        SELECT COALESCE(r.nom,'—') AS Chantier,
                               dr.code_poste AS Code,
                               dr.designation AS Désignation,
                               dr.unite AS Unité,
                               dr.quantite_marche AS Qté_Marché,
                               dr.prix_unitaire AS PU,
                               dr.quantite_marche*dr.prix_unitaire AS Montant_Marché,
                               COALESCE(SUM(rj.quantite_jour),0) AS Qté_Réalisée
                        FROM devis_rue dr
                        LEFT JOIN rues r ON r.id=dr.rue_id
                        LEFT JOIN realisations_journalieres rj ON rj.devis_id=dr.id
                    """
                    p=[]
                    if rid_rap: q+=" WHERE dr.rue_id=?"; p.append(rid_rap)
                    q+=" GROUP BY dr.id ORDER BY r.nom,dr.id"
                    df_r=qdf(q,p)
                    if not df_r.empty:
                        df_r["Montant_Réalisé"]=df_r["Qté_Réalisée"]*df_r["PU"]
                        df_r["Écart"]=df_r["Montant_Marché"]-df_r["Montant_Réalisé"]
                        df_r["%_Avancement"]=(df_r["Qté_Réalisée"]/df_r["Qté_Marché"].replace(0,1)*100).round(1)
                    sheets["Devis vs Réalisé"]=df_r

                # ── 10. Circuit approvisionnement ───────────────────
                elif type_rap.startswith("10"):
                    q=f"""
                        SELECT a.date_besoin AS Date_Besoin,
                               COALESCE(r.nom,'—') AS Chantier,
                               a.designation AS Désignation,
                               a.unite AS Unité,
                               a.quantite_demandee AS Qté_Demandée,
                               a.quantite_recue AS Qté_Reçue,
                               a.fournisseur AS Fournisseur,
                               a.statut AS Statut,
                               a.prix_unitaire_reel AS PU_Réel,
                               a.date_reception AS Date_Réception
                        FROM approvisionnements a
                        LEFT JOIN rues r ON r.id=a.rue_id
                        WHERE a.date_besoin BETWEEN '{d_deb}' AND '{d_fin}'
                    """
                    if rid_rap: q+=f" AND a.rue_id={rid_rap}"
                    sheets["Approvisionnement"]=qdf(q+" ORDER BY a.date_besoin")

                # ── 11. Rapport global chantier ─────────────────────
                else:
                    # Feuille 1 : Chantiers
                    qc="SELECT r.nom AS Chantier,COALESCE(d.nom,'—') AS Dossier,r.numero_marche AS N_Marché,r.maitre_ouvrage AS Maître_Ouvrage,r.entreprise AS Entreprise,r.date_notification AS Date_Notif,r.date_demarrage AS Date_Démarrage,r.delai_jours AS Délai_Jours FROM rues r LEFT JOIN dossiers d ON d.id=r.dossier_id"
                    sheets["Chantiers"]=qdf(qc+(f" WHERE r.id={rid_rap}" if rid_rap else ""))
                    # Feuille 2 : Avancement global
                    qa="SELECT COALESCE(r.nom,'—') AS Chantier,dr.designation AS Désignation,dr.unite AS Unité,dr.quantite_marche AS Qté_Marché,dr.prix_unitaire AS PU,COALESCE(SUM(rj.quantite_jour),0) AS Qté_Réalisée,dr.quantite_marche*dr.prix_unitaire AS Montant_Marché FROM devis_rue dr LEFT JOIN rues r ON r.id=dr.rue_id LEFT JOIN realisations_journalieres rj ON rj.devis_id=dr.id"
                    p2=[]; qa+=" WHERE 1=1"
                    if rid_rap: qa+=" AND dr.rue_id=?"; p2.append(rid_rap)
                    qa+=" GROUP BY dr.id ORDER BY r.nom,dr.id"
                    df_av=qdf(qa,p2)
                    if not df_av.empty:
                        df_av["Montant_Réalisé"]=df_av["Qté_Réalisée"]*df_av["PU"]
                    sheets["Avancement"]=df_av
                    # Feuille 3 : Stock
                    sheets["Stock"]=qdf("SELECT m.nom AS Matériau,m.unite AS Unité,m.stock_initial AS Stock_Initial,COALESCE(SUM(CASE WHEN mm.type_mvt='ENTREE' THEN mm.quantite ELSE 0 END),0)-COALESCE(SUM(CASE WHEN mm.type_mvt='SORTIE' THEN mm.quantite ELSE 0 END),0) AS Mouvements_Net FROM materiaux m LEFT JOIN mouvements_materiaux mm ON mm.materiau_id=m.id GROUP BY m.id ORDER BY m.nom")
                    # Feuille 4 : Personnel
                    sheets["Personnel"]=qdf("SELECT nom AS Nom,prenom AS Prénom,categorie AS Catégorie,poste AS Poste,telephone AS Téléphone FROM personnel ORDER BY categorie,nom")
                    # Feuille 5 : Sous-traitants
                    sheets["Sous-Traitants"]=qdf("SELECT nom AS Nom,specialite AS Spécialité,responsable AS Responsable,telephone AS Téléphone,montant_contrat AS Montant_Contrat,statut AS Statut FROM sous_traitants ORDER BY nom")
                    # Feuille 6 : Engins
                    sheets["Engins"]=qdf("SELECT nom AS Nom,immatriculation AS Immat,type_materiel AS Type,etat AS État FROM materiels ORDER BY nom")

            except Exception as e:
                st.error(f"Erreur lors de la construction du rapport : {e}")
            return sheets

        # ── Boutons Aperçu / Export ───────────────────────────────────
        st.markdown("---")
        bc1,bc2=st.columns(2)
        btn_prev=bc1.button("👁️ Aperçu du rapport",key="rap_prev",use_container_width=True)
        btn_exp=bc2.button("📥 Exporter en Excel",key="rap_gen",use_container_width=True)

        if btn_prev or btn_exp:
            nom_rap=type_rap.split(". ",1)[1] if ". " in type_rap else type_rap
            with st.spinner(f"Construction du rapport « {nom_rap} »…"):
                sheets=build_report_sheets(type_rap,rid_rap,d_deb,d_fin)

            # ── Aperçu interactif ─────────────────────────────────────
            if sheets:
                st.markdown(f"### 👁️ Aperçu — {nom_rap}")
                st.caption(f"Chantier : **{ch_rap}** | Période : {date_deb.strftime('%d/%m/%Y')} → {date_fin.strftime('%d/%m/%Y')}")

                if len(sheets)==1:
                    # Rapport mono-feuille
                    sname,df_prev=next(iter(sheets.items()))
                    if df_prev.empty:
                        st.info("Aucune donnée pour cette période / ce chantier.")
                    else:
                        # Métriques rapides
                        mc=st.columns(min(3,len(df_prev.columns)))
                        num_cols=[c for c in df_prev.columns if pd.api.types.is_numeric_dtype(df_prev[c])]
                        for i,nc in enumerate(num_cols[:3]):
                            mc[i].metric(nc,fmt(df_prev[nc].sum()) if "Montant" in nc or "Coût" in nc or "Solde" in nc or "Payé" in nc else f"{df_prev[nc].sum():,.1f}")
                        st.dataframe(df_prev,use_container_width=True,height=min(600,35+len(df_prev)*35))
                        st.caption(f"**{len(df_prev)}** ligne(s) — feuille « {sname} »")
                else:
                    # Rapport multi-feuilles : onglets
                    tab_names=list(sheets.keys())
                    tabs=st.tabs([f"📋 {n}" for n in tab_names])
                    for tab,sname in zip(tabs,tab_names):
                        with tab:
                            df_prev=sheets[sname]
                            if df_prev is None or df_prev.empty:
                                st.info("Aucune donnée.")
                            else:
                                num_cols=[c for c in df_prev.columns if pd.api.types.is_numeric_dtype(df_prev[c])]
                                if num_cols:
                                    mc=st.columns(min(3,len(num_cols)))
                                    for i,nc in enumerate(num_cols[:3]):
                                        mc[i].metric(nc,fmt(df_prev[nc].sum()) if any(k in nc for k in ["Montant","Coût","Payé","Solde","Contrat"]) else f"{df_prev[nc].sum():,.1f}")
                                st.dataframe(df_prev,use_container_width=True,height=min(500,35+len(df_prev)*35))
                                st.caption(f"**{len(df_prev)}** ligne(s)")

                # ── Export Excel (toujours visible après aperçu) ──────
                st.markdown("---")
                import io as _io
                buf=_io.BytesIO()
                with pd.ExcelWriter(buf,engine="openpyxl") as writer:
                    for sname,df_s in sheets.items():
                        if df_s is not None and not df_s.empty:
                            df_s.to_excel(writer,sheet_name=str(sname)[:31],index=False)
                        else:
                            pd.DataFrame(["Aucune donnée"]).to_excel(writer,sheet_name=str(sname)[:31],index=False,header=False)
                buf.seek(0)
                fname=f"rapport_{nom_rap.replace(' ','_')}_{ch_rap.replace(' ','_')}_{date.today()}.xlsx"
                st.download_button("📥 Télécharger le rapport Excel",data=buf,file_name=fname,
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                   key="rap_dl")
                if btn_exp:
                    st.success(f"✅ Rapport « {nom_rap} » prêt au téléchargement.")
            else:
                st.warning("Aucune donnée disponible pour ce rapport.")


# ── AUDIT TRAIL ───────────────────────────────────────────────────
elif page=="audit":
    st.header("🔍 Journal d'Audit")
    col1,col2,col3=st.columns(3)
    periode_au=col1.selectbox("Période",["Aujourd'hui","7 jours","30 jours","Tout"],key="au_periode")
    table_au=col2.text_input("Filtrer par table",key="au_table")
    action_au=col3.selectbox("Action",["Toutes","INSERT","UPDATE","DELETE"],key="au_action")
    days_au={"Aujourd'hui":0,"7 jours":7,"30 jours":30,"Tout":9999}
    n_days=days_au[periode_au]
    if n_days==0: d_lim_au=date.today().isoformat()
    elif n_days==9999: d_lim_au="2000-01-01"
    else: d_lim_au=(date.today()-timedelta(days=n_days)).isoformat()
    conditions_au=[f"timestamp>='{d_lim_au}'"]
    if table_au: conditions_au.append(f"table_name LIKE '%{table_au}%'")
    if action_au!="Toutes": conditions_au.append(f"action='{action_au}'")
    where_au="WHERE "+" AND ".join(conditions_au)
    df_au=qdf(f"SELECT timestamp AS Horodatage,table_name AS Table,action AS Action,record_id AS ID_Enreg,details AS Détails FROM audit_log {where_au} ORDER BY timestamp DESC LIMIT 500")
    if df_au.empty: st.info("Aucun événement d'audit.")
    else:
        st.metric("Événements trouvés",len(df_au))
        st.dataframe(df_au,use_container_width=True)

# ── MAINTENANCE ──────────────────────────────────────────────────
elif page=="maint":
    st.header("🔧 Maintenance des Engins")

    def get_engins_maint():
        return qdf("SELECT * FROM materiels ORDER BY nom")

    t1, t2, t3 = st.tabs(["➕ Enregistrer intervention","📋 Historique","📊 Par engin"])

    with t1:
        df_em = get_engins_maint()
        if df_em.empty:
            st.warning("Aucun engin enregistré. Ajoutez des engins dans Matériels & Engins.")
        else:
            with st.form("f_maint_page"):
                c1, c2 = st.columns(2)
                eng_mp = c1.selectbox("Engin *", df_em["nom"].tolist(), key="mp_eng")
                dm_p = c2.date_input("Date intervention", value=date.today(), key="mp_date")
                c3, c4 = st.columns(2)
                type_mp = c3.selectbox("Type d'intervention",
                    ["Préventive","Corrective","Vidange","Réparation","Inspection","Autre"], key="mp_type")
                cout_mp = c4.number_input("Coût intervention", min_value=0.0, key="mp_cout")
                c5, c6 = st.columns(2)
                prest_mp = c5.text_input("Prestataire", key="mp_prest")
                pieces_mp = c6.text_input("Pièces changées", key="mp_pieces")
                c7, c8 = st.columns(2)
                hrs_mp = c7.number_input("Heures compteur", min_value=0.0, key="mp_hrs")
                proch_mp = c8.number_input("Prochain entretien (h)", min_value=0.0, key="mp_proch")
                desc_mp = st.text_area("Description de l'intervention *", height=100, key="mp_desc")
                obs_mp = st.text_input("Observation", key="mp_obs")
                if st.form_submit_button("💾 Enregistrer l'intervention"):
                    if not desc_mp.strip():
                        st.error("La description est obligatoire.")
                    else:
                        er = df_em[df_em["nom"]==eng_mp].iloc[0]
                        mid = int(er["id"])
                        exsql("INSERT INTO maintenance_materiels(materiel_id,date_maintenance,type_maintenance,description,cout,prestataire,pieces_changees,heures_compteur,prochain_entretien_h,observation)VALUES(?,?,?,?,?,?,?,?,?,?)",
                              [mid, str(dm_p), type_mp, desc_mp.strip(), cout_mp, prest_mp.strip(), pieces_mp.strip(), hrs_mp, proch_mp, obs_mp.strip()])
                        # Mettre à jour l'état si maintenance corrective
                        if type_mp in ("Corrective","Réparation"):
                            exsql("UPDATE materiels SET etat='Opérationnel',date_derniere_maintenance=?,heures_totales=? WHERE id=?",
                                  [str(dm_p), hrs_mp, mid])
                        else:
                            exsql("UPDATE materiels SET date_derniere_maintenance=? WHERE id=?", [str(dm_p), mid])
                        audit("maintenance_materiels", "CREATE", mid, f"{type_mp} sur {eng_mp}")
                        st.success("✅ Intervention enregistrée."); st.rerun()

    with t2:
        st.subheader("📋 Historique complet des maintenances")
        # Filtres
        fc1, fc2, fc3 = st.columns(3)
        df_em2 = get_engins_maint()
        engin_filt = fc1.selectbox("Filtrer par engin", ["(Tous)"] + (df_em2["nom"].tolist() if not df_em2.empty else []), key="mp_filt_eng")
        type_filt = fc2.selectbox("Type", ["(Tous)","Préventive","Corrective","Vidange","Réparation","Inspection","Autre"], key="mp_filt_type")
        periode_filt = fc3.selectbox("Période", ["Tout","Ce mois","3 mois","6 mois","Cette année"], key="mp_filt_per")

        # Construire la requête
        conds = ["1=1"]
        params_h = []
        if engin_filt != "(Tous)" and not df_em2.empty:
            eid = int(df_em2[df_em2["nom"]==engin_filt].iloc[0]["id"])
            conds.append("mm.materiel_id=?"); params_h.append(eid)
        if type_filt != "(Tous)":
            conds.append("mm.type_maintenance=?"); params_h.append(type_filt)
        if periode_filt != "Tout":
            import datetime as _dt
            today_dt = date.today()
            if periode_filt == "Ce mois": d_lim = today_dt.replace(day=1)
            elif periode_filt == "3 mois": d_lim = today_dt - timedelta(days=90)
            elif periode_filt == "6 mois": d_lim = today_dt - timedelta(days=180)
            else: d_lim = today_dt.replace(month=1, day=1)
            conds.append("mm.date_maintenance>=?"); params_h.append(str(d_lim))

        df_hist = qdf(f"""
            SELECT mm.date_maintenance AS Date, m.nom AS Engin,
                   mm.type_maintenance AS Type, mm.description AS Description,
                   mm.cout AS Coût, mm.prestataire AS Prestataire,
                   mm.pieces_changees AS Pièces, mm.heures_compteur AS "Hrs compteur"
            FROM maintenance_materiels mm
            JOIN materiels m ON m.id=mm.materiel_id
            WHERE {" AND ".join(conds)}
            ORDER BY mm.date_maintenance DESC LIMIT 500
        """, params_h)
        if df_hist.empty:
            st.info("Aucune maintenance trouvée avec ces filtres.")
        else:
            st.metric("Interventions", len(df_hist))
            if "Coût" in df_hist.columns:
                st.metric("Coût total", fmt(df_hist["Coût"].sum()))
            st.dataframe(df_hist, use_container_width=True)
            st.download_button("📥 Export", to_xl({"Maintenance": df_hist}),
                               "historique_maintenance.xlsx",
                               "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    with t3:
        st.subheader("📊 Récapitulatif par engin")
        df_em3 = get_engins_maint()
        if df_em3.empty:
            st.info("Aucun engin.")
        else:
            rows_eng = []
            for _, e in df_em3.iterrows():
                eid = int(e["id"])
                r_m = qdf("SELECT COUNT(*) AS nb, COALESCE(SUM(cout),0) AS total_cout FROM maintenance_materiels WHERE materiel_id=?", [eid])
                nb = int(_v(r_m, "nb")); ct = float(_v(r_m, "total_cout"))
                last = qdf("SELECT MAX(date_maintenance) AS last FROM maintenance_materiels WHERE materiel_id=?", [eid])
                rows_eng.append({
                    "Engin": e["nom"],
                    "Type": str(e.get("type_materiel") or ""),
                    "État": str(e.get("etat") or "—"),
                    "Nb interventions": nb,
                    "Coût total maintenance": ct,
                    "Dernière intervention": str(_v(last, "last", "—"))
                })
            df_recap_eng = pd.DataFrame(rows_eng)
            st.dataframe(df_recap_eng, use_container_width=True)
            st.metric("Coût total parc", fmt(df_recap_eng["Coût total maintenance"].sum()))


# ── FIN DE L'APPLICATION ─────────────────────────────────────────
else:
    st.warning(f"Page inconnue : {page}")
