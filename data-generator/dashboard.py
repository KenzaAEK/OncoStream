import streamlit as st
import happybase
import pandas as pd
from datetime import datetime

# Configuration de la page
st.set_page_config(page_title="OncoStream Live", page_icon="🧬", layout="wide")
st.title("🧬 OncoStream - Real-Time Cancer Detection")

# 1. CONNEXION LÉGÈRE À HBASE (Via le port 9090 Thrift)
@st.cache_resource
def get_connection():
    # On se connecte à "localhost" car le port 9090 de Docker est mappé sur ton Windows
    return happybase.Connection('localhost', port=9090)

try:
    connection = get_connection()
    table = connection.table('oncostream_realtime') # La table créée tout à l'heure
except Exception as e:
    st.error(f"⚠️ Impossible de se connecter à HBase. Vérifie que Docker tourne ! Erreur: {e}")
    st.stop()

# 2. RÉCUPÉRATION DES DONNÉES (SCAN)
# HappyBase scanne la table et nous donne un générateur
st.write("Fetching live data from HBase...")
data = []

# On scanne tout (dans un vrai projet prod, on limiterait le scan)
for key, value in table.scan():
    # HBase renvoie des bytes (b'valeur'), il faut décoder en string
    row = {
        'read_id': key.decode('utf-8'),
        'mutation': value.get(b'cf1:mutation', b'Unknown').decode('utf-8'),
        'quality': float(value.get(b'cf1:quality', b'0').decode('utf-8')),
        'date': value.get(b'cf1:date', b'').decode('utf-8')
    }
    data.append(row)

# Transformation en Pandas pour les graphiques
df = pd.DataFrame(data)

# 3. VISUALISATION
if not df.empty:
    # Métriques Clés
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Analyses", len(df))
    col2.metric("Critical Mutations", df[df['mutation'].str.contains("FUSION|BRCA")].shape[0])
    col3.metric("Avg Quality Score", f"{df['quality'].mean():.2f}")

    # Graphique 1 : Top Mutations
    st.subheader("📊 Detected Biomarkers Distribution")
    mutation_counts = df['mutation'].value_counts()
    st.bar_chart(mutation_counts)

    # Tableau des dernières alertes (Exclure les NONE)
    st.subheader("🚨 Latest Critical Alerts")
    critical_df = df[df['mutation'] != 'NONE'].sort_values(by='date', ascending=False).head(10)
    st.dataframe(critical_df[['date', 'mutation', 'quality', 'read_id']], use_container_width=True)

else:
    st.warning("Waiting for data stream... (Launch Python Producer!)")

# Bouton de rafraichissement manuel
if st.button('Refresh Data 🔄'):
    st.rerun()