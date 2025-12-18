import streamlit as st
import happybase
import pandas as pd
import time
import os  

st.set_page_config(page_title="OncoStream Live", page_icon="🧬", layout="wide")
st.title("🧬 OncoStream - Real-Time Cancer Detection")

# --- FONCTION DE CONNEXION ROBUSTE ---
def smart_connect(retries=3, delay=1):
    """Tente de se connecter à HBase (Compatible Docker & Local)"""
    
    # On récupère l'adresse définie dans docker-compose.yml, sinon 'localhost' par défaut
    host = os.getenv('HBASE_HOST', 'localhost')
    port = int(os.getenv('HBASE_PORT', 9090))
    
    for i in range(retries):
        try:
            # On affiche (dans les logs Docker) où on essaie de se connecter
            print(f"🔌 Tentative de connexion HBase vers {host}:{port} (Essai {i+1}/{retries})...")
            
            connection = happybase.Connection(host, port=port, autoconnect=False)
            connection.open()
            print("✅ Connexion HBase réussie !")
            return connection
        except Exception as e:
            print(f"❌ Erreur : {e}")
            if i < retries - 1:
                time.sleep(delay)
                continue
            else:
                # On affiche l'erreur dans l'interface Web pour t'aider à débugger
                st.error(f"⚠️ Impossible de joindre HBase à l'adresse `{host}:{port}`. Erreur: {e}")
                return None

# 1. TENTATIVE DE CONNEXION
connection = smart_connect()
table = None

if connection:
    try:
        table = connection.table('oncostream_realtime')
        
        # 2. RÉCUPÉRATION DES DONNÉES
        # st.write("Fetching live data...") # Commenté pour cleaner l'interface
        data = []

        # Scan
        for key, value in table.scan():
            row = {
                'read_id': key.decode('utf-8'),
                'mutation': value.get(b'cf1:mutation', b'Unknown').decode('utf-8'),
                'quality': float(value.get(b'cf1:quality', b'0').decode('utf-8')),
                'date': value.get(b'cf1:date', b'').decode('utf-8')
            }
            data.append(row)

        df = pd.DataFrame(data)

        # 3. DASHBOARD
        if not df.empty:

            # Séparation des données "Pathogènes" (Malades) du "Bruit" (Sains/NONE)
            pathogenic_df = df[~df['mutation'].isin(['NONE', 'Unknown'])]

            # Métriques (Top de page)
            col1, col2, col3 = st.columns(3)
            col1.metric("🧬 Total Reads Processed", len(df))
            
            critical_count = df[~df['mutation'].isin(['NONE', 'Unknown'])].shape[0]
            col2.metric("☢️ Pathogenic Mutations", critical_count, delta_color="inverse")
            
            avg_qual = df['quality'].mean()
            col3.metric("✅ Global Quality Score", f"{avg_qual:.2f}", delta=f"{avg_qual-30:.1f}")

            # Layout : Graphique à gauche, Tableau à droite
            c1, c2 = st.columns([2, 1])
            
            with c1:
                st.subheader("📊 Mutation Type Distribution")
                if not pathogenic_df.empty:
                    st.bar_chart(pathogenic_df['mutation'].value_counts(), color="#FF4B4B")
                else:
                    st.info("No mutations detected yet (Patients are healthy).")

            with c2:
                st.subheader("🚨 Priority Alerts")
                # On affiche les derniers cas critiques détectés
                if not pathogenic_df.empty:
                    latest_alerts = pathogenic_df.sort_values(by='date', ascending=False).head(10)
                    st.dataframe(
                        latest_alerts[['mutation', 'quality']], 
                        hide_index=True,
                        use_container_width=True
                    )
                else:
                    st.success("✅ No critical alerts.")
        else:
            st.info("Waiting for data stream... Start the Python Producer!")

    except Exception as e:
        st.error(f"Erreur lecture: {e}")
    
    finally:
        # FERMETURE PROPRE OBLIGATOIRE
        try:
            connection.close()
        except:
            pass

# Bouton Refresh manuel (utile pour la démo)
if st.button('Actualiser les données 🔄', type="primary"):
    st.rerun()