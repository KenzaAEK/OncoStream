import time
import argparse
import random
import sys
from kafka import KafkaProducer

# Configuration par défaut (si aucun argument n'est donné)
DEFAULT_TOPIC = 'ngs-raw-reads'

# ==============================================================================
# BIOMARQUEURS CIBLES (Source: Illumina TruSight RNA Pan-Cancer & Eurofins)
# ==============================================================================
CANCER_SIGNATURES = {
    'BRCA1_DEL_AG': 'TGTCGATGG',  # Délétion fréquente BRCA1
    'BRCA2_INS_T':  'CCTCTAACC',  # Insertion BRCA2
    'EGFR_L858R':   'GTGGAGCAC',  # Mutation classique Poumon
    'BRAF_V600E':   'TAGCTACAG',  # Mutation classique Mélanome/Colon
    'FUSION_EML4_ALK':     'GCTTTTTG',  # Fusion critique Poumon
    'FUSION_TMPRSS2_ETV1': 'CGGAGGAG'   # Fusion Prostate
}

def generate_illumina_header(counter):
    """Génère un header réaliste type Illumina HiSeq"""
    instrument = "HWI-ST1276"
    flowcell = "C1W31ACXX"
    lane = random.randint(1, 8)
    tile = random.randint(1101, 2228)
    x_pos = random.randint(1000, 20000)
    y_pos = random.randint(1000, 20000)
    return f"@{instrument}:123:{flowcell}:{lane}:{tile}:{x_pos}:{y_pos} 1:N:0:ATCACG"

def generate_dna_sequence(length=100):
    return ''.join(random.choices(['A', 'C', 'T', 'G'], k=length))

def generate_quality_scores(length=100):
    qualities = []
    for i in range(length):
        base_score = 40 - (i / length * 10) 
        score = int(base_score + random.randint(-2, 2))
        qualities.append(chr(score + 33)) 
    return ''.join(qualities)

def create_fastq_record(counter):
    length = 100
    seq = generate_dna_sequence(length)
    mutation_type = "NONE"

    # Injection de mutation (10% de chance)
    if random.random() < 0.10:
        mut_name, mut_seq = random.choice(list(CANCER_SIGNATURES.items()))
        pos = random.randint(10, length - len(mut_seq) - 10)
        seq = seq[:pos] + mut_seq + seq[pos+len(mut_seq):]
        mutation_type = mut_name

    qual = generate_quality_scores(len(seq))
    header = generate_illumina_header(counter)
    fastq_entry = f"{header}\n{seq}\n+\n{qual}"
    return fastq_entry, mutation_type

def main():
    # --- GESTION DES ARGUMENTS LIGNE DE COMMANDE ---
    parser = argparse.ArgumentParser(description="Générateur de données NGS pour Stress Test")
    parser.add_argument("--rate", type=float, default=10.0, help="Nombre de messages par seconde (défaut: 10)")
    parser.add_argument("--duration", type=int, default=0, help="Durée du test en secondes (0 = infini)")
    args = parser.parse_args()

    # Calcul du délai entre les messages
    sleep_time = 1.0 / args.rate if args.rate > 0 else 0
    
    # Calcul du temps de fin
    start_time = time.time()
    end_time = start_time + args.duration if args.duration > 0 else float('inf')

    print(f"🧬 Lancement du Séquenceur Illumina (Simulé)")
    print(f"   ⚙️  Vitesse : {args.rate} msg/sec")
    print(f"   ⏱️  Durée   : {'Infini' if args.duration == 0 else f'{args.duration} secondes'}")

    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            api_version=(0, 10, 1)  # <--- On force une version compatible (hack classique)
        )
    except Exception as e:
        print(f"❌ Erreur connexion Kafka: {e}")
        return

    counter = 0
    try:
        while time.time() < end_time:
            counter += 1
            fastq, mut = create_fastq_record(counter)
            
            # Envoi Kafka
            producer.send(DEFAULT_TOPIC, fastq.encode('utf-8'))
            
            # Logs (on réduit le bruit si on va très vite)
            if args.rate < 50:
                if mut != "NONE":
                    print(f"[{counter}] ⚠️  ALERTE: {mut}")
                elif counter % 10 == 0:
                    print(f"[{counter}] ...")
            else:
                # Mode Stress Test (moins de logs pour ne pas ralentir)
                if counter % 1000 == 0:
                    print(f"🚀 [{counter}] messages envoyés...")
            
            # Contrôle de la vitesse
            time.sleep(sleep_time)

        print(f"✅ Fin du test. Total généré : {counter} reads.")

    except KeyboardInterrupt:
        print("\nArrêt manuel.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()