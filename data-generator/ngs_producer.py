import time
import argparse
import random
import sys
import os  # <--- Indispensable pour lire les variables Docker
from kafka import KafkaProducer

# Configuration par défaut
# On cherche d'abord la variable Docker, sinon on met 'localhost' pour tes tests hors Docker
DEFAULT_KAFKA_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
DEFAULT_TOPIC = os.getenv('KAFKA_TOPIC', 'ngs-raw-reads')

# ==============================================================================
# BIOMARQUEURS CIBLES
# ==============================================================================
CANCER_SIGNATURES = {
    'BRCA1_DEL_AG': 'TGTCGATGG',
    'BRCA2_INS_T':  'CCTCTAACC',
    'EGFR_L858R':   'GTGGAGCAC',
    'BRAF_V600E':   'TAGCTACAG',
    'FUSION_EML4_ALK':     'GCTTTTTG',
    'FUSION_TMPRSS2_ETV1': 'CGGAGGAG'
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
    # --- GESTION DES ARGUMENTS ---
    parser = argparse.ArgumentParser(description="Générateur de données NGS pour Stress Test")
    parser.add_argument("--rate", type=float, default=10.0, help="Nombre de messages par seconde")
    parser.add_argument("--duration", type=int, default=0, help="Durée du test en secondes (0 = infini)")
    # On permet de surcharger l'adresse Kafka via argument si besoin
    parser.add_argument("--bootstrap", type=str, default=DEFAULT_KAFKA_SERVER, help="Adresse serveur Kafka")
    args = parser.parse_args()

    sleep_time = 1.0 / args.rate if args.rate > 0 else 0
    start_time = time.time()
    end_time = start_time + args.duration if args.duration > 0 else float('inf')

    print(f"🧬 Lancement du Séquenceur Illumina (Simulé)")
    print(f"   🎯 Serveur Kafka : {args.bootstrap}")
    print(f"   ⚙️  Vitesse : {args.rate} msg/sec")
    print(f"   ⏱️  Durée   : {'Infini' if args.duration == 0 else f'{args.duration} secondes'}")

    # --- CONNEXION KAFKA ROBUSTE ---
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap,
            # api_version=(0, 10, 1) # Supprimé car souvent auto-détecté, remets-le si erreur "NoBrokersAvailable"
        )
    except Exception as e:
        print(f"❌ CRASH INITIAL : Impossible de joindre Kafka à {args.bootstrap}")
        print(f"❌ Erreur : {e}")
        return

    counter = 0
    try:
        while time.time() < end_time:
            counter += 1
            fastq, mut = create_fastq_record(counter)
            
            # Envoi vers le topic défini
            producer.send(DEFAULT_TOPIC, fastq.encode('utf-8'))
            
            if args.rate < 50:
                if mut != "NONE":
                    print(f"[{counter}] ⚠️  ALERTE: {mut}")
                elif counter % 10 == 0:
                    print(f"[{counter}] ...")
            else:
                if counter % 1000 == 0:
                    print(f"🚀 [{counter}] messages envoyés...")
            
            time.sleep(sleep_time)

        print(f"✅ Fin du test. Total généré : {counter} reads.")

    except KeyboardInterrupt:
        print("\nArrêt manuel.")
    finally:
        if producer:
            producer.close()

if __name__ == "__main__":
    main()