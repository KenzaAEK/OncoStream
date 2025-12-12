import time
import random
from kafka import KafkaProducer

# Configuration
KAFKA_TOPIC = 'ngs-raw-reads'
SPEED_SECONDS = 0.1  # Vitesse de génération

# ==============================================================================
# BIOMARQUEURS CIBLES (Source: Illumina TruSight RNA Pan-Cancer & Eurofins)
# ==============================================================================
CANCER_SIGNATURES = {
    # --- Variants Héréditaires (Sein / Ovaire) ---
    'BRCA1_DEL_AG': 'TGTCGATGG',  # Délétion fréquente BRCA1
    'BRCA2_INS_T':  'CCTCTAACC',  # Insertion BRCA2
    
    # --- Variants Somatiques (Poumon / Mélanome) ---
    'EGFR_L858R':   'GTGGAGCAC',  # Mutation classique Poumon
    'BRAF_V600E':   'TAGCTACAG',  # Mutation classique Mélanome/Colon
    
    # --- Fusions de Gènes (Structural Variants) ---
    'FUSION_EML4_ALK':     'GCTTTTTG',  # Fusion critique Poumon (Non-Fumeur)
    'FUSION_TMPRSS2_ETV1': 'CGGAGGAG'   # Fusion Prostate
}

def generate_illumina_header(counter):
    """Génère un header réaliste type Illumina HiSeq"""
    # Format: @Instrument:RunID:FlowCellID:Lane:Tile:X:Y
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
    """Simule une dégradation de la qualité vers la fin (Phénomène réel Illumina)"""
    qualities = []
    for i in range(length):
        # Le début est excellent (Q40), la fin dégrade un peu
        base_score = 40 - (i / length * 10) 
        score = int(base_score + random.randint(-2, 2))
        qualities.append(chr(score + 33)) # Conversion ASCII Phred+33
    return ''.join(qualities)

def create_fastq_record(counter):
    length = 100
    seq = generate_dna_sequence(length)
    mutation_type = "NONE"

    # Injection de mutation (10% de chance pour la démo)
    if random.random() < 0.10:
        mut_name, mut_seq = random.choice(list(CANCER_SIGNATURES.items()))
        # Insertion aléatoire de la signature dans la séquence
        pos = random.randint(10, length - len(mut_seq) - 10)
        seq = seq[:pos] + mut_seq + seq[pos+len(mut_seq):]
        mutation_type = mut_name

    qual = generate_quality_scores(len(seq))
    header = generate_illumina_header(counter)

    # Format FASTQ Standard (4 lignes)
    fastq_entry = f"{header}\n{seq}\n+\n{qual}"
    return fastq_entry, mutation_type

def main():
    print("🧬 Lancement du Séquenceur Illumina (Panel TruSight Simulé)...")
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    
    counter = 0
    try:
        while True:
            counter += 1
            fastq, mut = create_fastq_record(counter)
            
            producer.send(KAFKA_TOPIC, fastq.encode('utf-8'))
            
            if mut != "NONE":
                print(f"[{counter}] ⚠️  ALERTE: {mut} détectée !")
            elif counter % 100 == 0:
                print(f"[{counter}] ... Séquençage en cours ...")
            
            time.sleep(SPEED_SECONDS)
            
    except KeyboardInterrupt:
        producer.close()
        print("Arrêt.")

if __name__ == "__main__":
    main()