import time
import random
from kafka import KafkaProducer

# Configuration
KAFKA_TOPIC = 'ngs-raw-reads'
SPEED_SECONDS = 0.1  # Très rapide

# Séquences cibles (Mutations réelles du cancer du sein et poumon)
CANCER_SIGNATURES = {
    'BRCA1_DEL_AG': 'TGTCTTTT',  # Délétion de 2 bases (Cancer du sein)
    'EGFR_L858R':   'CAAGATCACAGATTTTGGGCTGGCCAAAC' # Mutation (Cancer poumon)
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
    """Simule une dégradation de la qualité vers la fin (Réaliste)"""
    qualities = []
    for i in range(length):
        # Le début est excellent (Q35-Q40), la fin est moins bonne (Q20-Q30)
        base_score = 40 - (i / length * 15) 
        # Ajout d'un peu de bruit aléatoire
        score = int(base_score + random.randint(-2, 2))
        # Conversion en ASCII (Phred+33)
        qualities.append(chr(score + 33))
    return ''.join(qualities)

def create_fastq_record(counter):
    length = 100
    seq = generate_dna_sequence(length)
    mutation_type = "NONE"

    # Injection de mutation (5% de chance)
    if random.random() < 0.05:
        mut_name, mut_seq = random.choice(list(CANCER_SIGNATURES.items()))
        # Insertion aléatoire
        pos = random.randint(10, length - len(mut_seq) - 10)
        seq = seq[:pos] + mut_seq + seq[pos+len(mut_seq):]
        mutation_type = mut_name

    qual = generate_quality_scores(len(seq))
    header = generate_illumina_header(counter)

    # Construction FASTQ
    fastq_entry = f"{header}\n{seq}\n+\n{qual}"
    return fastq_entry, mutation_type

def main():
    print("🧬 Lancement du Séquenceur Illumina (Simulé)...")
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