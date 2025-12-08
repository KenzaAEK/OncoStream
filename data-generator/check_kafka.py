from kafka import KafkaConsumer

TOPIC = 'ngs-raw-reads'
print(f"🕵️‍♂️  Mise sur écoute du topic Kafka : {TOPIC}...")

# On se connecte en tant que consommateur
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',  # On écoute seulement les nouveaux messages
    group_id='validation-squad'
)

print("✅ Connexion réussie. En attente de données...")

for message in consumer:
    # On décode le message (qui est en bytes) pour le lire
    texte = message.value.decode('utf-8')
    # On affiche juste la première ligne (l'ID) pour vérifier
    first_line = texte.split('\n')[0]
    print(f"📥 REÇU VIA KAFKA : {first_line}")