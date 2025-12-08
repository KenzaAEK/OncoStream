from kafka import KafkaConsumer
print("🎧 En attente de messages...")
consumer = KafkaConsumer('test-topic', bootstrap_servers='localhost:9092')
for msg in consumer:
    print(f"✅ REÇU : {msg.value.decode('utf-8')}")