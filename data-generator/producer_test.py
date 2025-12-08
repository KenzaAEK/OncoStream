from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send('test-topic', b'Hello OncoStream!')
print("📢 Message envoyé !")
producer.flush()