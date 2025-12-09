import org.apache.spark.sql.SparkSession

object OncoStreamApp {
  def main(args: Array[String]): Unit = {
    
    // 1. Initialiser le Cerveau (Spark Session)
    val spark = SparkSession.builder()
      .appName("OncoStream-Ingestion")
      // "local[*]" signifie : Utilise tous les cœurs de MON processeur (mode test)
      .master("local[*]")
      .getOrCreate()

    // On réduit le bruit (logs) pour ne voir que les erreurs ou les données
    spark.sparkContext.setLogLevel("WARN")
    
    println("🧬 Démarrage du Job Spark OncoStream...")

    // 2. L'Oreille : Configuration de la lecture Kafka
    // Note : On utilise 'localhost:9092' car tu lances ce code depuis WSL (l'extérieur du conteneur)
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:29092") 
      .option("subscribe", "ngs-raw-reads") // Le topic créé par ton script Python
      .option("startingOffsets", "latest")    // On écoute seulement les nouveaux messages
      .load()

    // 3. La Traduction : Kafka envoie des octets (binaire), on veut du texte
    import spark.implicits._
    val dataStream = kafkaStream.selectExpr("CAST(value AS STRING) as fastq_data")

    // 4. L'Affichage : Écrire le résultat dans la console
    val query = dataStream.writeStream
      .outputMode("append") // On ajoute chaque nouvelle ligne reçue
      .format("console")    // Sortie = Écran noir
      .option("truncate", "false") // Affiche toute la ligne sans couper
      .start()

    // Garde le programme allumé indéfiniment
    query.awaitTermination()
  }
}