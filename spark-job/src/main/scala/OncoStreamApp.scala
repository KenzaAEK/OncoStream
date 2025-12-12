import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

object OncoStreamApp {

  def main(args: Array[String]): Unit = {

    // 1. Initialisation
    val spark = SparkSession.builder()
      .appName("OncoStream-Analytics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    println("🧬 Démarrage OncoStream : Kafka -> Spark -> HDFS + HBase (Temps-Réel)...")

    // 2. Lecture Kafka
    val rawStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:29092")
      .option("subscribe", "ngs-raw-reads")
      .option("startingOffsets", "latest")
      .load()

    val inputStream = rawStream.selectExpr("CAST(value AS STRING) as raw_fastq", "timestamp")

    // 3. Logique Métier (Parsing & Détection)
    val calculateQuality = udf((qualityStr: String) => {
      if (qualityStr != null && qualityStr.length > 0) 
        qualityStr.map(c => c.toInt - 33).sum.toDouble / qualityStr.length 
      else 0.0
    })

    val detectMutation = udf((sequence: String) => {
      if (sequence.contains("TGTCGATGG")) "BRCA1_DEL_AG"
      else if (sequence.contains("CCTCTAACC")) "BRCA2_INS_T"
      else if (sequence.contains("GTGGAGCAC")) "EGFR_L858R"
      else if (sequence.contains("TAGCTACAG")) "BRAF_V600E"
      else if (sequence.contains("GCTTTTTG")) "FUSION_EML4_ALK"
      else if (sequence.contains("CGGAGGAG")) "FUSION_TMPRSS2_ETV1"
      else "NONE"
    })

    val parsedData = inputStream
      .withColumn("lines_array", split(col("raw_fastq"), "\n"))
      .select(
        col("timestamp"),
        col("lines_array").getItem(0).as("read_id"),
        col("lines_array").getItem(1).as("dna_sequence"),
        col("lines_array").getItem(3).as("quality_string")
      )
      .withColumn("quality_score", calculateQuality(col("quality_string")))
      .filter(col("quality_score") >= 30)
      .withColumn("mutation_detected", detectMutation(col("dna_sequence")))
      .withColumn("processing_date", to_date(col("timestamp")))

    // 4. SORTIES (Sinks)

    // A. SORTIE HDFS (Analytique / Historique)
    val hdfsQuery = parsedData
      .writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", "hdfs://namenode:9000/oncostream/processed_data_v3")
      .option("checkpointLocation", "hdfs://namenode:9000/oncostream/checkpoints/v3")
      .partitionBy("processing_date", "mutation_detected")
      .start()

    // B. BONUS : SORTIE HBASE (Temps-Réel / Médecin)
    // On n'envoie vers HBase QUE les mutations détectées (pour ne pas saturer)
    val hbaseQuery = parsedData
      .filter(col("mutation_detected") =!= "NONE")
      .writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], batchId: Long) =>
        
        // Cette fonction s'exécute sur le driver pour chaque micro-batch
        batchDF.foreachPartition { partitionIter: Iterator[org.apache.spark.sql.Row] =>
          // Cette partie s'exécute sur les workers (Executors)
          
          // 1. Configurer la connexion HBase (Localisée dans le conteneur)
          val conf = HBaseConfiguration.create()
          conf.set("hbase.zookeeper.quorum", "hbase")
          conf.set("hbase.zookeeper.property.clientPort", "2181")
          conf.set("zookeeper.znode.parent", "/hbase")
          
          // 2. Ouvrir la connexion (Une fois par partition pour la perf)
          val connection = ConnectionFactory.createConnection(conf)
          val table = connection.getTable(TableName.valueOf("oncostream_realtime"))
          
          try {
            partitionIter.foreach { row =>
              val readId = row.getAs[String]("read_id")
              val mutation = row.getAs[String]("mutation_detected")
              val quality = row.getAs[Double]("quality_score").toString
              val date = row.getAs[java.sql.Date]("processing_date").toString

              // 3. Créer l'objet PUT (RowKey = ReadID)
              // C'est ici la magie NoSQL : Accès direct par ID
              val put = new Put(Bytes.toBytes(readId))
              
              // Ajouter les colonnes dans la famille 'cf1'
              put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("mutation"), Bytes.toBytes(mutation))
              put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("quality"), Bytes.toBytes(quality))
              put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("date"), Bytes.toBytes(date))
              
              // 4. Envoyer à HBase
              table.put(put)
            }
          } finally {
            table.close()
            connection.close()
          }
        }
      }
      .start()

    spark.streams.awaitAnyTermination()
  }
}