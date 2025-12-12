import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object OncoStreamApp {

  def main(args: Array[String]): Unit = {

    // 1. Initialiser Spark en mode LOCAL (Stabilité Maximale pour Dev)
    val spark = SparkSession.builder()
      .appName("OncoStream-Analytics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    println("🧬 Démarrage de l'Analyse OncoStream (Parsing -> QC -> Detection)...")

    // 2. L'Oreille : Lecture Kafka
    val rawStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:29092")
      .option("subscribe", "ngs-raw-reads")
      .option("startingOffsets", "latest")
      .load()

    val inputStream = rawStream.selectExpr("CAST(value AS STRING) as raw_fastq", "timestamp")

    
    // 3. LOGIQUE MÉTIER (Business Logic)
    // A. Parsing FASTQ (Extraction ID, Séquence, Qualité)
    val parsedData = inputStream
      .withColumn("lines_array", split(col("raw_fastq"), "\n"))
      .select(
        col("timestamp"),
        col("lines_array").getItem(0).as("read_id"),
        col("lines_array").getItem(1).as("dna_sequence"),
        col("lines_array").getItem(3).as("quality_string")
      )

    // B. UDF : Calcul Score Qualité (Phred Score)
    val calculateQuality = udf((qualityStr: String) => {
      if (qualityStr != null && qualityStr.length > 0) {
        val scores = qualityStr.map(char => char.toInt - 33)
        scores.sum.toDouble / scores.length
      } else {
        0.0
      }
    })

    // C. UDF : Détection Biomarqueurs (Panel TruSight)
    val detectMutation = udf((sequence: String) => {
      // 1. Cancers Héréditaires (Sein / Ovaire)
      if (sequence.contains("TGTCGATGG")) "BRCA1_DEL_AG"
      else if (sequence.contains("CCTCTAACC")) "BRCA2_INS_T"
      
      // 2. Cancers Somatiques (Poumon / Mélanome)
      else if (sequence.contains("GTGGAGCAC")) "EGFR_L858R"
      else if (sequence.contains("TAGCTACAG")) "BRAF_V600E"
      
      // 3. Fusions de Gènes (Complexes)
      else if (sequence.contains("GCTTTTTG")) "FUSION_EML4_ALK"     // Poumon
      else if (sequence.contains("CGGAGGAG")) "FUSION_TMPRSS2_ETV1" // Prostate
      
      else "NONE"
    })

    // D. Application du Pipeline
    val analyzedData = parsedData
      .withColumn("quality_score", calculateQuality(col("quality_string")))
      .filter(col("quality_score") >= 30) // FILTRE QC (Standard Industriel)
      .withColumn("mutation_detected", detectMutation(col("dna_sequence")))
      .withColumn("processing_date", to_date(col("timestamp")))

   
    // 4. SORTIES (Sinks)
    // SORTIE 1 : CONSOLE (Temps Réel - Uniquement les alertes)
    val consoleQuery = analyzedData
      .filter(col("mutation_detected") =!= "NONE")
      .select("read_id", "mutation_detected", "quality_score")
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .start()

    // SORTIE 2 : HDFS (Data Lake - Partitionné pour l'historique)
    // On stocke les données traitées (Processed) au format Parquet
    val hdfsQuery = analyzedData
      .writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", "hdfs://namenode:9000/oncostream/processed_data")
      .option("checkpointLocation", "hdfs://namenode:9000/oncostream/checkpoints/processed_v2")
      .partitionBy("processing_date", "mutation_detected") // Partitionnement Intelligent
      .start()

    spark.streams.awaitAnyTermination()
  }
}