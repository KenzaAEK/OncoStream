name := "OncoStream"

version := "1.0"

scalaVersion := "2.12.18"

val sparkVersion = "3.5.3"

libraryDependencies ++= Seq(
  // 1. Le Cerveau Spark (Core + SQL)
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  
  // 2. L'Oreille (Pour écouter Kafka)
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  
  // 3. Les Outils (Logging pour éviter le spam dans la console)
  "ch.qos.logback" % "logback-classic" % "1.2.11"
)