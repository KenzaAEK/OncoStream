name := "OncoStream"

version := "1.0"

scalaVersion := "2.12.18"

val sparkVersion = "3.5.3"

// 1. ISOLATION (Règle le crash ClassCastException)
fork := true

// 2. PERMISSIONS JAVA 11+ (Indispensable pour éviter les erreurs d'accès mémoire)
javaOptions ++= Seq(
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED"
)

libraryDependencies ++= Seq(
  // Le Cerveau Spark
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  
  // L'Oreille Kafka
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  
  // Logging
  "ch.qos.logback" % "logback-classic" % "1.2.11"
)