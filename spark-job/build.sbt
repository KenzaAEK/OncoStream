name := "OncoStream"

version := "1.0"

scalaVersion := "2.12.18"

val sparkVersion = "3.5.3"

// 1. ISOLATION
fork := true

// 2. PERMISSIONS JAVA 11+
javaOptions ++= Seq(
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED"
)

libraryDependencies ++= Seq(
  // --- SPARK & KAFKA ---
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "ch.qos.logback" % "logback-classic" % "1.2.11",

  // --- BONUS HBASE  ---
  // On installe le client HBase 2.1.0, mais on EXCLUT toutes ses vieilles dépendances Hadoop
  // pour qu'il utilise celles de Spark 3.5 (Hadoop 3.x) à la place.
  "org.apache.hbase" % "hbase-client" % "2.1.0" 
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("org.apache.hadoop", "hadoop-common")
    exclude("org.apache.hadoop", "hadoop-auth")
    exclude("org.apache.hadoop", "hadoop-mapreduce-client-core"),
    
  "org.apache.hbase" % "hbase-common" % "2.1.0" 
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("org.apache.hadoop", "hadoop-common")
)