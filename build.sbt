name := """scala-twitter-stream-demo"""

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.4.7",
  "com.typesafe.akka" %% "akka-http-core" % "2.4.7",
  "com.hunorkovacs" %% "koauth" % "1.1.0",
  "org.json4s" %% "json4s-native" % "3.3.0", 
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.11-M3",
  "com.sksamuel.avro4s" %% "avro4s-core" % "1.4.3")


fork in run := true
