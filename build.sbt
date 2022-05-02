name := "sensor-app"

version := "0.1"

scalaVersion := "2.13.5"

val AkkaVersion = "2.6.14"
val AlpakkaCsvVersion = "3.0.4"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-csv" % AlpakkaCsvVersion
)
