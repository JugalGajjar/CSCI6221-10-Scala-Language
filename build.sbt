ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / evictionErrorLevel := Level.Warn
lazy val root = (project in file("."))
  .settings(
    name := "SimpleSbtProject",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.3",
      "org.apache.spark" %% "spark-core" % "3.5.3",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.3",
      "org.scala-lang.modules" % "scala-parser-combinators_2.12" % "2.4.0",
      "com.typesafe" %% "ssl-config-core" % "0.6.1" exclude("org.scala-lang.modules", "scala-parser-combinators_2.12"),
      "com.google.api-client" % "google-api-client" % "2.7.0",
      "com.google.apis" % "google-api-services-youtube" % "v3-rev20241105-2.0.0",
      "com.softwaremill.sttp.client3" %% "core" % "3.9.4",           // STTP client v2.x
      "com.softwaremill.sttp.client3" %% "play-json" % "3.9.1",   // Play JSON for JSON parsing
      "com.softwaremill.sttp.client3" %% "akka-http-backend" % "3.10.1",
      "com.typesafe.akka" %% "akka-actor" % "2.5.32",       // Core Akka dependency
      "com.typesafe.akka" %% "akka-stream" % "2.5.32",    // Akka Streams dependency
      "org.apache.commons" % "commons-csv" % "1.12.0",
      "org.jfree" % "jfreechart" % "1.5.3",
      "mysql" % "mysql-connector-java" % "8.0.33"
    ),
    Compile / mainClass := Some("First.Main")
  )
