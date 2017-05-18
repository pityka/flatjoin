lazy val commonSettings = Seq(
    organization := "io.github.pityka",
    scalaVersion := "2.11.11",
    version := "0.0.1-SNAPSHOT"
  ) ++ reformatOnCompileSettings

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin",
    publishArtifact := false
  )
  .aggregate(core, boopickle, upickle, akkastream, iterator, circe)

lazy val core = (project in file("core"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-core",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "2.1.5" % "test")
  )

lazy val akkastream = (project in file("akka-stream"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-akka-stream",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream" % "2.4.18",
      "org.scalatest" %% "scalatest" % "2.1.5" % "test")
  )
  .dependsOn(core)

lazy val fs2 = (project in file("fs2"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-fs2",
    libraryDependencies ++= Seq(
      "co.fs2" %% "fs2-io" % "0.9.5",
      "org.scodec" %% "scodec-stream" % "1.0.1",
      "org.scalatest" %% "scalatest" % "2.1.5" % "test")
  )
  .dependsOn(core)

lazy val boopickle = (project in file("boopickle"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-boopickle",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "2.1.5" % "test",
      "io.suzaku" %% "boopickle" % "1.2.6")
  )
  .dependsOn(core, iterator)

lazy val iterator = (project in file("iterator"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-iterator",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "2.1.5" % "test")
  )
  .dependsOn(core)

lazy val circe = (project in file("circe"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-circe",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "2.1.5" % "test",
      "io.circe" %% "circe-core" % "0.8.0",
      // "io.circe" %% "circe-generic"%"0.8.0",
      "io.circe" %% "circe-parser" % "0.8.0"
    )
  )
  .dependsOn(core, iterator)

lazy val upickle = (project in file("upickle"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-upickle",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "2.1.5" % "test",
      "com.lihaoyi" %% "upickle" % "0.4.3")
  )
  .dependsOn(core, iterator)
