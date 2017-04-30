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
  .aggregate(core, boopickle, upickle)

lazy val core = (project in file("core"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-core",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "2.1.5" % "test")
  )

lazy val boopickle = (project in file("boopickle"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-boopickle",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "2.1.5" % "test",
      "io.suzaku" %% "boopickle" % "1.2.6")
  )
  .dependsOn(core)

lazy val upickle = (project in file("upickle"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-upickle",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "2.1.5" % "test",
      "com.lihaoyi" %% "upickle" % "0.4.3")
  )
  .dependsOn(core)
