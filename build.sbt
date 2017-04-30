lazy val commonSettings = Seq(
    organization := "io.github.pityka",
    scalaVersion := "2.11.11",
    version := "0.0.1-SNAPSHOT"
  ) ++ reformatOnCompileSettings

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoins",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "2.1.5" % "test")
  )
