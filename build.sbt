inThisBuild(
  List(
    organization := "io.github.pityka",
    homepage := Some(url("https://pityka.github.io/flatjoin/")),
    licenses := List(("MIT", url("https://opensource.org/licenses/MIT"))),
    developers := List(
      Developer(
        "pityka",
        "Istvan Bartha",
        "bartha.pityu@gmail.com",
        url("https://github.com/pityka/flatjoin")
      )
    )
  )
)

lazy val commonSettings = Seq(
  organization := "io.github.pityka",
  scalaVersion := "2.13.6",
  crossScalaVersions := Seq("2.12.13", "2.13.6")
)

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin",
    publishArtifact := false
  )
  .aggregate(core, upickle, akkastream, circe, jsoniter)

lazy val core = (project in file("core"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-core",
    libraryDependencies ++= Seq(
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.3",
      "org.scalatest" %% "scalatest" % "3.2.9" % "test"
    )
  )

lazy val akkastream = (project in file("akka-stream"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-akka-stream",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream" % "2.6.14",
      "org.scalatest" %% "scalatest" % "3.2.9" % "test"
    )
  )
  .dependsOn(core)

lazy val iterator = (project in file("iterator"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-iterator",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.9" % "test"
    )
  )
  .dependsOn(core)

lazy val circe = (project in file("circe"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-circe",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.9" % "test",
      "io.circe" %% "circe-core" % "0.14.0",
      "io.circe" %% "circe-parser" % "0.14.0"
    )
  )
  .dependsOn(core, iterator % "test")

lazy val upickle = (project in file("upickle"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-upickle",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.9" % "test",
      "com.lihaoyi" %% "upickle" % "1.3.15"
    )
  )
  .dependsOn(core, iterator % "test")

lazy val jsoniter = (project in file("jsoniter"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-jsoniter",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.9" % "test",
      "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % "2.8.0" % Provided
    )
  )
  .dependsOn(core, iterator % "test")
