
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
  scalaVersion := "2.13.5",
  crossScalaVersions := Seq("2.12.13", "2.13.5")
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
      "org.scalatest" %% "scalatest" % "3.2.5" % "test"
    )
  )

lazy val akkastream = (project in file("akka-stream"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-akka-stream",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream" % "2.6.13",
      "org.scalatest" %% "scalatest" % "3.2.5" % "test"
    )
  )
  .dependsOn(core)

lazy val iterator = (project in file("iterator"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-iterator",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.5" % "test"
    )
  )
  .dependsOn(core)

lazy val circe = (project in file("circe"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-circe",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.5" % "test",
      "io.circe" %% "circe-core" % "0.13.0",
      "io.circe" %% "circe-parser" % "0.13.0"
    )
  )
  .dependsOn(core, iterator % "test")

lazy val upickle = (project in file("upickle"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-upickle",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.5" % "test",
      "com.lihaoyi" %% "upickle" % "1.3.11"
    )
  )
  .dependsOn(core, iterator % "test")

lazy val jsoniter = (project in file("jsoniter"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-jsoniter",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.5" % "test",
      "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % "2.6.4" % Provided
    )
  )
  .dependsOn(core, iterator % "test")
