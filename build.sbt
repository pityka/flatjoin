lazy val commonSettings = Seq(
  organization := "io.github.pityka",
  scalaVersion := "2.12.8",
  crossScalaVersions := Seq("2.12.6", "2.11.11"),
  version := "0.0.13-SNAPSHOT",
  licenses += ("MIT", url("https://opensource.org/licenses/MIT")),
  publishTo := sonatypePublishTo.value,
  pomExtra in Global := {
    <url>https://pityka.github.io/flatjoin/</url>
      <scm>
        <connection>scm:git:github.com/pityka/flatjoin</connection>
        <developerConnection>scm:git:git@github.com:pityka/flatjoin</developerConnection>
        <url>github.com/pityka/flatjoin</url>
      </scm>
      <developers>
        <developer>
          <id>pityka</id>
          <name>Istvan Bartha</name>
          <url>https://pityka.github.io/flatjoin/</url>
        </developer>
      </developers>
  }
)

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin",
    publishArtifact := false
  )
  .aggregate(core, boopickle, upickle, akkastream, iterator, circe, jsoniter)

lazy val core = (project in file("core"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-core",
    libraryDependencies ++= Seq(
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
      "org.scalatest" %% "scalatest" % "3.0.0" % "test")
  )

lazy val akkastream = (project in file("akka-stream"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-akka-stream",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream" % "2.5.18",
      "org.scalatest" %% "scalatest" % "3.0.0" % "test")
  )
  .dependsOn(core)

lazy val boopickle = (project in file("boopickle"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-boopickle",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.0" % "test",
      "io.suzaku" %% "boopickle" % "1.2.6")
  )
  .dependsOn(core, iterator)

lazy val iterator = (project in file("iterator"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-iterator",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.0" % "test")
  )
  .dependsOn(core)

lazy val circe = (project in file("circe"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-circe",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.0" % "test",
      "io.circe" %% "circe-core" % "0.8.0",
      "io.circe" %% "circe-parser" % "0.8.0"
    )
  )
  .dependsOn(core, iterator)

lazy val upickle = (project in file("upickle"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-upickle",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.0" % "test",
      "com.lihaoyi" %% "upickle" % "0.4.4")
  )
  .dependsOn(core, iterator)

lazy val jsoniter = (project in file("jsoniter"))
  .settings(commonSettings: _*)
  .settings(
    name := "flatjoin-jsoniter",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.0" % "test",
      "com.github.plokhotnyuk.jsoniter-scala" %% "macros" % "0.23.0")
  )
  .dependsOn(core, iterator)

scalafmtOnCompile in ThisBuild := true
