ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "Flight",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.apache.spark" %% "spark-sql" % "3.5.1",
      "org.apache.spark" %% "spark-mllib" % "3.5.1",
      "com.typesafe" % "config" % "1.4.1"
    ),
    fork := true,
    Compile / run / javaOptions += "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
  )