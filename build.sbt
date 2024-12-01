ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "Flight",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.apache.spark" %% "spark-sql" % "3.5.1",
      "org.apache.spark" %% "spark-mllib" % "3.5.1",
      "com.typesafe" % "config" % "1.4.3"
    ),
    fork := true,
    Compile / run / javaOptions += "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
      case PathList("META-INF", "org", "apache", "logging", "log4j", "core", "config", "plugins", "Log4j2Plugins.dat") => MergeStrategy.first
      case "module-info.class" => MergeStrategy.discard
      case "META-INF/native-image/org/apache/arrow/arrow-memory-core/arrow-git.properties" => MergeStrategy.first
      case "META-INF/native-image/org/apache/arrow/arrow-format/arrow-git.properties" => MergeStrategy.first
      case "META-INF/native-image/org/apache/arrow/arrow-memory-netty/arrow-git.properties" => MergeStrategy.first
      case "META-INF/native-image/org/apache/arrow/arrow-vector/arrow-git.properties" => MergeStrategy.first
      case x if x.endsWith("module-info.class") => MergeStrategy.discard
      case x if x.startsWith("google/protobuf/") => MergeStrategy.first
      case x if x.startsWith("org/apache/commons/logging/") => MergeStrategy.first
      case x if x.endsWith(".proto") => MergeStrategy.first
      case _ => MergeStrategy.first
    }
  )