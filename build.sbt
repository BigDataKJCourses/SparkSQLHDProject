ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.mysql" % "mysql-connector-j" % "9.0.0",
  "com.databricks" %% "spark-xml" % "0.18.0"
)

lazy val root = (project in file("."))
  .settings(
    name := "SparkSQLHDProject"
  )

// Ustawienie niestandardowej nazwy dla pliku JAR
artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  s"${name.value}.jar"
}
