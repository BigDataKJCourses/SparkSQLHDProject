ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.16"

val sparkVersion = "4.0.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.mysql" % "mysql-connector-j" % "9.5.0"
)

lazy val root = (project in file("."))
  .settings(
    name := "SparkSQLHDProject"
  )

// Ustawienie niestandardowej nazwy dla pliku JAR
artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  s"${name.value}.jar"
}
