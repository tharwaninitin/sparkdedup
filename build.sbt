val scala212         = "2.12.15"
val scala213         = "2.13.7"
val allScalaVersions = List(scala212, scala213)
val Version          = "0.1.0"
val SparkVersion     = "3.2.0"
val LogbackVersion   = "1.2.10"
val Sl4jVersion      = "1.7.32"

lazy val root = (project in file("."))
  .settings(
    name         := "sparkdedup",
    version      := Version,
    scalaVersion := scala212,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql"       % SparkVersion,
      "org.slf4j"         % "slf4j-api"       % Sl4jVersion,
      "ch.qos.logback"    % "logback-classic" % LogbackVersion
    ),
    Test / parallelExecution := false
  )
