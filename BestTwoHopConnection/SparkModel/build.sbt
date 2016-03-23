lazy val root = (project in file(".")).
  settings(
    name := "SparkModel2",
    version := "1.0",
    scalaVersion := "2.10.6",
    libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.6.0",
    "org.apache.spark" %% "spark-mllib" % "1.6.0"))
