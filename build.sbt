import sbt.Keys._

name := "yn-streaming"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "0.10.1.0"

libraryDependencies += "com.typesafe" % "config" % "1.2.1"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.42"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "provided"

libraryDependencies += "redis.clients" % "jedis" % "2.9.0"


lazy val hbase_lib = Seq(
    "org.apache.hbase" % "hbase-client" % "1.2.4" % "provided",
    "org.apache.hbase" % "hbase-common" % "1.2.4" % "provided",
    "org.apache.hbase" % "hbase-server" % "1.2.4"  % "provided" excludeAll ExclusionRule(organization = "org.mortbay.jetty")
)
libraryDependencies ++= hbase_lib

lazy val spark_version = "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % spark_version
libraryDependencies +=  "org.apache.spark" %% "spark-sql-kafka-0-10" % spark_version

lazy val spark_lib = Seq(
    "org.apache.spark" %% "spark-core" % spark_version ,
    "org.apache.spark" %% "spark-sql" % spark_version ,
    "org.apache.spark" %% "spark-streaming" % spark_version
)

libraryDependencies ++= spark_lib.map(_ % "provided")

test in assembly := {}
assemblyJarName in assembly := "yn-streaming.jar"
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
assemblyExcludedJars in assembly := {
    val cp = (fullClasspath in assembly).value
    cp filter { el =>
        (el.data.getName == "unused-1.0.0.jar") ||
        (el.data.getName == "spark-tags_2.11-2.1.0.jar")
    }
}

lazy val mainRunner = project.in(file("mainRunner")).dependsOn(RootProject(file("."))).settings(
    scalaVersion := "2.11.8",
    libraryDependencies ++= spark_lib.map(_ % "compile")
).disablePlugins(sbtassembly.AssemblyPlugin)