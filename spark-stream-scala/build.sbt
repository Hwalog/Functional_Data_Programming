// The simplest possible sbt build file is just one line:
//import AssemblyKeys._ 
scalaVersion := "2.11.10"

name := "kafka-app"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "1.6.3",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.3",
  "org.apache.kafka" %% "kafka" % "0.8.2.1",
  "org.apache.spark" %% "spark-sql" % "1.6.3"
)


mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}
