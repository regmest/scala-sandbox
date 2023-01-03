import sbt.Resolver

name := "te"

version := "0.1"

scalaVersion := "2.11.12"

resolvers ++= Seq(
  "Cloudera repos" at "https://repository.cloudera.com/artifactory/cloudera-repos",
  "Spring plugins" at "https://repo.spring.io/plugins-release",
  Resolver.typesafeRepo("releases")
)

val sparkVersion = "2.4.0-cdh6.3.0"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  "com.github.scopt" %% "scopt" % "3.7.0"

)

assemblyJarName in assembly := "te-fatjar-1.0.jar"
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard // вроде не нужен для незапускаемых jar
  case x => MergeStrategy.first
}
