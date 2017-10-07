name := "map-reduce"
version := "1.0"
scalaVersion := "2.11.8"
libraryDependencies ++= Seq(
	"org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.8.1",
	"org.apache.hadoop" % "hadoop-common" % "2.8.1",
	"org.scala-lang" % "scala-library" %"2.11.8"
)