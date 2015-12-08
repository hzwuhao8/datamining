name := """datamining"""

version := "1.0-SNAPSHOT"


scalaVersion := "2.10.6"


resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"

libraryDependencies ++= Seq(
	"org.apache.spark" % "spark-core_2.10" % "1.5.2",
	"org.apache.spark" % "spark-mllib_2.10" % "1.5.2" 
)


