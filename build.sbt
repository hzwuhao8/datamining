name := """datamining"""

version := "1.0-SNAPSHOT"


scalaVersion := "2.10.6"


resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"

resolvers += "NLP" at "http://maven.nlpcn.org"

libraryDependencies ++= Seq(
	"org.apache.spark" % "spark-core_2.10" % "1.6.0",
	"org.apache.spark" % "spark-mllib_2.10" % "1.6.0" ,
	"org.ansj" % "ansj_seg" % "3.0" , 
	"com.github.fommil.netlib" % "all" % "1.1.2" pomOnly(),
	"com.databricks" % "spark-csv_2.10" % "1.3.0",
	"org.jsoup" % "jsoup" % "1.8.3"
)



