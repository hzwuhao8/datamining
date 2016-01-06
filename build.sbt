name := """datamining"""

version := "1.0-SNAPSHOT"


scalaVersion := "2.10.6"


resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"

resolvers += "NLP" at "http://maven.nlpcn.org"

libraryDependencies ++= Seq(
	"org.apache.spark" % "spark-core_2.10" % "1.5.2",
	"org.apache.spark" % "spark-mllib_2.10" % "1.5.2" ,
	"org.ansj" % "ansj_seg" % "3.0" , 
	"com.github.fommil.netlib" % "all" % "1.1.2" pomOnly() 
	 
	
)


assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

