package spark.sql


object ReadJson  extends spark.Base {
   import sqlSc.implicits._
   val df =sqlSc.read.json("/tmp/h1.json").toDF()
   
   df.show()

}