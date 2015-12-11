package bigdata.spark.df

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.CaseWhen

/**
 * 读入数据
 * 从 json 中
 * 在组合数据
 * 将 有结果的作为 训练数据集
 * 没有结果的 作为  待处理数据集
 */
object Reader extends App {
  val appName = "Dface"
  val conf = new SparkConf().setAppName(appName).setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = SQLContext.getOrCreate(sc)
  import sqlContext.implicits._

  val shops = sqlContext.read.json("/tmp/dfacedata/shops.0571.json")
  val names = sqlContext.read.json("/tmp/dfacedata/names.0571.json")
  val similar3a = sqlContext.read.json("/tmp/dfacedata/similar3a.0571.json")

  //以上是 原始的数据 
  /**
   * shops , 最原始的数据
   * names , 对 name 分词后的数据
   * similar3a, 计算了 距离 相似度， 部分数据 有 人工判断的结果
   *
   */

  /**
   *
   * 和数据集是有关系的。
   * 必须 全局计算。
   * 这样 ，每次增加数据的 时候 都是变化的。 如何处理？？？？
   * 不同的集合 计算后 没有可比性？？
   * 先不考虑这个， 这个太复杂
   * val word2Vec = new Word2Vec()
   * .setInputCol("seq")
   * .setOutputCol("result")
   * .setVectorSize(10)
   * .setMinCount(0)
   *
   *
   *
   */
  /**
   * 将数据 转换成  合适的格式
   * DATAFRAME 支持 嵌套属性的
   * (id1-id2,id1,id2, name1, name2,seq1,seq2,tel1,tel2,password1,password2,type1,type2,t1,t2, c,c1,c2,d,menu1,menu2,utotal1,utotal2,res)
   * (id1-id2, s1(id,name,seq,tel,password,type,t,menu,utotal,city),s2( ....), c , d, c1, c2, res)
   *
   * tel  , password,menu -> 00,01,10,11 4中情况 ；  有 没有 组合
   *
   *  c 和 d , c1 ,c2 有关
   *  c1 , c2 和 seq1, seq2 有关
   *  shops.select("_id","name","tel","password","has_menu","type","t","utotal","city")
   * val namespart = names.select("_id", "seq")
   * val shoptmp =  shops.join(namespart,"_id").select("_id","name","tel","password","has_menu","type","t","utotal","city","seq")
   * val similar3apart = similar3a.selectExpr("_id","name as uname" , "s1.id as id1 ","s2.id as id2","c","c1","c2","d","res")
   *
   * val shoptmpA = shoptmp.select($"_id", $"name",when($"tel".isNotNull , 1 ).otherwise(0).as("tel")
   * ,when($"password".isNotNull, 1 ).otherwise(0).as("password"),
   * when($"has_menu".isNotNull  && $"has_menu".===(2) , 1 ).otherwise(0).as("menu") ,$"type",$"t",$"utotal",$"city",$"seq" )
   *
   * val shoptmpA1 = shoptmpA.select($"_id".as("id1"),$"name".as("name1"),$"tel".as("tel1")
   * ,$"password".as("password1"),$"menu".as("menu1"),$"type".as("type1")
   * ,$"t".as("t1"),$"utotal".as("utotal1"),$"city".as("city1"),$"seq".as("seq1"))
   *
   *
   *
   *
   * val shoptmpA2 = shoptmpA.select($"_id".as("id2"),$"name".as("name2"),$"tel".as("tel2"),
   * $"password".as("password2"),$"menu".as("menu2"),$"type".as("type2"),
   * $"t".as("t2"),$"utotal".as("utotal2"),$"city".as("city2"),$"seq".as("seq2"))
   *
   *
   * val p1 = similar3apart.join(shoptmpA1 , "id1").join(shoptmpA2,"id2")
   *
   *
   */
 val t = new Column( CaseWhen(Nil))
  
  val namespart = names.select("_id", "seq")
  val shoptmp = shops.join(namespart, "_id").select("_id", "name", "tel", "password", "has_menu", "type", "t", "utotal", "city", "seq")
  val similar3apart = similar3a.select($"_id", $"name".as("uname"), $"s1.id".as("id1"), $"s2.id".as("id2")
      , $"c", $"c1", $"c2", $"d", 
      t.when($"res".isNotNull, $"res").otherwise("").as("res"))

 
  
  val shoptmpA = shoptmp.select($"_id", $"name", t.when($"tel".isNotNull, 1).otherwise(0).as("tel"),
    t.when($"password".isNotNull, 1).otherwise(0).as("password"),
    t.when($"has_menu".isNotNull && $"has_menu".===(2), 1).otherwise(0).as("menu"),
    $"type", 
    t.when($"t".isNotNull , $"t").otherwise(0).as("t"), 
    t.when($"utotal".isNotNull, $"utotal").otherwise(0).as("utotal"), 
    $"city", $"seq")

  val shoptmpA1 = shoptmpA.select($"_id".as("id1"), $"name".as("name1"), $"tel".as("tel1"),
    $"password".as("password1"), $"menu".as("menu1"), $"type".as("type1"), $"t".as("t1"),
    $"utotal".as("utotal1"), $"city".as("city1"), $"seq".as("seq1"))

  val shoptmpA2 = shoptmpA.select($"_id".as("id2"), $"name".as("name2"), $"tel".as("tel2"),
    $"password".as("password2"), $"menu".as("menu2"), $"type".as("type2"),
    $"t".as("t2"), $"utotal".as("utotal2"), $"city".as("city2"), $"seq".as("seq2"))

  val p1 = similar3apart.join(shoptmpA1, "id1").join(shoptmpA2, "id2")
  
  p1.write.parquet("/tmp/p1txt")
  
  val df = p1.filter("res != \"\" ")
  
  
  val assembler = new VectorAssembler().setInputCols(Array("c", "c1", "c2", "d",
    "tel1", "tel2", "password1", "password2", "menu1", "menu2",
    "t1", "t2", "utotal1", "utotal2")).setOutputCol("features")

  val labeler = new StringIndexer().setInputCol("res").setOutputCol("label")

  val pipeline = new Pipeline().setStages(Array(labeler, assembler))

  val df2 = pipeline.fit(df).transform(df)

  
  //df2.show();

  val featureIndexed = new VectorIndexer().
    setInputCol("features").setOutputCol("indexedFeatures").
    setMaxCategories(4).fit(df2)

  val labelIndexer = new StringIndexer().setInputCol("label").
    setOutputCol("indexedLabel").fit(df2)

  val rf = new RandomForestClassifier().setLabelCol("indexedLabel").
    setFeaturesCol("indexedFeatures").setNumTrees(3)

  val labelConverter = new IndexToString().setInputCol("prediction").
    setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

  val pipeline2 = new Pipeline().setStages(Array(labelIndexer, featureIndexed, rf, labelConverter))

  val model = pipeline2.fit(df2)
  val str = model.stages(2).asInstanceOf[RandomForestClassificationModel].toDebugString

  println(str)
  val predictions = model.transform(df2)
  predictions.select("_id","res","label","prediction","predictedLabel","uname").show()

  val dft1 = p1.filter("res = \"\" ")
  val df3 = pipeline.fit(dft1).transform(dft1)
  val predictions2 = model.transform(df3)
  predictions2.select("_id","res","label","prediction","predictedLabel","uname").show()
  /**
   *  由于 id1-id2  的 组合，不一定是  name1<br>name2  ; 这里存在问题，
   *  组合成主键的时候 id1,id2 进行了排序， 但是 name1 和 name2 没有相应的排序， 所以
   *  存在问题
   *  需要从 name1 -> id1 , name2 -> id2
   *  但这里的数据 是从 s1, s2 取到 id1,id2, 没有使用 看上去的对应关系
   *  在 最后应用 人工审核的结果时 ， 需要对数据 进行一次校验 根据  name1<br>name2 得到 实际的 id1,id2
   *
   * 得到 p1 后
   *
   * res 是 label
   * c,c1,c2,
   * tel1,password1,menu1,
   *
   * t1,
   * utotal1
   *
   * tel2,password2,menu2,
   * t2,
   * utotal2
   *
   * val cvModel1tel = new StringIndexer().setInputCol("tel1").setOutputCol("tel1a").fit(df)
   *
   * val cvModel1password = new StringIndexer().setInputCol("password1").setOutputCol("password1a").fit(df)
   *
   * val cvModel1menu = new StringIndexer().setInputCol("tel1").setOutputCol("tel1a").fit(df)
   *
   * List("tel","password","menu").map{ key => new StringIndexer().setInputCol(s"${key}1").setOutputCol(s"${key}1a") }
   *
   * List("tel","password","menu").map{ key => new StringIndexer().setInputCol(s"${key}2").setOutputCol(s"${key}2a") }
   *
   *
   *
   */
}