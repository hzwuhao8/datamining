package spark.ml

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.RandomForestClassificationModel

object App03 extends spark.Base {
  import sqlSc.implicits._

  case class Data(age: String, prescript: String, astigmatic: String, tearRate: String, label: String)

  val txt = """young	myope	no	reduced	no lenses
young	myope	no	normal	soft
young	myope	yes	reduced	no lenses
young	myope	yes	normal	hard
young	hyper	no	reduced	no lenses
young	hyper	no	normal	soft
young	hyper	yes	reduced	no lenses
young	hyper	yes	normal	hard
pre	myope	no	reduced	no lenses
pre	myope	no	normal	soft
pre	myope	yes	reduced	no lenses
pre	myope	yes	normal	hard
pre	hyper	no	reduced	no lenses
pre	hyper	no	normal	soft
pre	hyper	yes	reduced	no lenses
pre	hyper	yes	normal	no lenses
presbyopic	myope	no	reduced	no lenses
presbyopic	myope	no	normal	no lenses
presbyopic	myope	yes	reduced	no lenses
presbyopic	myope	yes	normal	hard
presbyopic	hyper	no	reduced	no lenses
presbyopic	hyper	no	normal	soft
presbyopic	hyper	yes	reduced	no lenses
presbyopic	hyper	yes	normal	no lenses
"""
  val textSeq = txt.split("\n").toSeq
  val dataSet = textSeq.map(_.split("\t")).
    map(arr => Data(arr(0), arr(1), arr(2), arr(3), arr(4)))

  val df = dataSet.toDF()
  val cvModel1 = new StringIndexer().setInputCol("age").setOutputCol("age2").fit(df)

  val cvModel2 = new StringIndexer().setInputCol("prescript").setOutputCol("prescript2").fit(df)

  val cvModel3 = new StringIndexer().setInputCol("astigmatic").setOutputCol("astigmatic2").fit(df)

  val cvModel4 = new StringIndexer().setInputCol("tearRate").setOutputCol("tearRate2").fit(df)

  val cvModel5 = new StringIndexer().setInputCol("label").setOutputCol("label2").fit(df)

  val assembler = new VectorAssembler().setInputCols(Array("age2", "prescript2", "astigmatic2","tearRate2"))
    .setOutputCol("features")

  val pipeline = new Pipeline().setStages(Array(cvModel1, cvModel2, cvModel3, cvModel4,cvModel5, assembler))

  val m = pipeline.fit(df).transform(df)
  m.show()
  val df2 = m 
  
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
  predictions.show()
   
}