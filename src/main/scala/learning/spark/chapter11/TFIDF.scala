package learning.spark.chapter11

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ZackL on 2018/9/16.
  */
object TFIDF extends App {
  val conf = new SparkConf().setMaster("local[*]").setAppName("TFIDF")
  val sc = new SparkContext(conf)

  val spam = sc.textFile("files/spam.txt")
  val normal = sc.textFile("files/ham.txt")

  val tf = new HashingTF(numFeatures = 10000)

  val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
  spamFeatures.cache()
  val normalFeatures = normal.map(email => tf.transform(email.split(" ")))
  normalFeatures.cache()

  val idf = new IDF()
  val entireFeatures = spamFeatures.union(normalFeatures)
  val idfModel = idf.fit(entireFeatures)

  val spamTfIdfFeature = idfModel.transform(spamFeatures)
  val normalTfIdfFeature = idfModel.transform(normalFeatures)

  val positiveExamples = spamTfIdfFeature.map(feature => LabeledPoint(1, feature))
  val negativeExamples = normalTfIdfFeature.map(feature => LabeledPoint(0, feature))

  val trainData = positiveExamples.union(negativeExamples)
  trainData.cache()

  val model = new LogisticRegressionWithLBFGS().run(trainData)

  val posTest = idfModel.transform(tf.transform(
    "O M G GET cheap stuff by sending money to ...".split(" ")))
  val negTest = idfModel.transform(tf.transform(
    "Hi Dad, I started studying Spark the other ...".split(" ")))

  println("""The prediction for "O M G GET cheap stuff by sending money to ..." is """ + model.predict(posTest))
  println("""The prediction for "Hi Dad, I started studying Spark the other ..." is """ + model.predict(negTest))

}
