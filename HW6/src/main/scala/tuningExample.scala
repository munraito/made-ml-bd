import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.made.RndHpCosLSH

object tuningExample extends App {
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("hw6")
    .getOrCreate()

  // sample size just to speed up test runs
  val data = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("tripadvisor_hotel_reviews.csv")
    .sample(0.05)
//  data.show(10)

  val tokenizer = new Tokenizer()
    .setInputCol("Review")
    .setOutputCol("words")
  val hashingTF = new HashingTF()
    .setNumFeatures(1000)
    .setInputCol(tokenizer.getOutputCol)
    .setOutputCol("features")
  val idf = new IDF()
    .setInputCol(hashingTF.getOutputCol)
    .setOutputCol("tfidf")
  val tfidfPipe = new Pipeline()
    .setStages(Array(tokenizer, hashingTF, idf))

  val Array(train, test) = data.randomSplit(Array(0.8, 0.2), seed=1337)
  val pipe = tfidfPipe.fit(train)
  val XTrain = pipe.transform(train).cache()
  val XTest = pipe.transform(test).withColumn("id", monotonically_increasing_id()).cache()
//  XTrain.show(10)
//  XTest.show(10)

  println("Start searching for best hyperparameters")
  val results = Array.range(3, 6, 2).map(numHashes => {
    val rhc =  new RndHpCosLSH()
      .setInputCol("tfidf")
      .setOutputCol("buckets")
      .setNumHashTables(numHashes)
      .setSeed(1337)
      .fit(XTrain)
    val thresholds = Array( .7, .8).map(thres => { // .4, .5, .6,
      val neighbors = rhc.approxSimilarityJoin(XTrain, XTest, thres)
      val preds = neighbors
        .withColumn("similarity", lit(1) - col("distCol"))
        .groupBy("datasetB.id")
        .agg(
          (sum(col("similarity") * col("datasetA.Rating")) / sum(col("similarity"))).as("predict") ,
          count("datasetA.Rating").as("numNeighbors")
        )
      val forMetric = XTest.join(preds, Seq("id"))
      val metrics = new RegressionEvaluator()
        .setLabelCol("Rating")
        .setPredictionCol("predict")
        .setMetricName("rmse")
      val numNeighbors = forMetric.select(avg("numNeighbors")).collect.head(0)
      val currRMSE = metrics.evaluate(forMetric)
      println(s"numHashes=${numHashes}, threshold=${thres}:")
      println(s"RMSE=${currRMSE}, neighbors=${numNeighbors}")
      (numHashes, thres, currRMSE)
    })
    thresholds
  })
  var bestRes = (-1, -1.0, 1e10)
  println("All results:")
  results.map(x => x.map(y => {
    println(y)
    if (y._3 < bestRes._3) {
      bestRes = y
    }
  }))
  println(s"Best RMSE: ${bestRes._3} with numHashes=${bestRes._1} and threshold=${bestRes._2}")
}
