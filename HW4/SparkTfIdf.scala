import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SparkTfIdf {
  def main(args: Array[String]): Unit = {
    // make spark session
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("hw4")
      .getOrCreate()

    // dataset: https://www.kaggle.com/andrewmvd/trip-advisor-hotel-reviews

    val data = spark.read
      // read:
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("tripadvisor_hotel_reviews.csv")
      .drop("Rating")
      // clean:
      .withColumn("Review", lower(col("Review")))
      .withColumn("Review", regexp_replace(col("Review"), "[^a-z0-9 ]", ""))
      .withColumn("Review", split(col("Review"), " "))
      // make IDs:
      .withColumn("review_id", monotonically_increasing_id())

    // unfold each review to words
    val columns = data.columns.map(col) :+
      (explode(col("Review")) as "word")
    val unfolded = data
      .select(columns: _*)
      .filter(col("word") =!= "")

    // calculate term frequencies
    val tf = unfolded
      .groupBy("review_id", "word")
      .agg(countDistinct("Review") as "tf")

    // calculate document frequencies, get top 100
    val df = unfolded
      .groupBy("word")
      .agg(countDistinct("review_id") as "df")
      .orderBy(desc("df"))
      .limit(100)
      .cache()

    // calculate inverse document frequency
    def calcIdf(docCount: Long, df: Long): Double =
      math.log((docCount.toDouble + 1) / (df.toDouble + 1))
    val docCount = data.count()
    val calcIdfUdf = udf { df: Long => calcIdf(docCount, df) }

    val idf = df
      .withColumn("idf", calcIdfUdf(col("df")))
      .drop("df")
      .cache()

    // join into final dataframe and pivot
    val wordsTfIdf = idf
      .join(tf, Seq("word"), "left")
      .withColumn("tf_idf", col("tf") * col("idf"))
      .drop("tf", "idf")
    val pivoted = wordsTfIdf
      .groupBy("review_id").pivot("word").max("tf_idf")

    pivoted.show()
  }
}
