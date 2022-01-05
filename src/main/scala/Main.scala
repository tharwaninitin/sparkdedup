import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.{current_timestamp, lit}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
import org.apache.spark.sql.types._

object Main extends App {

  case class Rating(user_id: Int, movie_id: Int, rating: Double, timestamp: Long, watermark_ts: java.sql.Timestamp)
  case class RatingFlag(user_id: Int, movie_id: Int, rating: Double, timestamp: Long, is_duplicate: Boolean)

  val spark: SparkSession = SparkManager.createSparkSession(hive_support = false)

  def deduplicate(
      currentKey: (Int, Int),
      currentBatchData: Iterator[Rating],
      state: GroupState[(Int, Int)]
  ): Iterator[RatingFlag] = {

    val currentState: Option[(Int, Int)] = state.getOption

    val list = currentBatchData.toList

    val localDeduplication =
      for { i <- list.indices } yield
        if (i == 0)
          RatingFlag(list(i).user_id, list(i).movie_id, list(i).rating, list(i).timestamp, is_duplicate = false)
        else
          RatingFlag(list(i).user_id, list(i).movie_id, list(i).rating, list(i).timestamp, is_duplicate = true)

    val finalOp = localDeduplication.map { item =>
      if (currentState.isDefined) {
        if (item.user_id == currentState.get._1 && item.movie_id == currentState.get._2)
          RatingFlag(item.user_id, item.movie_id, item.rating, item.timestamp, is_duplicate = true)
        else
          RatingFlag(item.user_id, item.movie_id, item.rating, item.timestamp, is_duplicate = false)
      } else {
        state.update((item.user_id, item.movie_id))
        item
      }
    }

    finalOp.toIterator
  }

  import spark.implicits._
  val enc = Encoders.product[Rating]
  val schema = new StructType()
    .add("user_id", IntegerType)
    .add("movie_id", IntegerType)
    .add("rating", DoubleType)
    .add("timestamp", LongType)

  val df = spark.readStream
    .schema(schema)
    .option("header", "true")
    .csv("src/main/resources/input/ratings/*")
    .withColumn("watermark_ts", lit(current_timestamp()))
    .as[Rating](enc)
    .withWatermark("watermark_ts", "10 days")
    .groupByKey(r => (r.user_id, r.movie_id))
    .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.EventTimeTimeout())(deduplicate)
    .coalesce(1)
    .writeStream
    .format("json")
    .outputMode("append")
    .trigger(Trigger.Once())
    .option("checkpointLocation", "src/main/resources/checkpoint")
    .start("src/main/resources/output")
    .awaitTermination()

}
