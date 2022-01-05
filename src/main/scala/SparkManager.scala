import org.apache.spark.sql.SparkSession

object SparkManager extends ApplicationLogger {
  private def showSparkProperties(spark: SparkSession): Unit = {
    logger.info("spark.scheduler.mode = " + spark.sparkContext.getSchedulingMode)
    logger.info("spark.default.parallelism = " + spark.conf.getOption("spark.default.parallelism"))
    logger.info("spark.sql.shuffle.partitions = " + spark.conf.getOption("spark.sql.shuffle.partitions"))
    logger.info(
      "spark.sql.sources.partitionOverwriteMode = " + spark.conf.getOption("spark.sql.sources.partitionOverwriteMode")
    )
    logger.info("spark.sparkContext.uiWebUrl = " + spark.sparkContext.uiWebUrl)
    logger.info("spark.sparkContext.applicationId = " + spark.sparkContext.applicationId)
    logger.info("spark.sparkContext.sparkUser = " + spark.sparkContext.sparkUser)
    logger.info("spark.eventLog.dir = " + spark.conf.getOption("spark.eventLog.dir"))
    logger.info("spark.eventLog.enabled = " + spark.conf.getOption("spark.eventLog.enabled"))
    spark.conf.getAll.filter(m1 => m1._1.contains("yarn")).foreach(kv => logger.info(kv._1 + " = " + kv._2))
  }

  def createSparkSession(
      props: Map[String, String] = Map(
        "spark.scheduler.mode"                     -> "FAIR",
        "spark.sql.sources.partitionOverwriteMode" -> "dynamic",
        "spark.default.parallelism"                -> "10",
        "spark.sql.shuffle.partitions"             -> "10"
      ),
      hive_support: Boolean = true
  ): SparkSession =
    if (SparkSession.getActiveSession.isDefined) {
      val spark = SparkSession.getActiveSession.get
      logger.info(s"###### Using Already Created Spark Session ##########")
      spark
    } else {
      logger.info(s"###### Creating Spark Session ##########")
      var sparkBuilder = SparkSession.builder()

      props.foreach { prop =>
        sparkBuilder = sparkBuilder.config(prop._1, prop._2)
      }

      sparkBuilder = sparkBuilder
        .config("spark.ui.enabled", "false")
        .master("local[*]")

      if (hive_support) sparkBuilder = sparkBuilder.enableHiveSupport()

      val spark = sparkBuilder.getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
      showSparkProperties(spark)
      spark
    }
}
