import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task2 {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Task 2")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val numPartitions = if (args.length > 2) args(2).toInt else sc.defaultParallelism
    val textFile = sc.textFile(args(0), numPartitions)

    // Efficient per-line counting
    val ratingCounts = textFile.map { line =>
      val parts = line.split(",", -1)
      var count = 0
      var i = 1
      while (i < parts.length) {
        if (parts(i).trim.nonEmpty) count += 1
        i += 1
      }
      count
    }

    // Sum up all counts to get the total number of ratings
    val totalRatings = ratingCounts.reduce(_ + _)

    // Save the result as a single line in a single file
    sc.parallelize(Seq(totalRatings)).saveAsTextFile(args(1))
  }
}
