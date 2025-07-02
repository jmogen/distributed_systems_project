import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task3 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 3")
    val sc = new SparkContext(conf)

    val numPartitions = if (args.length > 2) args(2).toInt else sc.defaultParallelism
    val textFile = sc.textFile(args(0), numPartitions)

    // Find the maximum number of user columns (excluding the movie name)
    val numUsers = textFile.map { line =>
      line.split(",", -1).length - 1
    }.reduce(math.max)

    // For each line, emit (userIndex, 1) for each non-blank rating
    val userCounts = textFile.flatMap { line =>
      val parts = line.split(",", -1)
      for {
        i <- 1 until parts.length
        if parts(i).trim.nonEmpty
      } yield (i, 1)
    }
    .reduceByKey(_ + _)
    .collectAsMap() // bring only the small result to the driver

    // Prepare output for all users, filling in zeros where needed
    val output = sc.parallelize(
      (1 to numUsers).map(i => s"$i,${userCounts.getOrElse(i, 0)}")
    )
    output.saveAsTextFile(args(1))
  }
}
