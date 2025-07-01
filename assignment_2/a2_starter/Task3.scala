import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task3 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 3")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // Read all lines and split into arrays
    val lines = textFile.map(line => line.split(",").map(_.trim)).collect()

    // Find the maximum number of user columns (excluding the movie name)
    val numUsers = lines.map(parts => parts.length - 1).max

    // Initialize an array to hold counts for each user
    val userCounts = Array.fill(numUsers)(0)

    // Count non-blank ratings for each user
    for (parts <- lines) {
      for (i <- 1 to numUsers) {
        if (i < parts.length && parts(i).nonEmpty) {
          userCounts(i - 1) += 1
        }
      }
    }

    // Prepare output: (user column number, count) for each user
    val output = sc.parallelize((1 to numUsers).map(i => s"$i,${userCounts(i - 1)}"))
    output.saveAsTextFile(args(1))
  }
}
