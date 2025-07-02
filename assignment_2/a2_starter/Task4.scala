import org.apache.spark.{SparkConf, SparkContext}

// please don't change the object name
object Task4 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val inputPath = args(0)
    val outputPath = args(1)

    // Read file: (movie, Array[rating strings])
    val moviesRDD = sc.textFile(inputPath)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { line =>
        val parts = line.split(",").map(_.trim)
        val movie = parts(0)
        val ratings = parts.drop(1)
        (movie, ratings)
      }

    // Cartesian self-join and filter unique pairs (lexicographic order)
    val moviePairs = moviesRDD.cartesian(moviesRDD)
      .filter { case ((movieA, _), (movieB, _)) => movieA < movieB }

    // Compute similarity for each unique pair
    val similarities = moviePairs.map { case ((movieA, ratingsA), (movieB, ratingsB)) =>
      val minLen = math.min(ratingsA.length, ratingsB.length)
      var similarity = 0
      for (i <- 0 until minLen) {
        val a = ratingsA(i)
        val b = ratingsB(i)
        if (a.nonEmpty && b.nonEmpty && a == b) {
          similarity += 1
        }
      }
      s"$movieA,$movieB,$similarity"
    }

    // Save output (no need to sort; test script does it)
    similarities.saveAsTextFile(outputPath)
  }
}
