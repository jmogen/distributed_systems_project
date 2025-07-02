import org.apache.spark.{SparkConf, SparkContext}

// please don't change the object name
object Task4 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val inputPath = args(0)
    val outputPath = args(1)

    // Read the input: (movie, Array of ratings)
    val moviesRDD = sc.textFile(inputPath)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { line =>
        val parts = line.split(",").map(_.trim)
        val movie = parts(0)
        val ratings = parts.drop(1)
        (movie, ratings)
      }
      // Repartition to 1 for best runtime on current input size
      .repartition(2)
      .cache()  // Cache since used multiple times (cartesian can be expensive)

    // Cartesian self-join, filter unique pairs lex order (movieA < movieB)
    val moviePairs = moviesRDD.cartesian(moviesRDD)
      .filter { case ((movieA, _), (movieB, _)) => movieA < movieB }

    // Compute similarity: count positions where ratings match exactly
    val similarities = moviePairs.map { case ((movieA, ratingsA), (movieB, ratingsB)) =>
      val minLen = math.min(ratingsA.length, ratingsB.length)
      var similarity = 0
      for (i <- 0 until minLen) {
        if (ratingsA(i).nonEmpty && ratingsB(i).nonEmpty && ratingsA(i) == ratingsB(i)) {
          similarity += 1
        }
      }
      s"$movieA,$movieB,$similarity"
    }

    // Save output without sorting (test script will sort)
    similarities.saveAsTextFile(outputPath)

    sc.stop()
  }
}


