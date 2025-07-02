import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task4 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // Parse each line into (movie, ratings) pairs
    val movies = textFile.map { line =>
      val parts = line.split(",", -1).map(_.trim)
      val movie = parts(0)
      val ratings = parts.drop(1)
      (movie, ratings)
    }

    // Generate all movie pairs using cartesian product
    val moviePairs = movies.cartesian(movies)
      .filter { case ((movieA, _), (movieB, _)) => movieA < movieB } // lexicographic order
      .map { case ((movieA, ratingsA), (movieB, ratingsB)) =>
        // Compute similarity: count users who gave same rating to both movies
        val minLen = math.min(ratingsA.length, ratingsB.length)
        var similarity = 0
        var k = 0
        while (k < minLen) {
          if (ratingsA(k).nonEmpty && ratingsB(k).nonEmpty && ratingsA(k) == ratingsB(k)) {
            similarity += 1
          }
          k += 1
        }
        s"$movieA,$movieB,$similarity"
      }

    // Output the results
    moviePairs.saveAsTextFile(args(1))
  }
}
