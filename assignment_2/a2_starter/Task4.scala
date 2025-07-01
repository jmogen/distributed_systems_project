import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task4 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // Read all lines and split into (movie name, ratings array)
    val lines = textFile.map(line => {
      val parts = line.split(",").map(_.trim)
      val movie = parts(0)
      val ratings = parts.drop(1)
      (movie, ratings)
    }).collect()

    // Get all unique pairs of movies (sorted lexicographically)
    val moviePairs = for {
      i <- lines.indices
      j <- (i + 1) until lines.length
      movieA = lines(i)._1
      movieB = lines(j)._1
      if movieA < movieB // lexicographic order
    } yield ((movieA, lines(i)._2), (movieB, lines(j)._2))

    // For each pair, compute similarity
    val results = moviePairs.map { case ((movieA, ratingsA), (movieB, ratingsB)) =>
      val minLen = math.min(ratingsA.length, ratingsB.length)
      var similarity = 0
      for (k <- 0 until minLen) {
        if (ratingsA(k).nonEmpty && ratingsB(k).nonEmpty && ratingsA(k) == ratingsB(k)) {
          similarity += 1
        }
      }
      s"$movieA,$movieB,$similarity"
    }

    // Output the results
    val output = sc.parallelize(results)
    output.saveAsTextFile(args(1))
  }
}
