import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task4 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))
    // TEST
    // Parse each line into (movie, Array[rating])
    val movieRatings = textFile.map { line =>
      val parts = line.split(",", -1)
      val movie = parts(0).trim
      val ratings = parts.drop(1).map(_.trim)
      (movie, ratings)
    }

    // For each user, emit (userIndex, (movie, rating))
    val userMovieRatings = movieRatings.flatMap { case (movie, ratings) =>
      ratings.zipWithIndex.collect {
        case (rating, idx) if rating.nonEmpty =>
          (idx, (movie, rating))
      }
    }

    // For each user, generate unique pairs of movies with the same rating
    val moviePairs = userMovieRatings
      .groupByKey()
      .flatMap { case (_, movieRatingIterable) =>
        val arr = movieRatingIterable.toArray
        // Only consider each pair once (A < B)
        for {
          i <- arr.indices
          j <- (i + 1) until arr.length
          (movieA, ratingA) = arr(i)
          (movieB, ratingB) = arr(j)
          if movieA < movieB && ratingA == ratingB
        } yield ((movieA, movieB), 1)
      }

    // Use reduceByKey to sum up similarities
    val similarityCounts = moviePairs
      .reduceByKey(_ + _)
      .map { case ((a, b), count) => s"$a,$b,$count" }

    similarityCounts.saveAsTextFile(args(1))
  }
}
