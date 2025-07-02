import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task4 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // Parse each line into (movie, Array[rating])
    val movieRatings = textFile.map { line =>
      val parts = line.split(",", -1).map(_.trim)
      val movie = parts(0)
      val ratings = parts.drop(1)
      (movie, ratings)
    }.cache()

    // Transpose the ratings: for each user, get (userIndex, (movie, rating))
    val userMovieRatings = movieRatings.flatMap { case (movie, ratings) =>
      ratings.zipWithIndex.collect {
        case (rating, idx) if rating.nonEmpty =>
          (idx, (movie, rating))
      }
    }

    // For each user, get all pairs of movies they rated with the same rating
    val moviePairs = userMovieRatings
      .groupByKey() // group by user
      .flatMap { case (_, movieRatingIterable) =>
        val movieRatingList = movieRatingIterable.toList
        // For each pair of movies with the same rating, emit ((movieA, movieB), 1)
        for {
          (movieA, ratingA) <- movieRatingList
          (movieB, ratingB) <- movieRatingList
          if movieA < movieB && ratingA == ratingB
        } yield ((movieA, movieB), 1)
      }

    // Count the number of users who gave the same rating to both movies
    val similarityCounts = moviePairs
      .reduceByKey(_ + _)

    // Get all movie names (for zero similarity pairs)
    val allMovies = movieRatings.map(_._1).distinct().collect().sorted

    // Generate all possible pairs (lex order)
    val allPairs = sc.parallelize(
      for {
        i <- allMovies.indices
        j <- (i + 1) until allMovies.length
      } yield (allMovies(i), allMovies(j))
    )

    // Join with similarity counts, fill in zeros where needed
    val similarityMap = similarityCounts.map { case ((a, b), count) => ((a, b), count) }
    val result = allPairs
      .map(pair => (pair, 0))
      .leftOuterJoin(similarityMap)
      .map { case ((a, b), (zero, optCount)) =>
        s"$a,$b,${optCount.getOrElse(0)}"
      }

    result.saveAsTextFile(args(1))
  }
}
