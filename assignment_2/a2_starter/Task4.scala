import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task4 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // Parse each line into (movie, Array[rating])
    val movieRatings = textFile.map { line =>
      val parts = line.split(",", -1)
      val movie = parts(0).trim
      val ratings = parts.drop(1).map(_.trim)
      (movie, ratings)
    }

    // For each user, emit (userIndex, (movie, rating))
    val userMovieRatings = movieRatings.flatMap { case (movie, ratings) =>
      val trimmedMovie = movie.trim
      ratings.zipWithIndex.collect {
        case (rating, idx) if rating.nonEmpty =>
          (idx, (trimmedMovie, rating))
      }
    }

    // Group all ratings for each user together
    val userGrouped = userMovieRatings.groupByKey()

    // For each user, generate all pairs of movies they rated the same
    val moviePairs = userGrouped.flatMap { case (_, movieRatingIterable) =>
      val arr = movieRatingIterable.toArray
      for {
        i <- arr.indices
        j <- (i + 1) until arr.length
        (movieA, ratingA) = arr(i)
        (movieB, ratingB) = arr(j)
        val trimmedA = movieA.trim
        val trimmedB = movieB.trim
        if trimmedA < trimmedB && ratingA == ratingB
      } yield ((trimmedA, trimmedB), 1)
    }

    // Use reduceByKey to sum up similarities
    val similarityCounts = moviePairs.reduceByKey(_ + _)

    // Get all movie names (for zero similarity pairs)
    val allMovies = movieRatings.map(_._1.trim).distinct().collect().sorted

    // Generate all possible pairs (lex order)
    val allPairs = sc.parallelize(
      for {
        i <- allMovies.indices
        j <- (i + 1) until allMovies.length
      } yield (allMovies(i), allMovies(j))
    )

    // Join with similarity counts, fill in zeros where needed
    val similarityMap = similarityCounts.map { case ((a, b), count) => ((a.trim, b.trim), count) }
    val result = allPairs
      .map(pair => (pair, 0))
      .leftOuterJoin(similarityMap)
      .map { case ((a, b), (zero, optCount)) => s"$a,$b,${optCount.getOrElse(0)}" }

    result.saveAsTextFile(args(1))
  }
}
