import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task4 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // Helper normalization function
    def normalize(s: String): String = s.replaceAll("\\s+", " ").trim

    // First, determine the maximum number of columns (users) in the file
    val maxUsers = textFile.map { line =>
      line.split(",", -1).length - 1
    }.reduce(math.max)

    // Parse each line into (movie, Array[rating]), padding to maxUsers
    val movieRatings = textFile.map { line =>
      val parts = line.split(",", -1)
      val movie = normalize(parts(0))
      val ratings = parts.drop(1).map(_.trim).padTo(maxUsers, "")
      (movie, ratings)
    }

    // For each user, emit (userIndex, (movie, rating))
    val userMovieRatings = movieRatings.flatMap { case (movie, ratings) =>
      val normMovie = normalize(movie)
      ratings.zipWithIndex.collect {
        case (rating, idx) if rating.nonEmpty =>
          (idx, (normMovie, rating.trim))
      }
    }

    // Group all ratings for each user together
    val userGrouped = userMovieRatings.groupByKey()

    // For each user, generate all pairs of movies they rated, and count if ratings are equal
    val moviePairs = userGrouped.flatMap { case (_, movieRatingIterable) =>
      val arr = movieRatingIterable.toArray.distinct // (movie, rating) pairs, deduped
      for {
        i <- arr.indices
        j <- (i + 1) until arr.length
        (movieA, ratingA) = arr(i)
        (movieB, ratingB) = arr(j)
        val normA = normalize(movieA)
        val normB = normalize(movieB)
        if normA < normB && ratingA == ratingB
      } yield ((normA, normB), 1)
    }

    // Use reduceByKey to sum up similarities
    val similarityCounts = moviePairs.reduceByKey(_ + _)

    // Get all movie names (for zero similarity pairs)
    val allMovies = movieRatings.map(_._1).distinct().collect().map(normalize).distinct.sorted

    // Generate all possible pairs (lex order)
    val allPairs = sc.parallelize(
      for {
        i <- allMovies.indices
        j <- (i + 1) until allMovies.length
      } yield (allMovies(i), allMovies(j))
    )

    // Join with similarity counts, fill in zeros where needed
    val similarityMap = similarityCounts.map { case ((a, b), count) => ((normalize(a), normalize(b)), count) }
    val result = allPairs
      .map(pair => (pair, 0))
      .leftOuterJoin(similarityMap)
      .map { case ((a, b), (zero, optCount)) => (normalize(a), normalize(b), optCount.getOrElse(0)) }
      .sortBy(t => (t._1, t._2)) // Sort by movieA, then movieB
      .map { case (a, b, count) => s"$a,$b,$count" }

    result.saveAsTextFile(args(1))
  }
}
