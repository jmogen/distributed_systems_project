import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task4 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // Helper normalization function
    def normalize(s: String): String = Option(s).getOrElse("").replaceAll("\\s+", " ").trim

    // First, determine the maximum number of columns (users) in the file (skip blank/malformed lines)
    val maxUsers = textFile.filter(_.trim.nonEmpty).map { line =>
      val parts = line.split(",", -1)
      if (parts.length > 1) parts.length - 1 else 0
    }.reduce(math.max)

    // Parse each line into (movie, Array[rating]), padding to maxUsers, skip blank/malformed lines
    val movieRatingsRDD = textFile.filter(_.trim.nonEmpty).flatMap { line =>
      val parts = line.split(",", -1)
      if (parts.length < 2) None
      else {
        val movie = normalize(parts(0))
        if (movie.isEmpty) None
        else {
          val ratings = parts.drop(1).map(x => normalize(x)).padTo(maxUsers, "")
          Some((movie, ratings))
        }
      }
    }.cache()

    // Get all unique movies, normalized and sorted
    val allMovies = movieRatingsRDD.map(_._1).distinct().map(normalize).collect().sorted

    // For each user, get (userIdx, (movie, rating)) for non-blank ratings
    val userMovieRatings = movieRatingsRDD.flatMap { case (movie, ratings) =>
      ratings.zipWithIndex.collect { case (rating, userIdx) if rating.nonEmpty => (userIdx, (normalize(movie), normalize(rating))) }
    }

    // For each user, group their (movie, rating) pairs, dedupe by movie
    val userGrouped = userMovieRatings.groupByKey()

    // For each user, for each pair of movies (lex order), if same non-blank rating, emit ((movieA, movieB), 1)
    val pairCounts = userGrouped.flatMap { case (userIdx, movieRatings) =>
      val deduped = movieRatings.groupBy(_._1).map(_._2.head).toArray
      val pairs = for {
        i <- deduped.indices
        j <- (i + 1) until deduped.length
        (movieA, ratingA) = deduped(i)
        (movieB, ratingB) = deduped(j)
        val normA = normalize(movieA)
        val normB = normalize(movieB)
        if normA < normB && ratingA == ratingB && ratingA.nonEmpty
      } yield ((normA, normB), 1)
      pairs
    }

    // Sum up counts for each pair
    val similarity = pairCounts.reduceByKey(_ + _)

    // Prepare all possible pairs (lex order)
    val allPairs = sc.parallelize(for {
      i <- allMovies.indices
      j <- (i + 1) until allMovies.length
      movieA = allMovies(i)
      movieB = allMovies(j)
    } yield (movieA, movieB))

    // Join with similarity counts, fill missing with 0
    val similarityMap = similarity.map { case ((a, b), count) => ((a, b), count) }
    val result = allPairs.leftOuterJoin(similarityMap).map {
      case ((a, b), (_, Some(count))) => s"$a,$b,$count"
      case ((a, b), (_, None))        => s"$a,$b,0"
    }

    // Sort lexicographically
    val sortedResult = result.sortBy(identity)
    sortedResult.saveAsTextFile(args(1))
  }
}
