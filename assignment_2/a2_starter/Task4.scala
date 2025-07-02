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
    }.collect() // collect all movie ratings for local processing

    // Get all movie names (normalized, deduped, sorted)
    val allMovies = movieRatings.map(_._1).distinct.sorted

    // For each user, collect their (movie, rating) pairs
    val userMovieRatings = Array.ofDim[(String, String)](maxUsers, movieRatings.length)
    for ((mr, movieIdx) <- movieRatings.zipWithIndex) {
      val (movie, ratings) = mr
      for (userIdx <- 0 until maxUsers) {
        userMovieRatings(userIdx)(movieIdx) = (movie, ratings(userIdx))
      }
    }

    // For each pair of movies, count number of users who gave the same non-blank rating to both
    import scala.collection.mutable
    val similarity = mutable.Map.empty[(String, String), Int].withDefaultValue(0)

    for (userIdx <- 0 until maxUsers) {
      // For this user, get all (movie, rating) pairs with non-blank rating
      val rated = userMovieRatings(userIdx).filter(_._2.nonEmpty)
      // For all pairs (lex order)
      for {
        i <- rated.indices
        j <- (i + 1) until rated.length
        (movieA, ratingA) = rated(i)
        (movieB, ratingB) = rated(j)
        if movieA < movieB && ratingA == ratingB
      } {
        similarity((movieA, movieB)) += 1
      }
    }

    // Output all possible pairs (lex order), with similarity or 0
    val result = for {
      i <- allMovies.indices
      j <- (i + 1) until allMovies.length
      movieA = allMovies(i)
      movieB = allMovies(j)
      count = similarity.getOrElse((movieA, movieB), 0)
    } yield s"$movieA,$movieB,$count"

    sc.parallelize(result.toSeq).saveAsTextFile(args(1))
  }
}
