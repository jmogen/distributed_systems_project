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
    val movieRatings = textFile.filter(_.trim.nonEmpty).flatMap { line =>
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
    }.collect()

    // Deduplicate and strictly normalize movie names
    val allMovies = movieRatings.map(_._1).map(normalize).distinct.sorted

    // For each user, collect their (movie, rating) pairs
    val userMovieRatings = Array.ofDim[(String, String)](maxUsers, movieRatings.length)
    for ((mr, movieIdx) <- movieRatings.zipWithIndex) {
      val (movie, ratings) = mr
      for (userIdx <- 0 until maxUsers) {
        userMovieRatings(userIdx)(movieIdx) = (normalize(movie), normalize(ratings(userIdx)))
      }
    }

    // For each pair of movies, count number of users who gave the same non-blank rating to both
    import scala.collection.mutable
    val similarity = mutable.Map.empty[(String, String), Int].withDefaultValue(0)

    for (userIdx <- 0 until maxUsers) {
      // For this user, get all (movie, rating) pairs with non-blank rating, deduped by movie
      val rated = userMovieRatings(userIdx).filter{ case (m, r) => m.nonEmpty && r.nonEmpty }.groupBy(_._1).map(_._2.head).toArray
      // For all pairs (lex order)
      for {
        i <- rated.indices
        j <- (i + 1) until rated.length
        (movieA, ratingA) = rated(i)
        (movieB, ratingB) = rated(j)
        val normA = normalize(movieA)
        val normB = normalize(movieB)
        if normA < normB && ratingA == ratingB
      } {
        similarity((normA, normB)) += 1
      }
    }

    // Output all possible pairs (lex order), with similarity or 0, strictly normalized
    val result = for {
      i <- allMovies.indices
      j <- (i + 1) until allMovies.length
      movieA = normalize(allMovies(i))
      movieB = normalize(allMovies(j))
      count = similarity.getOrElse((movieA, movieB), 0)
    } yield s"$movieA,$movieB,$count"

    sc.parallelize(result.toSeq).saveAsTextFile(args(1))
  }
}
