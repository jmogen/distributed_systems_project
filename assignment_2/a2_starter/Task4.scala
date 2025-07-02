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
      ratings.zipWithIndex.collect {
        case (rating, idx) if rating.nonEmpty =>
          (idx, (movie, rating))
      }
    }

    // Partition by user index to parallelize pair generation
    val numPartitions = userMovieRatings.getNumPartitions
    val partitioned = userMovieRatings.partitionBy(new org.apache.spark.HashPartitioner(numPartitions))

    // Use mapPartitions to aggregate movie pairs locally per user
    val moviePairs = partitioned.mapPartitions(iter => {
      val userMap = scala.collection.mutable.Map[Int, scala.collection.mutable.ListBuffer[(String, String)]]()
      iter.foreach { case (userIdx, (movie, rating)) =>
        val list = userMap.getOrElseUpdate(userIdx, scala.collection.mutable.ListBuffer())
        list += ((movie, rating))
      }
      userMap.iterator.flatMap { case (_, movieRatingList) =>
        val arr = movieRatingList.toArray
        for {
          i <- arr.indices
          j <- (i + 1) until arr.length
          (movieA, ratingA) = arr(i)
          (movieB, ratingB) = arr(j)
          if movieA < movieB && ratingA == ratingB
        } yield ((movieA, movieB), 1)
      }
    })

    // Use reduceByKey to sum up similarities
    val similarityCounts = moviePairs
      .reduceByKey(_ + _)
      .map { case ((a, b), count) => s"$a,$b,$count" }

    similarityCounts.saveAsTextFile(args(1))
  }
}
