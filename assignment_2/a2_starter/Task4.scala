import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task4 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val numPartitions = 128 // For 16 machines x 4 cores = 64 cores, 128 partitions is a good default
    val textFile = sc.textFile(args(0), numPartitions)

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

    val partitioned = userMovieRatings.partitionBy(new org.apache.spark.HashPartitioner(numPartitions))

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

    val similarityCounts = moviePairs
      .reduceByKey(_ + _)
      .map { case ((a, b), count) => s"$a,$b,$count" }

    similarityCounts.saveAsTextFile(args(1))
  }
}
