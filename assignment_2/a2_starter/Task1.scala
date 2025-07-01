import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // Efficient map: no extra collections, no shuffles, no wide dependencies
    val output = textFile.map { line =>
      val parts = line.split(",", -1) // -1 keeps trailing empty columns
      val movie = parts(0)
      // Only parse ratings that are non-empty, keep their indices
      var maxRating = Int.MinValue
      val ratings = new Array[Int](parts.length - 1)
      for (i <- 1 until parts.length) {
        if (parts(i).nonEmpty) {
          val rating = parts(i).trim.toInt
          ratings(i - 1) = rating
          if (rating > maxRating) maxRating = rating
        } else {
          ratings(i - 1) = Int.MinValue // Mark as missing
        }
      }
      // Find all user indices with the max rating
      val userIndices = for (i <- ratings.indices if ratings(i) == maxRating) yield (i + 1)
      movie + "," + userIndices.mkString(",")
    }
    
    output.saveAsTextFile(args(1))
  }
}
