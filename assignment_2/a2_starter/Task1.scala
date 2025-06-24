import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // For each line, process to find the user(s) with the highest rating
    val output = textFile.map(line => {
      val parts = line.split(",") // Split by comma
      val movie = parts(0) // First part is the movie name
      val ratings = parts.drop(1).map(_.trim.toInt) // The rest are ratings, convert to Int
      val maxRating = ratings.max // Find the highest rating
      // Find all user indices (1-based) where the rating equals the max
      val userIndices = ratings.zipWithIndex.collect {
        case (rating, idx) if rating == maxRating => idx + 1 // +1 for 1-based index
      }
      // Combine movie name and user indices as required output
      movie + "," + userIndices.mkString(",")
    })
    
    output.saveAsTextFile(args(1))
  }
}
