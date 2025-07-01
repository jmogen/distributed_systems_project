import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Task 1")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val numPartitions = if (args.length > 2) args(2).toInt else sc.defaultParallelism
    val textFile = sc.textFile(args(0), numPartitions)

    // Efficient map: no extra collections, no shuffles, no wide dependencies
    val output = textFile.map { line =>
      val parts = line.split(",", -1) // -1 keeps trailing empty columns
      val movie = parts(0)
      // Only parse ratings that are non-empty, keep their indices
      var maxRating = Int.MinValue
      val ratings = new Array[Int](parts.length - 1)
      var i = 1
      while (i < parts.length) {
        if (parts(i).nonEmpty) {
          val rating = parts(i).trim.toInt
          ratings(i - 1) = rating
          if (rating > maxRating) maxRating = rating
        } else {
          ratings(i - 1) = Int.MinValue // Mark as missing
        }
        i += 1
      }
      // Find all user indices with the max rating
      val userIndices = new StringBuilder
      var first = true
      i = 0
      while (i < ratings.length) {
        if (ratings(i) == maxRating) {
          if (!first) userIndices.append(",")
          userIndices.append(i + 1)
          first = false
        }
        i += 1
      }
      movie + "," + userIndices.toString
    }
    
    output.saveAsTextFile(args(1))
  }
}
