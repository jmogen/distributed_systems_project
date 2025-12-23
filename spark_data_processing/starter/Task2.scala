/**
 * Spark Data Processing Task 2
 * Advanced RDD operations and aggregations
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

// please don't change the object name
object Task2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 2")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // For each line, count the number of non-blank ratings (ignore the movie name)
    val ratingCounts = textFile.map(line => {
      val parts = line.split(",")
      // Drop the movie name, count non-empty ratings
      parts.drop(1).count(_.trim.nonEmpty)
    })

    // Sum up all counts to get the total number of ratings
    val totalRatings = ratingCounts.reduce(_ + _)

    // Save the result as a single line in a single file
    sc.parallelize(Seq(totalRatings)).saveAsTextFile(args(1))
  }
}
