import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import Model._

class My_KMeans(masterURL: String, points: RDD[Point]) extends Serializable {

  points.persist(StorageLevel.MEMORY_AND_DISK)
  System.err.println("Read " + points.count() + " points.")

  def mySparkConf = new SparkConf()
  mySparkConf.setAppName("WikipediaEdits")
  mySparkConf.setMaster(masterURL)
  mySparkConf.set("spark-serializer", "org.apache.spark.serializer.KryoSerializer")
  mySparkConf.set("spark.kryoserializer.buffer.max", "2047")
  mySparkConf.registerKryoClasses(Array(classOf[Point]))

  val sc = SparkSession
    .builder()
    .config(mySparkConf)
    .getOrCreate().sparkContext


  object KMeansHelper {
    /**
     * Finds the closest centroid to the given point.
     */
    def closestCentroid(centroids: Seq[Point], point: Point) = {
      centroids.reduceLeft((a, b) => if ((point distance a) < (point distance b)) a else b)
    }
  }


  def clusterize(clusterNumbers: Int, startCentroids: Array[Point], epsilon: Double, sc: SparkContext) = {

    // Use the given centroids, or initialize k random ones
    val centroids = (
      if (startCentroids.length == clusterNumbers)
        startCentroids
      else
        Array.fill(clusterNumbers) { Point.random }
      )

    // Start the Spark run
    val resultCentroids = kmeans(centroids, 0.1, sc)

    System.err.println("Final centroids: ")
    println(resultCentroids.map(centroid => "%3f\t%3f\n".format(centroid.x, centroid.y)).mkString)

  }

  def kmeans(centroids: Seq[Point], epsilon: Double, sc: SparkContext): Seq[Point] = {
    // Assign points into clusters based on their closest centroids,
    // and take the average of all points in each cluster
    val clusters =
      (
        this.points
          .map(point => KMeansHelper.closestCentroid(centroids, point) -> (point, 1))
          //.reduceByKeyToDriver({
          .reduceByKeyLocally({
            case ((ptA, numA), (ptB, numB)) => (ptA + ptB, numA + numB)
          })
          .map({
            case (centroid, (ptSum, numPts)) => centroid -> ptSum / numPts
          })
      )

    // Recalculate centroids based on their clusters
    // (or leave them alone if they don't have any points in their cluster)
    val newCentroids = centroids.map(oldCentroid => {
      clusters.get(oldCentroid) match {
        case Some(newCentroid) => newCentroid
        case None => oldCentroid
      }
    })

    // Calculate the centroid movement for the stopping condition
    val movement = (centroids zip newCentroids).map({ case (a, b) => a distance b })

    System.err.println("Centroids changed by\n" +
      "\t   " + movement.map(d => "%3f".format(d)).mkString("(", ", ", ")") + "\n" +
      "\tto " + newCentroids.mkString("(", ", ", ")"))

    // Iterate if movement exceeds threshold
    if (movement.exists(_ > epsilon))
      kmeans(newCentroids, epsilon, sc)
    else
      newCentroids
  }

}
