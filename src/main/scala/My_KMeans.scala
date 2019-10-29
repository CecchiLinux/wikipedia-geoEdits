package My_KMeans

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

  def sc = SparkSession
    .builder()
    .config(mySparkConf)
    .getOrCreate().sparkContext


  object KMeansHelper extends Serializable {
    /**
     * Finds the closest centroid to the given point.
     */
    def closestCentroid(centroids: Array[Point], point: Point) = {
      centroids.reduceLeft((a, b) => if ((point distance a) < (point distance b)) a else b)
    }
  }


  def clusterize(clusterNumbers: Int, startCentroids: Array[Point], epsilon: Double) = {

    // Use the given centroids, or initialize k random ones
    val centroids = (
      if (startCentroids.length == clusterNumbers)
        startCentroids
      else
        Array.fill(clusterNumbers) { Point.random }
      )

    // Start the Spark run
    val resultCentroids = kmeans(centroids, epsilon)

    System.err.println("Final centroids: ")
    println(resultCentroids.map(centroid => "%3f,%3f\n".format(centroid.x, centroid.y)).mkString)
    resultCentroids

  }

  def kmeans(centroids: Array[Point], epsilon: Double): Array[Point] = {
    // Assign points into clusters based on their closest centroids,
    // and take the average of all points in each cluster
    val clusters =
      (
        this.points
          // RDD[Point]
          .map(point => KMeansHelper.closestCentroid(centroids, point) -> (point, 1))
          // RDD[Point, (Point, Int)] // RDD[ClosestCentroid, (Point, 1)]
          //.reduceByKeyToDriver({
          .reduceByKeyLocally({
            case ((ptA, numA), (ptB, numB)) => (ptA + ptB, numA + numB)
          })
          // Map[Point, (Point, Int)]
          .map({
            case (centroid, (ptSum, numPts)) => centroid -> ptSum / numPts
          })
          // Map[Point, Point]
      )

    // OK
    //val clus = this.points
    //  // RDD[Point]
    //  .map(point => KMeansHelper.closestCentroid(centroids, point) -> (point, 1))
    //clus.saveAsTextFile("/home/enrico/datasets/clusters")

    val clusters_debug = this.points
      // RDD[Point]
      .map(point => KMeansHelper.closestCentroid(centroids, point) -> (point, 1))
      // RDD[Point, (Point, Int)] // RDD[ClosestCentroid, (Point, 1)]
      .reduceByKeyLocally({
        case ((ptA, numA), (ptB, numB)) => (ptA + ptB, numA + numB)
      })
      // Map[Point, (Point, Int)]
        .map(c => c._1).foreach(println)
    println()

    //    clusters.keys.map(c => clusters.get(c)).foreach(println)
    //clusters.keys.foreach(println)
    //println()
    //centroids.map(c => (c)).foreach(println)
    //println()
    //centroids.map(c => clusters.get(c)).foreach(println)

    //clusters_debug.keys.foreach(println)
    //centroids.map(c => clusters.get(c)).foreach(println)

    // Recalculate centroids based on their clusters
    // (or leave them alone if they don't have any points in their cluster)
    val newCentroids = centroids.map(oldCentroid => {
      clusters.get(oldCentroid) match {
        case Some(newCentroid) => newCentroid
        case None => oldCentroid
      }
    })

    //centroids.foreach(println)
    //newCentroids.foreach(println)

    // Calculate the centroid movement for the stopping condition
    val movement = (centroids zip newCentroids).map({ case (a, b) => a distance b })

    System.err.println("Centroids changed by\n" +
      "\t   " + movement.map(d => "%3f".format(d)).mkString("(", ", ", ")") + "\n" +
      "\tto " + newCentroids.mkString("(", ", ", ")"))

    // Iterate if movement exceeds threshold
    if (movement.exists(_ > epsilon))
      kmeans(newCentroids, epsilon)
    else
      newCentroids
  }

}
