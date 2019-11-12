package My_KMeans

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import Model._
import org.apache.commons.math3.genetics.StoppingCondition

class My_KMeans(masterURL: String, points: RDD[Point], epsilon: Double, iterations: Int) extends Serializable {

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

    def stopCondIterations(centroids: Array[Point], newCentroids: Array[Point], it: Int): Boolean = {
      // Calculate the centroid movement for the stopping condition
      val movement = (centroids zip newCentroids).map({ case (a, b) => a distance b })
      printInfo(movement, it)
      // Iterate if iterations is lower than the threshold
      if (it < iterations-1) true
      else false
    }

    def stopCondVariance(centroids: Array[Point], newCentroids: Array[Point], it: Int): Boolean = {
      // Calculate the centroid movement for the stopping condition
      val movement = (centroids zip newCentroids).map({ case (a, b) => a distance b })
      printInfo(movement, it)
      // Iterate if movement exceeds threshold
      if (movement.exists(_ > epsilon)) true
      else false
    }

    def printInfo(movement: Array[Double], it: Int) = {
      System.err.println("Iteration: " + it + "\t" + "Centroids changed by\t" + movement.map(d => "%3f".format(d)).mkString("(", ", ", ")"))
    }

    //def fixedCentroidsEurope(k: Int) = {
    //  //val Europe: Array[Point] = [Point(1, 2)]
    //  val Europe: Array[(String, String)] = Array(("a","b"), ("c","d"))
    //}
  }

  def clusterize(clusterNumbers: Int, startCentroids: Array[Point],
                 stopCondition: (Array[Point], Array[Point], Int) => Boolean) = {
    // Use the given centroids, or initialize k random ones
    val centroids = (
      if (startCentroids.length == clusterNumbers)
        startCentroids
      else
        Array.fill(clusterNumbers) { Point.random }
      )

    kmeans(centroids, 0, stopCondition)
    // Array[Point]

  }

  def kmeans(centroids: Array[Point], it: Int, stopF: (Array[Point], Array[Point], Int) => Boolean): Array[Point] = {

    // associate each point with his closest centroids, than for each cluster calculate the average
    val clusters =
      (
        this.points
          // RDD[Point]
          .map(point => KMeansHelper.closestCentroid(centroids, point) -> (point, 1))
          // RDD[Point, (Point, Int)] // RDD[ClosestCentroid, (Point, 1)]
          .reduceByKeyLocally({
            case ((ptA, numA), (ptB, numB)) => (ptA + ptB, numA + numB)
          })
          // Map[Point, (Point, Int)]
          .map({
            case (centroid, (ptSum, numPts)) => centroid -> ptSum / numPts
          })
          // Map[Point, Point]
      )

    // Recalculate centroids
    val newCentroids = centroids
      .map(oldCentroid => {
        clusters.get(oldCentroid) match {
          case Some(newCentroid) => newCentroid
          case None => oldCentroid
        }
      })
    // Array[Point]

    if (stopF(centroids, newCentroids, it)) kmeans(newCentroids, it+1, stopF)
    else newCentroids

    //// Calculate the centroid movement for the stopping condition
    //val movement = (centroids zip newCentroids).map({ case (a, b) => a distance b })

    //System.err.println("Centroids changed by\n" +
    //  "\t   " + movement.map(d => "%3f".format(d)).mkString("(", ", ", ")") + "\n" +
    //  "\tto " + newCentroids.mkString("(", ", ", ")"))

    //// Iterate if movement exceeds threshold
    //if (movement.exists(_ > epsilon))
    //  kmeans(newCentroids, epsilon)
    //else
    //  newCentroids
  }

}
