import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.broadcast.Broadcast
import javax.imageio.ImageIO
import java.awt.{Color, Graphics, RenderingHints}
import java.awt.image.BufferedImage
import java.io.File
import java.util.Calendar

import MyConf.Conf
import Utility.{DataLoader, WorldMap}
import Model._
import My_KMeans._

object Main extends App {

  object locationColumns extends Enumeration {
    val classIp, countryCode, countryName, regionName, city, latitude, longitude = Value
  }

  implicit class CSVWrapper(val prod: Product) extends AnyVal {
    def toCSV(): String = prod.productIterator.map {
      case Some(value) => value
      case None => ""
      case rest => rest
    }.mkString("|")
  }

  def checker[T <: Long](target: T, ips: Array[T]): T = {
    def search(start: Int = 0, end: Int = ips.length - 1): T = {
      val mid = start + (end - start) / 2
      if (start > end) ips(start - 1)
      else if (ips(mid) == target) ips(mid)
      else if (ips(mid) > target) search(start, mid - 1)
      else search(mid + 1, end)
    }

    search()
  }

  override def main(args: Array[String]): Unit = {

    // ===================== conf
    val conf = new Conf(args)
    val mySparkConf = new SparkConf()
    val masterURL = conf.masterURL.apply()
    mySparkConf.setAppName("WikipediaEdits")
    //mySparkConf.set("spark.driver.allowMultipleContexts", "true")
    mySparkConf.setMaster(masterURL)
    mySparkConf.set("spark-serializer", "org.apache.spark.serializer.KryoSerializer")
    mySparkConf.set("spark.kryoserializer.buffer.max", "2047")
    mySparkConf.registerKryoClasses(Array(classOf[Point]))

    val sc = SparkSession
      .builder()
      .config(mySparkConf)
      .getOrCreate().sparkContext
    //sc.setLogLevel("ERROR")

    //var inFileEditsPath = conf.editsPath.apply().getAbsolutePath
    //val inFileLocationsPath = conf.ip2locationPath.apply().getAbsolutePath

    // ========================================================================
    // Phase 1 - from raw Edits to Categories with Ips
    // use -a option the first time to perform this phase
    // =================================================================================================================
    if (conf.associateLocation.apply()) {
      associateIps(conf.editsPath, conf.ip2LocationPath, conf.outCategoriesIps, sc)
    }

    // ========================================================================
    // Phase 2 - filter category by input words, perform the k-means, print sub categories in the map
    // =================================================================================================================

    // ===================== load files
    val locationsRDD = DataLoader.loadLocations(conf.ip2LocationPath, separator = "\",\"")
    //val editsRDD = DataLoader.loadEditsWithClass(inFileEditsPath + "/part-00[0-5]*", separator='|')
    // ===================== broadcast array through clusters
    val locationsMap = sc.broadcast(locationsRDD.collect.toMap)

    // ////val reg = ".*_war(s)?_.*".r
    //val catFilter = "war"
    //val catFilter = conf.words.apply().split(' ')
    val catFilter = conf.words.apply()
    val outFileName = conf.mainFolder.apply().toPath + "/filters-" + catFilter.mkString("_")
    //filterCategories(catFilter, conf.outCategoriesIps, sc)
    //  // RDD[String, String]
    //  .map(x => s"${x._1}#${x._2}")
    //  .coalesce(1, true).saveAsTextFile(outFileName)


    // K-Means
    val points = DataLoader.loadPoints(outFileName + "/part-00[0-5]*", '|')
      .map(ip => {
        val locationString = locationsMap.value(ip)
        val location = locationString.split('|')
        val latitude = location(locationColumns.latitude.id)
        val longitude = location(locationColumns.longitude.id)
        new Point(longitude.toDouble, latitude.toDouble)
      }
      )

    System.err.println("Number of points: " + points.collect.length + "\n")

    //val centroids = Array.fill(10) { Point.random }
    val centroids = points.takeSample(withReplacement = false, conf.k.apply())
    System.err.println(centroids.mkString("(", ", ", ")"))

    val mkmeans = new My_KMeans(masterURL, points)
    // Start the Spark run
    val resultCentroids = mkmeans.clusterize(conf.k.apply(), centroids, conf.epsilon.apply())

    val groups = points
      // RDD[Point]
      .map(point => (mkmeans.KMeansHelper.closestCentroid(resultCentroids, point), List(point)))
      // RDD[(Point, List[Point])]
      .reduceByKey(_ ::: _)
      // RDD[(Point, List[Point])]
      .map {
        case (point, points) => (resultCentroids.indexOf(point), points)
      }
      // RDD[(Int, List[Points])]
      .collect()
      // Array[(Int, List[Point])]

    // create image
    val image = new BufferedImage(conf.imageWidth, conf.imageHeight, BufferedImage.TYPE_INT_ARGB)
    val graphics = image.createGraphics
    graphics.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)

    // draw map
    val imageFile = Main.getClass.getClassLoader.getResourceAsStream(conf.backgroundImageFileName)
    val imageFileBoundaries = Main.getClass.getClassLoader.getResourceAsStream(conf.foregroundBoundaries)
    val groupColors = for (group <- 0 until conf.k.apply()) yield WorldMap.generateColor(group, conf.k.apply())
    WorldMap.drawMapBackground(imageFile, graphics, conf.imageWidth, conf.imageHeight)
    WorldMap.drawIps(groups, graphics, conf.imageWidth, conf.imageHeight, groupColors)
    WorldMap.drawMapBackground(imageFileBoundaries, graphics, conf.imageWidth, conf.imageHeight)
    WorldMap.drawCentroid(resultCentroids, graphics, conf.imageWidth, conf.imageHeight, groupColors)
    WorldMap.drawIpsCounts(groups, graphics, conf.imageWidth, conf.imageHeight, groupColors)
    // write image to disk
    //ImageIO.write(image, "png", new File("mappa1.png"))
    ImageIO.write(image, conf.imageFormat, new File(conf.mainFolder.apply().getAbsolutePath + "/map" + "_" + catFilter.mkString("_") + ".png"))

  }


  def filterCategories(catFilter: List[String], inFileEditsPath: String, sc: SparkContext) = {
    // val keys = List("hi","random","test")
    val editsRDD = sc.textFile(inFileEditsPath + "/part-00[0-5]*")

    // val reg = ".*_world_war_ii_.*".r
    //val reg = ".*_" + regString + "_.*"
    editsRDD.map(line => line.split('#'))
      //firstEdits.map( line => line.split('|') )
      // RDD[ Array[ String ]
      .map(arr => {
        val category = arr(0).toLowerCase()
        val ips = arr(1)
        (category, ips)
      })
      // RDD [String, String]
      //.filter(x => reg.r.pattern.matcher(x._1).matches)
      //.filter(x => catFilter.exists(x._1.contains))
      .filter(x => catFilter.forall(f => x._1.split("_").contains(f)))

  }

  def associateIps(inFileEditsPath: String, inFileLocationsPath: String, outFile: String, sc: SparkContext) = {

    //// ===================== load files
    val editsRDD = DataLoader.loadEdits(inFileEditsPath, separator = '|')
    val locationsRDD = DataLoader.loadLocations(inFileLocationsPath, separator = "\",\"")
    //// ===================== broadcast array through clusters
    val longIps: Broadcast[Array[Long]] = sc.broadcast(locationsRDD.keys.collect.sorted)

    //// === partial run
    //val firstEdits = sc.parallelize(editsRDD.take(20))
    //val editsWithIpClass = firstEdits.mapValues {
    //  case edit: Edit => (checker(edit.longIp, longIps.value), edit)
    //}
    //// === full
    val editsWithIpClass = editsRDD.mapValues {
      case edit: Edit => (checker(edit.longIp, longIps.value), edit)
    }
      // RDD[ String, (Long, Edit) ]
      .values.flatMap(
      // RDD[ Long, Edit ]
      edit => {
        val categoriesString = edit._2.categories
        val categories = categoriesString.split(' ')
        val ipClass = edit._1
        categories.map(category => (category, ipClass))
      })
      // RDD[ ( String, Long ) ]
      .map(t => (t._1, List(t._2)))
      .reduceByKey(_ ::: _)
      //// RDD[ ( String, List[Long] ) ]
      .map(x => x._1 + "#" + x._2.mkString("|"))
      .saveAsTextFile(outFile, classOf[BZip2Codec])

    //// ===============


    longIps.destroy

  }

  // def groupCategories(inFileEditsPath: String, outFile: String, sc: SparkContext) = {

  //   //// ====================== cat - List[ips]
  //   val editsRDD = sc.textFile(inFileEditsPath + "/part-00[0-5]*")
  //   //val firstEdits = sc.parallelize(editsRDD.take(20))
  //   // RDD[ String ]
  //   //val splitEditRdd = editsRDD2.map( line => line.split('|') )
  //   editsRDD.map(line => line.split('|'))
  //     //firstEdits.map( line => line.split('|') )
  //     // RDD[ Array[ String ]
  //     .flatMap(arr => {
  //       val categoriesString = arr(4)
  //       val categories = categoriesString.split(' ')
  //       val ipClass = arr(7)
  //       categories.map(category => (category, ipClass))
  //     })
  //     //// RDD[ ( String, String ) ]
  //     .map(t => (t._1, List(t._2)))
  //     .reduceByKey(_ ::: _)
  //     //// RDD[ ( String, List[String] ) ]
  //     .map(x => x._1 + ":" + x._2.mkString("|"))
  //     .saveAsTextFile(outFile, classOf[BZip2Codec])
  // }

  def categoriesCounter(inFileEditsPath: String, outFile: String, sc: SparkContext) = {

    // ===================== categories count
    val editsRDD = DataLoader.loadEditsWithClass(inFileEditsPath + "/part-00[0-5]*", separator = '|')
    val catCounts = editsRDD.values.
      map { case (edit) => edit.categories }.
      //filter((x => x != "") ).
      flatMap(categories => categories.split(" ")).
      map(x => (x, 1)).reduceByKey(_ + _)
    val catCountsSort = catCounts.map(x => (x._2, x._1)).sortByKey()
    catCountsSort.coalesce(1, true).saveAsTextFile("/home/enrico/datasets/catCounts.txt")

  }

  // def distinct[A](list: Iterable[A]): List[A] =
  //   list.foldLeft(List[A]()) {
  //     case (acc, item) if acc.contains(item) => acc
  //     case (acc, item) => item :: acc
  //   }
  // }

}
