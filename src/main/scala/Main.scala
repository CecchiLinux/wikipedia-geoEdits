import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.broadcast.Broadcast
import javax.imageio.ImageIO
import java.awt.{Color, Graphics, RenderingHints}
import java.awt.image.BufferedImage
import java.io.File
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

    var inFileEditsPath = conf.editsPath.apply().getAbsolutePath
    val inFileLocationsPath = conf.ip2locationPath.apply().getAbsolutePath

    if (conf.associateLocation.apply()) {
      //-a -i /home/enrico/datasets/IP2LOCATION-LITE-DB9.CSV.bz2 -e /home/enrico/datasets/enwiki-longIp.bz2 -o /home/enrico/datasets/outputTest
      var outFile = conf.outLocationsFile.apply().getAbsolutePath
      associateIps(inFileEditsPath, inFileLocationsPath, outFile, sc)

      inFileEditsPath = outFile
      //outFile = "/home/enrico/datasets/catIpsFinal"
      outFile = "/home/enrico/datasets/catIpsFinalTest"
      groupCategories(inFileEditsPath, outFile, sc)

    } else {

      // ===================== load files
      val locationsRDD = DataLoader.loadLocations(inFileLocationsPath, separator = "\",\"")
      //val editsRDD = DataLoader.loadEditsWithClass(inFileEditsPath + "/part-00[0-5]*", separator='|')
      // ===================== broadcast array through clusters
      val locationsMap = sc.broadcast(locationsRDD.collect.toMap)

      // ////val reg = ".*_war(s)?_.*".r
      val inFile = "/home/enrico/datasets/catIpsFinal"
      val catFilter = "war"
      //filterCategories(catFilter, inFile, sc)


      val inFileFiltered = "/home/enrico/datasets/" + catFilter

      // K-Means test
      val points = DataLoader.loadPoints(inFileFiltered + "/part-00[0-5]*", '|')
        .map(ip =>
          {
            val locationString = locationsMap.value(ip)
            val location = locationString.split('|')
            val latitude = location(locationColumns.latitude.id)
            val longitude = location(locationColumns.longitude.id)
            new Point(longitude.toDouble, latitude.toDouble)
          }
        )

      System.err.println("Number of points: " + points.collect.length + "\n")

      //val centroids = Array.fill(10) { Point.random }
      val centroids = points.takeSample(withReplacement = false, 10)
      println(centroids.mkString("(", ", ", ")"))

      val mkmeans = new My_KMeans(masterURL, points)

      // Start the Spark run
      val resultCentroids = mkmeans.clusterize(10, centroids, 0.01)

      // create image
      val image = new BufferedImage(conf.imageWidth, conf.imageHeight, BufferedImage.TYPE_INT_ARGB)
      val graphics = image.createGraphics
      graphics.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)

      // draw map
      val imageFile = Main.getClass.getClassLoader.getResourceAsStream(conf.backgroundImageFileName)
      WorldMap.drawMapBackground(imageFile, graphics, conf.imageWidth, conf.imageHeight)
      WorldMap.drawCentroid(resultCentroids, graphics, conf.imageWidth, conf.imageHeight)
      WorldMap.drawIps(points.collect(), graphics, conf.imageWidth, conf.imageHeight)
      // write image to disk
      //ImageIO.write(image, "png", new File("mappa1.png"))
      ImageIO.write(image, "png", new File("/home/enrico/datasets/mappaWar.png"))

    }

  }

  def filterCategories (regString: String, inFileEditsPath: String, sc: SparkContext) = {

    val editsRDD = sc.textFile(inFileEditsPath + "/part-00[0-5]*")

    // val reg = ".*_world_war_ii_.*".r
    val reg = ".*_" + regString + "_.*"
    editsRDD.map( line => line.split(':') )
      //firstEdits.map( line => line.split('|') )
      // RDD[ Array[ String ]
      .map( arr => {
        val category = arr(0)
        val ips = arr(1)
        ( category,  ips )
      })
      .filter(x => reg.r.pattern.matcher(x._1).matches)
      .map(x => s"${x._1}:${x._2}")
      .coalesce(1, true).saveAsTextFile("/home/enrico/datasets/" + regString)

  }

  def associateIps (inFileEditsPath: String, inFileLocationsPath: String, outFile: String, sc: SparkContext) = {

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
    //// ===============

    editsWithIpClass.values.map(x => s"${x._2.toCSV()}|${x._1}").
      saveAsTextFile(outFile, classOf[BZip2Codec])

    longIps.destroy

  }

  def groupCategories (inFileEditsPath: String, outFile: String, sc: SparkContext) = {

    //// ====================== cat - List[ips]
    val editsRDD = sc.textFile(inFileEditsPath + "/part-00[0-5]*")
    //val firstEdits = sc.parallelize(editsRDD.take(20))
    // RDD[ String ]
    //val splitEditRdd = editsRDD2.map( line => line.split('|') )
    editsRDD.map( line => line.split('|') )
      //firstEdits.map( line => line.split('|') )
      // RDD[ Array[ String ]
      .flatMap( arr => {
        val categoriesString = arr(4)
        val categories = categoriesString.split(' ')
        val ipClass = arr(7)
        categories.map( category => ( category,  ipClass ) )
      })
      //// RDD[ ( String, String ) ]
      .map(t => (t._1, List(t._2)))
      .reduceByKey(_:::_)
      //// RDD[ ( String, List[String] ) ]
      .map(x => x._1 + ":" + x._2.mkString("|"))
      .saveAsTextFile(outFile, classOf[BZip2Codec])
  }

  def categoriesCounter(inFileEditsPath: String, outFile: String, sc: SparkContext) = {

    // ===================== categories count
    val editsRDD = DataLoader.loadEditsWithClass(inFileEditsPath + "/part-00[0-5]*", separator='|')
    val catCounts = editsRDD.values.
      map{ case (edit) => edit.categories }.
      //filter((x => x != "") ).
      flatMap( categories => categories.split(" ") ).
      map( x => (x, 1) ).reduceByKey(_+_)
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
