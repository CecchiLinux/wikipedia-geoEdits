import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.io.compress.{BZip2Codec}
import org.apache.spark.broadcast.Broadcast
import MyConf.Conf
import Utility.DataLoader
import Model._
import javax.imageio.ImageIO
import java.awt.{Color, Graphics, RenderingHints}
import java.awt.image.BufferedImage
import java.io.File

import scala.collection.mutable
import java.util.Locale
import java.text.NumberFormat

object Main extends App {

  private def toImageCoordinates(longitude: Double, latitude: Double, imageWidth: Int, imageHeight: Int): (Int, Int) = {
    (
      (imageWidth * (0.5 + longitude / 360)).toInt,
      (imageHeight * (0.5 - latitude / 180)).toInt
    )
  }

  private def generateColor(group: Int, groupCount: Int): Color = {
    val hue = group.toFloat / groupCount
    val saturation = 0.8f
    val lightness = 0.5f
    Color.getHSBColor(hue, saturation, lightness)
  }

  private def drawMapBackground(graphics: Graphics, backgroundImageFileName: String, imageHeight: Int, imageWidth: Int) {
    val mapBackground = ImageIO.read(Main.getClass.getClassLoader.getResourceAsStream(backgroundImageFileName))
    graphics.drawImage(mapBackground, 0, 0, imageWidth, imageHeight, Color.WHITE, null)
    graphics.setColor(new Color(0, 0, 0, 100))
    graphics.fillRect(0, 0, imageWidth, imageHeight)
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
    mySparkConf.setMaster(masterURL)

    val sc = SparkSession
      .builder()
      .config(mySparkConf)
      .getOrCreate().sparkContext
    //sc.setLogLevel("ERROR")
    val loader = new DataLoader()

    var inFileEditsPath = conf.editsPath.apply().getAbsolutePath
    val inFileLocationsPath = conf.ip2locationPath.apply().getAbsolutePath

    if (conf.associateLocation.apply()) {
      //-a -i /home/enrico/datasets/IP2LOCATION-LITE-DB9.CSV.bz2 -e /home/enrico/datasets/enwiki-longIp.bz2 -o /home/enrico/datasets/outputTest
      var outFile = conf.outLocationsFile.apply().getAbsolutePath
      associateIps(loader, inFileEditsPath, inFileLocationsPath, outFile, sc)

      inFileEditsPath = outFile
      //outFile = "/home/enrico/datasets/catIpsFinal"
      outFile = "/home/enrico/datasets/catIpsFinalTest"
      groupCategories(loader, inFileEditsPath, outFile, sc)

    } else {

      // ===================== load files
      val locationsRDD = loader.loadLocations(inFileLocationsPath, separator = "\",\"", sc)
      //val editsRDD = loader.loadEditsWithClass(inFileEditsPath + "/part-00[0-5]*", separator='|', sc)

      // ===================== broadcast array through clusters
      val locationsMap = sc.broadcast(locationsRDD.collect.toMap)




     // ////val reg = ".*_war(s)?_.*".r
     // ////editsRDDCatIps.map( line => line.toLowerCase)
     // ////  .filter( line => reg.pattern.matcher(line).matches )
     // ////  .coalesce(1, true).saveAsTextFile("/home/enrico/datasets/catIpsWar.txt", classOf[GzipCodec])





      //val editsRDDCatIps = sc.objectFile[(String, List)]("/home/enrico/datasets/catIpsWWII")

      //editsRDDCatIps.keys.foreach(println)

    }

  }

  def filterCategories () = {

    // val reg = ".*_world_war_ii_.*".r
    // editsRDDCatIps.map(line => line.toLowerCase)
    //   .filter(line => reg.pattern.matcher(line).matches)
    //   .coalesce(1, true).saveAsObjectFile("/home/enrico/datasets/catIpsWWII")
  }

  def associateIps (loader: DataLoader, inFileEditsPath: String, inFileLocationsPath: String, outFile: String, sc: SparkContext) = {

    //// ===================== load files
    val editsRDD = loader.loadEdits(inFileEditsPath, separator = '|', sc)
    val locationsRDD = loader.loadLocations(inFileLocationsPath, separator = "\",\"", sc)
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

  def groupCategories (loader: DataLoader, inFileEditsPath: String, outFile: String, sc: SparkContext) = {

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

  def categoriesCounter(loader: DataLoader, inFileEditsPath: String, outFile: String, sc: SparkContext) = {

    // ===================== categories count
    val editsRDD = loader.loadEditsWithClass(inFileEditsPath + "/part-00[0-5]*", separator='|', sc)
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
