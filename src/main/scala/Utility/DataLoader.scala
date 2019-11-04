package Utility

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import Model._
import org.apache.spark.sql.SparkSession

object DataLoader {

  def mySparkConf = new SparkConf()
  mySparkConf.setAppName("WikipediaEdits")
  mySparkConf.set("spark-serializer", "org.apache.spark.serializer.KryoSerializer")
  mySparkConf.set("spark.kryoserializer.buffer.max", "2047")
  mySparkConf.registerKryoClasses(Array(classOf[Point]))

  val sc = SparkSession
    .builder()
    .config(mySparkConf)
    .getOrCreate().sparkContext


  def loadEdits(path: String, separator: Char): RDD[(String, Edit)] = {
    object editColumns extends Enumeration {
      val artId, revId, artName, ip, categories, entity, longIp = Value
    }

    val editsTextRDD = sc.textFile(path)
    editsTextRDD.map {
      line =>
        val col = line.split(separator)
        val key = col(1).trim()
        val longIp = if (!col(6).trim().isEmpty &&
          col(6).trim().forall(_.isDigit)) col(6).trim().toLong else 0.toLong
        val edit = Edit(
          artId = col(0),
          revId = col(1),
          artName = col(2),
          ip = col(3),
          categories = col(4),
          entity = col(5),
          longIp = longIp)
        (key, edit)
    }
  }
  // case class Location(longIp: Long, countryCode: String, countryName: String, regionName: String, city: String, latitude: String , longitude: String)

  def loadLocations(path: String, separator: String): RDD[(Long, String)] = {
    object locationColumns extends Enumeration {
      val classIp, countryCode, countryName, regionName, city, latitude, longitude = Value
    }

    val locationsTextRDD = sc.textFile(path)
    val s = "|"
    locationsTextRDD.map {
      line =>
        val col = line.split(separator) // remove the '"'
        val keyIp = col(0).replaceAll("[^0-9.]", "").toLong
        val location = col(0) + s + col(2) + s +
          col(3) + s + col(4) + s + col(5) + s +
          col(6) + s + col(7)
        (keyIp, location)
    }
  }

  def loadEditsWithClass(path: String, separator: Char): RDD[(String, Edit)] = {
    object editColumns extends Enumeration {
      val artId, revId, artName, ip, categories, entity, longIp = Value
    }

    val editsTextRDD = sc.textFile(path)
    editsTextRDD.map {
      line =>
        val col = line.split(separator)
        val key = col(1).trim()
        val longIp = if (!col(7).trim().isEmpty &&
          col(7).trim().forall(_.isDigit)) col(7).trim().toLong else 0.toLong
        val edit = Edit(
          artId = col(0),
          revId = col(1),
          artName = col(2),
          ip = col(3),
          categories = col(4),
          entity = col(5),
          longIp = longIp)
        (key, edit)
    }
  }

  def loadEditsWithLoc(path: String, separator: Char): RDD[(String, EditWithLoc)] = {
    val editsTextRDD = sc.textFile(path)
    editsTextRDD.map {
      line =>
        val col = line.split(separator)
        val key = col(1).trim()
        val longIp = if (!col(8).trim().isEmpty && col(8).trim().forall(_.isDigit)) col(8).trim().toLong else 0.toLong
        val edit = EditWithLoc(
          artId = col(0),
          revId = col(1),
          artName = col(2),
          category = col(5),
          entity = col(7),
          longIp = longIp,
          countryCode = col(9),
          countryName = col(10),
          regionName = col(11),
          city = col(12),
          latitude = col(13),
          longitude = col(14)
        )
        (key, edit)
    }
  }

  def loadPoints(path: String, separator: Char): RDD[(String, List[Long])] = {
    val categoriesTextRdd = sc.textFile(path)
    // RDD[ String ]
    categoriesTextRdd.map( line => line.split('#') )
      // RDD[ Array[ String ]
      .flatMap( arr => {
        val category = arr(0)
        val ipsString = arr(1)
        val ips = ipsString.split('|')
        ips.map(ip => (category, ip.toLong))
      })
      // RDD[(String, Long)]
      .map( x => (x._1, List(x._2)) )
      .reduceByKey(_ ::: _)
  }

  //def loadPoints(path: String, separator: Char) = {
  //  val categoriesTextRdd = sc.textFile(path)
  //  // RDD[ String ]
  //  categoriesTextRdd.map( line => line.split('#') )
  //    // RDD[ Array[ String ]
  //    .flatMap( arr => {
  //      val ipsString = arr(1)
  //      val ips = ipsString.split('|')
  //      ips.map(ip => ip.toLong)
  //      // RDD[ Long ]
  //    })
  //}
}
