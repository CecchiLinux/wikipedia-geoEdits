package Utility

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import Model._

class DataLoader {

  // case class Location(longIp: Long, countryCode: String, countryName: String, regionName: String, city: String, latitude: String , longitude: String)

  def loadLocations(path: String, sc: SparkContext): RDD[(Long, String)] = {
    val locationsTextRDD = sc.textFile(path)
    locationsTextRDD.map {
      line =>
        val col = line.replaceAll("\"", "").split(",")
        val keyIp = col(0).replaceAll("[^0-9.]", "").toLong
        val location = col(0) + "," + col(2) + "," + col(3) + "," + col(4) + "," + col(5) + "," + col(6) + "," + col(7)
        (keyIp, location)
    }
  }

  def loadEdits(path: String, sc: SparkContext): RDD[(String, Edit)] = {
    val editsTextRDD = sc.textFile(path)
    editsTextRDD.map {
      line =>
        val col = line.split("\t")
        val key = col(1).trim()
        val longIp = if (!col(8).trim().isEmpty && col(8).trim().forall(_.isDigit)) col(8).trim().toLong else 0.toLong
        val edit = Edit(
          artId=col(0),
          revId=col(1),
          artName=col(2),
          datetime=col(3),
          ip=col(4),
          category=col(5),
          relPages=col(6),
          entity=col(7),
          longIp=longIp)
        (key, edit)
    }
  }
}
