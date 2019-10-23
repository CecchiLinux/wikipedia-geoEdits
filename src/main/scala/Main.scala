import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.broadcast.Broadcast
import MyConf.Conf
import Utility.DataLoader
import Model._

object Main {

  implicit class CSVWrapper(val prod: Product) extends AnyVal {
    def toCSV(): String = prod.productIterator.map{
      case Some(value) => value
      case None => ""
      case rest => rest
    }.mkString(",")
  }

  def checker[T <: Long](target: T, ips: Array[T]): T = {
    def search(start: Int = 0, end: Int = ips.length-1): T = {
      val mid = start + (end-start)/2
      if (start > end) ips(start-1)
      else if (ips(mid) == target) ips(mid)
      else if (ips(mid) > target) search(start, mid-1)
      else search(mid+1, end)
    }
    search()
  }
  def main(args : Array[String]): Unit = {

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
    val dataLoader = new DataLoader()

    if(conf.associateLocation.apply()) { // phase 1
        // ===================== load files
        val locationsRDD = dataLoader.loadLocations(conf.ip2locationPath.apply().getAbsolutePath, sc)
        val editsRDD = dataLoader.loadEdits(conf.editsPath.apply().getAbsolutePath, sc)


        // broadcast structures through clusters
        val longIps: Broadcast[Array[Long]] = sc.broadcast(locationsRDD.keys.collect.sorted) // broadcast array to share between nodes
        val locationsMap = sc.broadcast(locationsRDD.collect.toMap)

        // ====
        //val firstEdits = sc.parallelize(editsRDD.take(20))
        //val editsWithIpClass = firstEdits.mapValues { case edit: Edit => (checker(edit.longIp, longIps.value), edit) }
        // ===
        val editsWithIpClass = editsRDD.mapValues { case edit: Edit => (checker(edit.longIp, longIps.value), edit) }
        // ===
        ////editsWithIpClass.values.coalesce(1, true).saveAsTextFile("src/main/resources/output.txt")

        //val editsWithLocation = editsWithIpClass.values.join(locationsRDD)
        val editsWithLocation = editsWithIpClass.values.flatMap { case (key, value) =>
          locationsMap.value.get(key).map {
            otherValue =>
              (key, (value, otherValue))
          }
        }

      editsWithLocation.values.map(x => s"${x._1.toCSV()},${x._2}").saveAsTextFile(conf.outLocationsFile.apply().getAbsolutePath, classOf[GzipCodec])

      longIps.destroy
      locationsMap.destroy

    } else { // phase 2

    }
  }
}
