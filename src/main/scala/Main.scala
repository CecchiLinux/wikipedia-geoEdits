import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.hadoop.fs._
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.broadcast.Broadcast

object Main {

  implicit class CSVWrapper(val prod: Product) extends AnyVal {
    def toCSV(): String = prod.productIterator.map{
      case Some(value) => value
      case None => ""
      case rest => rest
    }.mkString(",")
  }

  case class Location(longIp: Long, countryCode: String, countryName: String, regionName: String, city: String, latitude: String , longitude: String)
  case class Edit(artId: String, revId: String, artName: String, datetime:String, ip: String, category: String,
                  relPages: String, entity: String, longIp: Long)

  // tail-recursive binary search
  // return the target element if present,
  // otherwise the closest element on the left
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

    // ======================== conf and input
    val mySparkConf = new SparkConf()
    mySparkConf.setAppName("WikipediaEdits")
    mySparkConf.setMaster("local[8]")

    val folderPath = args(0)
    val ip2locationFile = folderPath + "IP2LOCATION-LITE-DB9.CSV.bz2"
    val editsFile = folderPath + "enwiki-longIp.bz2"
    val outputFile = folderPath + args(1)

    val sc = SparkSession
      .builder()
      .config(mySparkConf)
      .getOrCreate().sparkContext
    sc.setLogLevel("ERROR")

    val fs = FileSystem.get(sc.hadoopConfiguration)
    if(!fs.exists(new Path(editsFile))) {
      println("Input path does not exists")
      return
    }
    if(fs.exists(new Path(outputFile))) {
      println("Output file already exists")
      return
    }

    val locationsTextRDD = sc.textFile(ip2locationFile)

    // ==========================================================
    val locationsRDD = locationsTextRDD.map {
      line =>
        val col = line.replaceAll("\"", "").split(",")
        val keyIp = col(0).replaceAll("[^0-9.]", "").toLong
        // val location = col(0) + "," + col(2) + "," + col(3) + "," + col(4) + "," + col(5) + "," + col(6) + "," + col(7)
        val location = Location(
          longIp = keyIp,
          countryCode = col(2),
          countryName = col(3),
          regionName = col(4),
          city = col(5),
          latitude = col(6),
          longitude = col(7))
        (keyIp, location)
    }
    val longIps: Broadcast[Array[Long]] = sc.broadcast(locationsRDD.keys.collect.sorted) // broadcast array to share between nodes
    val smallLookup = sc.broadcast(locationsRDD.collect.toMap)

    val editsTextRDD = sc.textFile(editsFile)
    // editsTextRDD.take(10).foreach(println)
    val editsRDD = editsTextRDD.map {
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

    // ====
    //val firstEdits = sc.parallelize(editsRDD.take(20))
    //val editsWithIpClass = firstEdits.mapValues { case edit: Edit => (checker(edit.longIp, longIps.value), edit) }
    // ===
    val editsWithIpClass = editsRDD.mapValues { case edit: Edit => (checker(edit.longIp, longIps.value), edit) }
    // ===
    ////editsWithIpClass.values.coalesce(1, true).saveAsTextFile("src/main/resources/output.txt")

    //val editsWithLocation = editsWithIpClass.values.join(locationsRDD)
    val editsWithLocation = editsWithIpClass.values.flatMap { case (key, value) =>
      smallLookup.value.get(key).map {
        otherValue =>
          (key, (value, otherValue))
      }
    }

    //editsWithLocation.values.map(x => s"${x._1.toCSV()},${x._2}").saveAsTextFile(outputFile, classOf[GzipCodec])
    editsWithLocation.values.map(x => s"${x._1.toCSV()},${x._2.toCSV()}").saveAsTextFile(outputFile, classOf[GzipCodec])

    longIps.destroy
    smallLookup.destroy
  }
}
