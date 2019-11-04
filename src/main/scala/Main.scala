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
import org.apache.spark.rdd.RDD

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
    /**
     * Dichotomic search: search for the target element through the array.
     * Return the target element if present or the nearest smaller element on the left.
     * Thought for the interval research of the target.
     */
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
    mySparkConf.set("spark-serializer", "org.apache.spark.serializer.KryoSerializer")
    mySparkConf.set("spark.kryoserializer.buffer.max", "2047")
    mySparkConf.registerKryoClasses(Array(classOf[Point]))
    val mainFolderPath = conf.mainFolder.apply().getAbsolutePath()

    val sc = SparkSession
      .builder()
      .config(mySparkConf)
      .getOrCreate().sparkContext
    sc.setLogLevel("ERROR")

    //var inFileEditsPath = conf.editsPath.apply().getAbsolutePath
    //val inFileLocationsPath = conf.ip2locationPath.apply().getAbsolutePath
    val locationsRDD: RDD[(Long, String)] =
    DataLoader.loadLocations(conf.ip2LocationPath, separator = "\",\"") // location into RDD


    // ========================================================================
    // -a option | From raw Edits to Categories with Ips
    // From: (artId, revId, artName, ip, categories, entity, longIp)
    // To: (single category, List(classIp))
    //  where classIp is the class associated to the original longIp
    // =================================================================================================================
    if (conf.associateLocation.apply()) {
      val editsRDD: RDD[(String, Edit)] =
        DataLoader.loadEdits(conf.editsPath, separator = '|') // edits into RDD
      val longIps: Broadcast[Array[Long]] =
        sc.broadcast(locationsRDD.keys.collect.sorted) // broadcast array through clusters

      associateIps(editsRDD, longIps)
        // RDD[(String, List[Long])] // (category, List of ips)
        .map { case (category, ips) => s"${category}#${ips.mkString("|")}" }
        //.map(x => x._1 + "#" + x._2.mkString("|"))
        .saveAsTextFile(conf.outCategoriesIps, classOf[BZip2Codec])
    }


    // ========================================================================
    // Filter category by input words, perform the k-means, print sub categories in the map
    // -w <words to include> -k <k> -e <e>
    //  where "k" is the number of the clusters and "e" is the epsilon to be used for the termination condition
    // =================================================================================================================
    val locationsMap: Broadcast[Map[Long, String]] =
    sc.broadcast(locationsRDD.collect.toMap) // broadcast array through clusters

    ////val reg = ".*_war(s)?_.*".r
    val catFilter = conf.words.apply()
    val outFileName = mainFolderPath + "/filters-" + catFilter.mkString("_") // es. filters-italian_food

    if (conf.filterCategories.apply()) {
      val editsRDD: RDD[String] = sc.textFile(conf.outCategoriesIps + "/part-00[0-5]*")
      filterCategories(catFilter, editsRDD)
        // RDD[(String, String)]
        .map { case (category, ips) => s"${category}#${ips}" }
        //.map(x => s"${x._1}#${x._2}")
        .coalesce(1, true).saveAsTextFile(outFileName)
    }


    // ========================================
    // K-Means
    //
    // =================================================================================================================
    val k = conf.k.apply()
    val epsilon = conf.epsilon.apply()

    val catPtsRDD = DataLoader.loadPoints(outFileName + "/part-00[0-5]*", '|') // ip to coordinates
      // RDD[(String, List[Long])]
      .mapValues(
        ips => {
          ips.map(ip => {
            val locationString = locationsMap.value(ip)
            val location = locationString.split('|')
            val latitude = location(locationColumns.latitude.id)
            val longitude = location(locationColumns.longitude.id)
            new Point(longitude.toDouble, latitude.toDouble)
          })
        }
      )
    // RDD[(String, List[Point])]

    val pts = catPtsRDD
      // RDD[(String, List[Point])]
      .flatMap { case (_, pts) => pts }
    // RDD [Point]

    System.err.println("Number of points: " + pts.collect.length + "\n")

    val centroids = pts.takeSample(withReplacement = false, k)
    System.err.println("Generated centroids: " + centroids.mkString("(", ", ", ")"))

    // Performe the k-means
    val mkmeans = new My_KMeans(masterURL, pts)
    val resultCentroids = mkmeans.clusterize(k, centroids, epsilon)
    System.err.println(resultCentroids.map(centroid => "%3f,%3f\n".format(centroid.x, centroid.y)).mkString)
    //    System.err.println("Result centroids: " + resultCentroids.mkString("(", ", ", ")"))


    // Group the points into clusters
    val groups = pts
      // RDD[Point]
      .map(pt => (mkmeans.KMeansHelper.closestCentroid(resultCentroids, pt), List(pt)))
      // RDD[(Point, List[Point])]
      .reduceByKey(_ ::: _)
      // RDD[(Point, List[Point])]
      .map { case (pt, pts) => (resultCentroids.indexOf(pt), pts) }
    // RDD[(Int, List[Points])]
    //.collect()
    // Array[(Int, List[Point])] //Array[(centroidIndex, List[closest points]


    // ========================================
    // Count clusters categories
    //
    // =================================================================================================================

    val catPtsCount = catPtsRDD
      // RDD[(String, List[Point])]
      .map {
        case (cat, pts) => {
          val ptsOccurences = pts.groupBy(pt => pt).mapValues(_.size).toList
          (cat, ptsOccurences)
        }
      }
      // RDD[(String, List[(Point, Int)])]
      .flatMap {
        case (cat, ptsOccurrences) =>
          ptsOccurrences.map(po => (po._1, (cat, po._2)))
      }
      // RDD[(Point, List[(String, Int)])]
      .map(t => (t._1, List(t._2)))
      .reduceByKey(_ ::: _)
    // RDD[(Point, List[(String, Int)])]

    val pts2CatsBroad: Broadcast[Map[Point, List[(String, Int)]]] =
      sc.broadcast(catPtsCount.collect.toMap) // broadcast array through clusters

    val groupsCats = groups
      // RDD[(Int, List[Point])]
      .mapValues(pts => pts distinct) // remove the duplicates (they are already considered on the pts2CatsBroad)
      .mapValues(
        pts => {
          pts.flatMap(pt => {
            pts2CatsBroad.value(pt)
          })
        }
      )
    // RDD[(Int, List[(String, Int)])]
      .map { case (group, cats) =>
        (group, cats.groupBy(_._1).mapValues(_.map(_._2).sum).toList)
      }
    // RDD[(Int, List[(String, Int)])]

    //groupsCats.foreach(println)

    val mainCats =
      groupsCats.mapValues(
        cats => cats.reduce((x, y) => if (x._2 >= y._2) x else y)
      )
    // RDD[(Int, (String, Int))]
    mainCats.foreach(println)




    if(true) {

      // ========================================
      // Print map
      //
      // =================================================================================================================
      val image = new BufferedImage(conf.imageWidth, conf.imageHeight, BufferedImage.TYPE_INT_ARGB)
      val graphics = image.createGraphics
      graphics.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)

      // draw map
      val localGroups = groups.collect()
      val imageFile = Main.getClass.getClassLoader.getResourceAsStream(conf.backgroundImageFileName)
      val imageFileBoundaries = Main.getClass.getClassLoader.getResourceAsStream(conf.foregroundBoundaries)
      val groupColors = for (group <- 0 until k) yield WorldMap.generateColor(group, k)
      WorldMap.drawMapBackground(imageFile, graphics, conf.imageWidth, conf.imageHeight)
      WorldMap.drawIps(localGroups, graphics, conf.imageWidth, conf.imageHeight, groupColors)
      WorldMap.drawMapBackground(imageFileBoundaries, graphics, conf.imageWidth, conf.imageHeight)
      WorldMap.drawCentroid(resultCentroids, graphics, conf.imageWidth, conf.imageHeight, groupColors)
      WorldMap.drawIpsCounts(localGroups, graphics, conf.imageWidth, conf.imageHeight, groupColors)
      WorldMap.writeTopCategories(mainCats.collect(), graphics, conf.imageWidth, conf.imageHeight, groupColors)
      // write image to disk
      ImageIO.write(image, conf.imageFormat, new File(mainFolderPath + "/map" + "-" + catFilter.mkString("_") + ".png"))
    }
  }


  def filterCategories(catFilter: List[String], editsRDD: RDD[String]) = {

    // val reg = ".*_world_war_ii_.*".r
    editsRDD.map(line => line.split('#'))
      // RDD[ Array[ String ]
      .map(arr => {
        val category = arr(0).toLowerCase()
        val ips = arr(1)
        (category, ips)
      })
      // RDD [String, String]
      //.filter(x => reg.r.pattern.matcher(x._1).matches)
      .filter(x => catFilter.forall(f => x._1.split("_").contains(f))) // filter categories that contain all the words

  }

  def associateIps(editsRDD: RDD[(String, Edit)], longIps: Broadcast[Array[Long]]) = {
    //// === partial run
    //val firstEdits = sc.parallelize(editsRDD.take(20))
    //val editsWithIpClass = firstEdits.mapValues {
    //  case edit: Edit => (checker(edit.longIp, longIps.value), edit)
    //}
    //// === full run
    editsRDD.mapValues { case edit: Edit => (checker(edit.longIp, longIps.value), edit) }
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

  //def categoriesCounter(inFileEditsPath: String, outFile: String, sc: SparkContext) = {

  //  // ===================== categories count
  //  val editsRDD = DataLoader.loadEditsWithClass(inFileEditsPath + "/part-00[0-5]*", separator = '|')
  //  val catCounts = editsRDD.values.
  //    map { case (edit) => edit.categories }.
  //    //filter((x => x != "") ).
  //    flatMap(categories => categories.split(" ")).
  //    map(x => (x, 1)).reduceByKey(_ + _)
  //  val catCountsSort = catCounts.map(x => (x._2, x._1)).sortByKey()
  //  catCountsSort.coalesce(1, true).saveAsTextFile("/home/enrico/datasets/catCounts.txt")

  //}

  //TODO
  /**
   *
   * @param list
   * @tparam A
   * @return
   */
  def distinct[A](list: Iterable[A]): List[A] = {
    list.foldLeft(List[A]()) {
      case (acc, item) if acc.contains(item) => acc
      case (acc, item) => item :: acc
    }
  }

}
