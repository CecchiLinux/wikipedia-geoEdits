import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.broadcast.Broadcast
import javax.imageio.ImageIO
import java.awt.{Color, RenderingHints}
import java.awt.image.BufferedImage
import java.io.File

import Utility.{Conf, DataLoader, WorldMap}
import Model._
import My_KMeans._
import org.apache.spark.rdd.RDD
import Ordering.Implicits._ // tell compiler to use scala.math.Ordering.Ops

object Main extends App {

  object locationColumns extends Enumeration {
    val classIp, countryCode, countryName, regionName, city, latitude, longitude = Value
  }

  //implicit class CSVWrapper(val prod: Product) extends AnyVal {
  //  def toCSV(separator: String): String = prod.productIterator.map {
  //    case Some(value) => value
  //    case None => ""
  //    case rest => rest
  //  }.mkString(separator)
  //}

  def checker[T: Numeric](target: T, ips: Array[T]): T = { // Numeric <: Ordering (Ordering is an upper bound)
    /**
     * Dichotomic search: search for the target element through the array.
     * Return the target element if present or the nearest smaller element on the left.
     * Thought for the interval research of the target.
     */

    // tail-recursive binary search
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

    val conf = new Conf(args)
    val mySparkConf = new SparkConf()
    val masterURL = conf.master_url.apply()
    mySparkConf.setAppName("WikipediaEdits")
    mySparkConf.setMaster(masterURL)
    mySparkConf.set("spark-serializer", "org.apache.spark.serializer.KryoSerializer")
    mySparkConf.set("spark.kryoserializer.buffer.max", "2047")
    mySparkConf.registerKryoClasses(Array(classOf[Point]))
    val mainFolderPath = conf.dataset_folder.apply().getAbsolutePath()

    val k = conf.k.apply()
    val iterations = conf.iterations.apply()
    val epsilon = conf.epsilon.apply()

    val sc = SparkSession
      .builder()
      .config(mySparkConf)
      .getOrCreate().sparkContext
    //sc.setLogLevel("ERROR")

    //var inFileEditsPath = conf.editsPath.apply().getAbsolutePath
    //val inFileLocationsPath = conf.ip2locationPath.apply().getAbsolutePath
    val locationsRDD: RDD[(Long, String)] =
      DataLoader.loadLocations(conf.ip2LocationPath, separator = "\",\"") // location into RDD


    // ========================================================================
    // Phase 1 - Assign location to each edit
    // -a option | From raw Edits to Categories with Ips
    // From: (artId, revId, artName, ip, categories, entity, longIp)
    // To: (single category, List(classIp))
    //  where classIp is the class associated to the original longIp
    // =================================================================================================================
    if (conf.associate_location.apply()) {
      val editsRDD: RDD[(String, Edit)] =
        DataLoader.loadEdits(conf.editsPath, separator = '|') // edits into RDD
      val longIps: Broadcast[Array[Long]] =
        sc.broadcast(locationsRDD.keys.collect.sorted) // broadcast array through clusters

      //// === partial run
      //val firstEdits = sc.parallelize(editsRDD.take(20))
      //val editsWithIpClass = firstEdits.mapValues {
      //  case edit: Edit => (checker(edit.longIp, longIps.value), edit)
      //}
      //// === full run
      val editsWithIpRDD = editsRDD
        .mapValues { case edit: Edit => (checker(edit.longIp, longIps.value), edit) }
        // RDD[String, (Long, Edit)]
        .values.flatMap(
        // RDD[Long, Edit]
        edit => {
          val categoriesString = edit._2.categories
          val categories = categoriesString.split(' ')
          val ipClass = edit._1
          categories.map(category => (category, ipClass))
        })
        // RDD[(String, Long)]
        .map(t => (t._1, List(t._2)))
        .reduceByKey(_ ::: _)
      // RDD[(String, List[Long])] // (category, List of ips)

      editsWithIpRDD.map { case (category, ips) => s"${category}#${ips.mkString("|")}" }
        .coalesce(1, true).saveAsTextFile(conf.outCategoriesIps, classOf[BZip2Codec])
    }


    // ========================================================================
    // Phase 2 - Filter category by input words, perform the k-means, print sub categories in the map
    // -w <words to include> -n <words to exclude> -k <k> -e <e>
    //  where "k" is the number of the clusters and "e" is the epsilon to be used for the termination condition
    // =================================================================================================================
    val locationsMap: Broadcast[Map[Long, String]] =
     sc.broadcast(locationsRDD.collect.toMap) // broadcast array through clusters

    val filterWords = conf.words.apply()
    val excludedWords = conf.no_words.apply()
    var outFolderName = mainFolderPath + "/filters-" + filterWords.mkString("_") // es. filters-italian_food
    if (excludedWords.length > 0) outFolderName += "-" + excludedWords.mkString("_")
    outFolderName += "-" + k + "-" + iterations + "-" + epsilon

    val catIpsRDD = DataLoader.loadCatsWithIps(conf.outCategoriesIps + "/part-00[0-5]*", '|') // ip to coordinates
    val filteredRDD = filterWords.length match {
      case 0 => catIpsRDD
      case _=> catIpsRDD
        .filter(x => filterWords.exists(f => x._1.replaceAll("[-+.^:,]","").split("_").contains(f))) // filter categories that contain all the words
        .filter(x => excludedWords.exists(f => !(x._1.replaceAll("[-+.^:,]","").split("_").contains(f))))
    }

    // ========================================
    // Phase 2.1 - K-Means
    //
    // =================================================================================================================
    // assigns points to categories
    //val catPtsRDD = DataLoader.loadPoints(outFolderName + "/part-00[0-5]*", '|') // ip to coordinates
    val catPtsRDD = filteredRDD
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
    // RDD[(String, List[Point])] // (category, (point1, point2, ...))

    // kmeans input parameters: points and initial centroids
    val pts = catPtsRDD
      // RDD[(String, List[Point])]
      .flatMap { case (_, pts) => pts }
    // RDD [Point]
    System.err.println("Number of points: " + pts.collect.length + "\n")

    val centroids = pts.takeSample(withReplacement = false, k)
    // System.err.println("Generated centroids: " + centroids.mkString("(", ", ", ")"))


    val mkmeans = new My_KMeans(masterURL, pts, epsilon, iterations)

    val resultCentroids = {
      if (iterations > 0) mkmeans.clusterize(k, centroids, mkmeans.KMeansHelper.stopCondIterations)
      else mkmeans.clusterize(k, centroids, mkmeans.KMeansHelper.stopCondVariance)
    }
    System.err.println(resultCentroids.map(c => "%3f,%3f\n".format(c.x, c.y)).mkString)

    // Group the points into clusters
    val groups = pts
      // RDD[Point]
      .map(pt => (mkmeans.KMeansHelper.closestCentroid(resultCentroids, pt), List(pt)))
      // RDD[(Point, List[Point])]
      .reduceByKey(_ ::: _)
      // RDD[(Point, List[Point])]
      .map { case (pt, pts) => (resultCentroids.indexOf(pt), pts) }
    // RDD[(Int, List[Points])]



    // ========================================
    // Phase 2.2 - Count clusters categories
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

    //val pts2CatsBroad: Broadcast[Map[Point, List[(String, Int)]]] =
    //  sc.broadcast(catPtsCount.collect.toMap) // broadcast array through clusters

    val pts2Cats: Map[Point, List[(String, Int)]] =
      catPtsCount.collect.toMap

    val groupsCats = groups
      // RDD[(Int, List[Point])]
      .mapValues(pts => pts distinct) // remove the duplicates (they are already considered on the pts2Cats)
      .mapValues(
        pts => {
          pts.flatMap(pt => {
            pts2Cats(pt)
          })
        }
      )
    // RDD[(Int, List[(String, Int)])]
      .map { case (group, cats) =>
        (group, cats.groupBy(_._1).mapValues(_.map(_._2).sum).toList)
      }
    // RDD[(Int, List[(String, Int)])]

    // Write to disk the clusters with their categories
    groupsCats.map {
      case (cluster, cats) =>
        val latitude = resultCentroids(cluster).y
        val longitude = resultCentroids(cluster).x
        s"${cluster},${latitude},${longitude}:\n\t${cats.mkString("\n\t")}"
    }.coalesce(1, true).saveAsTextFile(outFolderName + "/clustersCats.csv")


    // select the category with the highest number of contributions
    val mainCats =
      groupsCats.mapValues(
        cats => cats.reduce((x, y) => if (x._2 >= y._2) x else y)
      )
    // RDD[(Int, (String, Int))]

    // Write to disk the clusters with their main categories
    mainCats.map {
      case (cluster, (cat, number)) => {
        val latitude = resultCentroids(cluster).y
        val longitude = resultCentroids(cluster).x
        s"${cluster},${latitude},${longitude},${cat},${number}"
      }
    }.coalesce(1, true).saveAsTextFile(outFolderName + "/mainCats.csv")

    // group clusters by categories
    val superCluster = mainCats
      .map {
        case (cluster, (cat, _)) =>
          (cat, cluster)
      }
      .map(t => (t._1, List(t._2)))
      .reduceByKey(_ ::: _)
      // RDD[String, List[Int]]
      .collect()


    // ========================================
    // Phase 3 - Print the map
    //
    // =================================================================================================================
    // draw map
    val localGroups = groups.collect()
    val image = new BufferedImage(conf.imageWidth, conf.imageHeight, BufferedImage.TYPE_INT_ARGB)
    val graphics = image.createGraphics
    val imageFile = Main.getClass.getClassLoader.getResourceAsStream(conf.backgroundImageFileName)
    val imageFileBoundaries = Main.getClass.getClassLoader.getResourceAsStream(conf.foregroundBoundaries)
    graphics.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)

    val groupColors = for (group <- 0 until k) yield WorldMap.generateColor(group, k) // select a color for each cluster
    WorldMap.drawMapBackground(imageFile, graphics, conf.imageWidth, conf.imageHeight)
    WorldMap.drawInfo(k, iterations, epsilon, filterWords.toArray, excludedWords.toArray, graphics, conf.imageWidth, conf.imageHeight, groupColors)
    WorldMap.drawIps(localGroups, graphics, conf.imageWidth, conf.imageHeight, groupColors)
    WorldMap.drawMapBackground(imageFileBoundaries, graphics, conf.imageWidth, conf.imageHeight)
    WorldMap.drawCentroid(resultCentroids, graphics, conf.imageWidth, conf.imageHeight, groupColors)
    WorldMap.drawIpsCounts(resultCentroids, localGroups, conf.print_numbers.apply(), graphics, conf.imageWidth, conf.imageHeight, groupColors)
    //WorldMap.writeTopCategories(mainCats.collect(), graphics, conf.imageWidth, conf.imageHeight, groupColors)
    // write image to disk
    ImageIO.write(image, conf.imageFormat, new File(outFolderName + "/map-" + filterWords.mkString("_") + "-" + k + ".png"))


    val image2 = new BufferedImage(conf.imageWidth, conf.imageHeight, BufferedImage.TYPE_INT_ARGB)
    val graphics2 = image2.createGraphics
    val imageFile2 = Main.getClass.getClassLoader.getResourceAsStream(conf.backgroundImageFileName)
    val imageFileBoundaries2 = Main.getClass.getClassLoader.getResourceAsStream(conf.foregroundBoundaries)
    graphics2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)

    val colors = for (group <- 0 until superCluster.length) yield WorldMap.generateColor(group, superCluster.length) // select a color for each cluster
    val superGroupColors = new Array[Color](k)
    var i = 0
    for ((_, groups) <- superCluster) {
      for (group <- groups) {
        superGroupColors(group) = colors(i)
      }
      i += 1
    }
    WorldMap.drawMapBackground(imageFile2, graphics2, conf.imageWidth, conf.imageHeight)
    WorldMap.drawInfo(k, iterations, epsilon, filterWords.toArray, excludedWords.toArray, graphics2, conf.imageWidth, conf.imageHeight, superGroupColors)
    WorldMap.drawIps(localGroups, graphics2, conf.imageWidth, conf.imageHeight, superGroupColors)
    WorldMap.drawMapBackground(imageFileBoundaries2, graphics2, conf.imageWidth, conf.imageHeight)
    WorldMap.drawCentroid(resultCentroids, graphics2, conf.imageWidth, conf.imageHeight, superGroupColors)
    //WorldMap.drawIpsCounts(resultCentroids, localGroups, conf.print_numbers.apply(), graphics2, conf.imageWidth, conf.imageHeight, superGroupColors)
    WorldMap.writeSuperCategories(superCluster, graphics2, conf.imageWidth, conf.imageHeight, colors)
    // write image to disk
    ImageIO.write(image2, conf.imageFormat, new File(outFolderName + "/map-" + filterWords.mkString("_") + "-" + k + "-grouped.png"))
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

  def distinct[A](list: Iterable[A]): List[A] = {
    list.foldLeft(List[A]()) {
      case (acc, item) if acc.contains(item) => acc
      case (acc, item) => item :: acc
    }
  }

}
