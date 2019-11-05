package MyConf

import java.io.File
import org.rogach.scallop.ScallopConf

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {

  val associate_location = opt[Boolean](
    descr = "Perform the association phase"
  )

  val filter_categories = opt[Boolean](
    descr = "Perform the filter",
    required = true
  )

  val main_folder = opt[File](
    argName = "folder path",
    descr = "resources files folder",
    required = true
  )
  validateFileIsDirectory(main_folder)

  //val ip2locationPath = opt[File](
  //  argName = "ip2location path",
  //  descr = "Path to the IP2LOCATION-LITE-DB9.CSV file",
  //  required = true
  //)
  //validateFileExists(ip2locationPath)

  //val editsPath = opt[File](
  //  argName = "edits path",
  //  descr = "Path to the edits file (enwiki)",
  //  required = true
  //)
  //validateFileExists(editsPath)

  //val outLocationsPath= opt[File](
  //  argName = "output path",
  //  descr = "Path where to store the edit with location"
  //)
  //validateFileDoesNotExist(outLocationsPath)

  val words = opt[List[String]](
    argName = "required words",
    descr = "Comma separated required words.",
    required = true
  )

  val no_words = opt[List[String]](
    argName = "excluded words",
    descr = "Comma separated excluded words.",
    default = Some(List[String]())
  )

  val k = opt[Int](
    argName = "clusters",
    descr = "Number of clusters to create.",
    validate = (arg) => arg >= 0,
    default = Some(0)
  )

  val iterations = opt[Int](
    descr = "Number of iterations that the clustering algorythm will be run for.",
    validate = (arg) => arg >= 0,
    default =  Some(0)
  )

  val epsilon = opt[Double](
    descr = "Variance improvement threshold",
    validate = (arg) => arg >= 0.0,
    default =  Some(0.01)
  )

  //val backgroundImageFileName = "world-map2.png"
  //val imageFormat = "png"
  //val imageWidth = 1000
  //val imageHeight = 500

  //val backgroundImageFileName = "Day_lrg_white.png"
  val backgroundImageFileName = "world.png"
  val foregroundBoundaries = "world_wbt.png"
  //val backgroundImageFileName = "Day_lrg.png"
  val imageFormat = "png"
  val imageWidth = 2000
  val imageHeight = 1000


  val masterURL = opt[String](
    argName = "Master URL",
    descr = "Master URL",
    default = Some("local[*]")
  )

  verify()

  val ip2LocationPath = main_folder.apply().getAbsolutePath + "/IP2LOCATION-LITE-DB9.CSV.bz2"
  val editsPath = main_folder.apply().getAbsolutePath + "/enwiki-longIpOnly.bz2"
  val outCategoriesIps = main_folder.apply().getAbsolutePath + "/catIpsFinal"
}

