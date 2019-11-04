package MyConf

import java.io.File
import org.rogach.scallop.ScallopConf

//-a -i /home/enrico/datasets/IP2LOCATION-LITE-DB9.CSV.bz2 -e /home/enrico/datasets/enwiki-longIp.bz2 -o /home/enrico/datasets/outputTest
class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {

  val associateLocation = opt[Boolean](
    descr = "Perform the association phase"
  )

  val filterCategories = opt[Boolean](
    descr = "", //TODO
    required = true
  )

  val mainFolder = opt[File](
    argName = "folder path",
    descr = "resources files folder",
    required = true
  )
  validateFileIsDirectory(mainFolder)

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
    argName = "filter words",
    descr = "Comma separated filter words.",
    required = true
  )

  val k = opt[Int](
    argName = "clusters",
    descr = "Number of clusters to create.",
    validate = (arg) => arg>=0,
    default = Some(0)
  )

  val iterations = opt[Int](
    descr = "Number of iterations that the clustering algorythm will be run for.",
    validate = (arg) => arg>=0,
    default =  Some(0)
  )

  val epsilon = opt[Double](
    descr = "", //TODO
    validate = (arg) => arg>=0.0,
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

  val ip2LocationPath = mainFolder.apply().getAbsolutePath + "/IP2LOCATION-LITE-DB9.CSV.bz2"
  val editsPath = mainFolder.apply().getAbsolutePath + "/enwiki-longIpOnly.bz2"
  val outCategoriesIps = mainFolder.apply().getAbsolutePath + "/catIpsFinal"
}

