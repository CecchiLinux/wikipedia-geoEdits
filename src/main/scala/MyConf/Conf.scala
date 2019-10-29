package MyConf

import java.io.File
import org.rogach.scallop.ScallopConf

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {

  val associateLocation = opt[Boolean](
    descr = "Perform the association phase"
  )

  val ip2locationPath = opt[File](
    argName = "ip2location path",
    descr = "Path to the IP2LOCATION-LITE-DB9.CSV file",
    required = true
  )
  validateFileExists(ip2locationPath)

  val editsPath = opt[File](
    argName = "edits path",
    descr = "Path to the edits file (enwiki)",
    required = true
  )
  validateFileExists(editsPath)

  val outLocationsFile = opt[File](
    argName = "output path",
    descr = "Path where to store the edit with location"
  )
  validateFileDoesNotExist(outLocationsFile)

  val backgroundImageFileName = "world-map.png"
  val imageWidth = 1000
  val imageHeight = 500

  val masterURL = opt[String](
    argName = "Master URL",
    descr = "Master URL",
    default = Some("local[*]")
  )

  verify()
}

