package Utility

import java.awt.{Color, Graphics}
import java.io.{File, InputStream}

import Model.Point
import javax.imageio.ImageIO

object WorldMap {

  def toImageCoordinates(longitude: Double, latitude: Double, imageWidth: Int, imageHeight: Int): (Int, Int) = {
    (
      (imageWidth * (0.5 + longitude / 360)).toInt,
      (imageHeight * (0.5 - latitude / 180)).toInt
    )
  }

  def generateColor(group: Int, groupCount: Int): Color = {
    val hue = group.toFloat / groupCount
    val saturation = 0.8f
    val lightness = 0.5f
    Color.getHSBColor(hue, saturation, lightness)
  }

  def drawMapBackground(imfFile: InputStream, graphics: Graphics, imageWidth: Int, imageHeight: Int) {
    val mapBackground = ImageIO.read(imfFile)
    graphics.drawImage(mapBackground, 0, 0, imageWidth, imageHeight, Color.WHITE, null)
    graphics.setColor(new Color(0, 0, 0, 100))
    graphics.fillRect(0, 0, imageWidth, imageHeight)
  }

  def drawIps(coordinates: Seq[(Int, List[Point])],graphics: Graphics, imageWidth: Int, imageHeight: Int, groupColors: IndexedSeq[Color]) {
    for ((group, ips) <- coordinates) {
        val color = groupColors(group).brighter()
        graphics.setColor(new Color(color.getRed, color.getGreen, color.getBlue, 50))
        for (coordinate <- ips) {
          val (x, y) = toImageCoordinates(coordinate.x, coordinate.y, imageWidth, imageHeight)
          graphics.fillOval(x - 1, y - 1, 2, 2)
        }
      }
  }

  def drawCentroid(coordinates: Seq[Point],graphics: Graphics, imageWidth: Int, imageHeight: Int, groupColors: IndexedSeq[Color]) {
    for (group <- 0 until coordinates.length) {
      val (x, y) = toImageCoordinates(coordinates(group).x, coordinates(group).y, imageWidth, imageHeight)
      val color = groupColors(group)
      graphics.setColor(color)
      graphics.fillOval(x - 6, y - 6, 12, 12)
      graphics.setColor(Color.WHITE)
      graphics.drawOval(x - 6, y - 6, 12, 12)
    }
  }

}
