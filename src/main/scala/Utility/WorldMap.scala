package Utility

import java.awt.{Color, Font, Graphics, Graphics2D}
import java.io.{File, InputStream}
import java.text.NumberFormat
import java.util.Locale

import Model.Point
import javax.imageio.ImageIO
import javax.swing.{BoxLayout, JPanel}

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
    val c = new Color(0f, 0f, 0f, 0f)
    graphics.drawImage(mapBackground, 0, 0, imageWidth, imageHeight, c, null)
    //graphics.setColor(new Color(0, 0, 0, 100))
    //graphics.fillRect(0, 0, imageWidth, imageHeight)
  }

  def drawIps(ips: Seq[(Int, List[Point])],graphics: Graphics, imageWidth: Int, imageHeight: Int, groupColors: IndexedSeq[Color]) {
    for ((group, coordinates) <- ips) {
        val color = groupColors(group).brighter()
        graphics.setColor(new Color(color.getRed, color.getGreen, color.getBlue, 50))
        for (coordinate <- coordinates) {
          val (x, y) = toImageCoordinates(coordinate.x, coordinate.y, imageWidth, imageHeight)
          graphics.fillOval(x - 1, y - 1, 4, 4)
        }
      }
  }

  def drawCentroid(coordinates: Seq[Point], graphics: Graphics, imageWidth: Int, imageHeight: Int, groupColors: IndexedSeq[Color]) {
    for (group <- 0 until coordinates.length) {
      val (x, y) = toImageCoordinates(coordinates(group).x, coordinates(group).y, imageWidth, imageHeight)
      val color = groupColors(group)
      graphics.setColor(color)
      graphics.fillOval(x - 6, y - 6, 12, 12)
      graphics.setColor(Color.WHITE)
      graphics.drawOval(x - 6, y - 6, 12, 12)
    }
  }

  def drawIpsCounts(resultCentroids: Seq[Point], ips: Seq[(Int, List[Point])], graphics: Graphics2D, imageWidth: Int, imageHeight: Int, groupColors: IndexedSeq[Color]) {
    graphics.setColor(Color.WHITE)
    val numberFormat = NumberFormat.getNumberInstance(Locale.US)
    val font = new Font(Font.SANS_SERIF, Font.BOLD, 18)
    graphics.setFont(font)
    for ((group, coordinates) <- ips) {
      val ipsCount = numberFormat.format(coordinates.size)
      val bound = font.getStringBounds(ipsCount, graphics.getFontRenderContext)
      val (x, y) = toImageCoordinates(resultCentroids(group).x, resultCentroids(group).y, imageWidth, imageHeight)
      // draw text shadow
      graphics.setColor(Color.BLACK)
      graphics.drawString(ipsCount, (x - bound.getWidth / 2).toInt + 1, (y + bound.getHeight + 10).toInt + 1)
      // draw text
      graphics.setColor(Color.WHITE)
      graphics.drawString(ipsCount, (x - bound.getWidth / 2).toInt, (y + bound.getHeight + 10).toInt)
    }
  }

  def writeTopCategories(topCats: Seq[(Int, (String, Int))], graphics: Graphics2D, imageWidth: Int, imageHeight: Int, groupColors: IndexedSeq[Color]) {
    val offset = 30
    var i = 0
    graphics.setFont(new Font("Arial Black", Font.BOLD, 20))

    for ((group, cat) <- topCats) {
      val color = groupColors(group)
      graphics.setColor(color)
      graphics.fillRect(10, imageHeight / 2 + offset*i, 20, 20)
      graphics.setColor(Color.WHITE)
      graphics.drawRect(10, imageHeight / 2 + offset*i, 20, 20)
      graphics.drawString(cat._1.replace("_", " ") + " " + cat._2.toString, 38, imageHeight / 2 + offset*i + 18)
      i = i+1
    }

  }

}
