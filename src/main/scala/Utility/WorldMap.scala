package Utility

import java.awt.{Color, Font, Graphics, Graphics2D}
import java.io.InputStream
import java.text.NumberFormat
import java.util.Locale

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
    val c = new Color(0f, 0f, 0f, 0f)
    graphics.drawImage(mapBackground, 0, 0, imageWidth, imageHeight, c, null)
    //graphics.setColor(new Color(0, 0, 0, 100))
    //graphics.fillRect(0, 0, imageWidth, imageHeight)
  }

  def drawIps(ips: Seq[(Int, List[Point])],graphics: Graphics, imageWidth: Int, imageHeight: Int, groupColors: IndexedSeq[Color]) {
    var tot_points = 0

    for ((group, coordinates) <- ips) {
      val color = groupColors(group).brighter()
      graphics.setColor(new Color(color.getRed, color.getGreen, color.getBlue, 50))
      for (coordinate <- coordinates) {
        val (x, y) = toImageCoordinates(coordinate.x, coordinate.y, imageWidth, imageHeight)
        graphics.fillOval(x - 1, y - 1, 4, 4)
      }
     tot_points += coordinates.length
    }

    graphics.setFont(new Font("Arial Black", Font.BOLD, 20))
    graphics.setColor(Color.BLACK)
    graphics.drawString("Tot. points: " + tot_points.toString, imageWidth / 2, imageHeight.toInt - 20)
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

  def drawIpsCounts(resultCentroids: Seq[Point], ips: Seq[(Int, List[Point])], numbers: Boolean, graphics: Graphics2D, imageWidth: Int, imageHeight: Int, groupColors: IndexedSeq[Color]) {
    graphics.setColor(Color.WHITE)
    val numberFormat = NumberFormat.getNumberInstance(Locale.US)
    val fontSize = if (resultCentroids.length > 30) 10 else 18
    val font = new Font(Font.SANS_SERIF, Font.BOLD, fontSize)
    graphics.setFont(font)
    for ((group, coordinates) <- ips) {
      val ipsCount = if (numbers) "(" + group.toString() + ") " + numberFormat.format(coordinates.size) else "(" + group.toString() + ")"
      val bound = font.getStringBounds(ipsCount, graphics.getFontRenderContext)
      val (x, y) = toImageCoordinates(resultCentroids(group).x, resultCentroids(group).y, imageWidth, imageHeight)
      // draw text shadow
      graphics.setColor(Color.BLACK)
      graphics.drawString(ipsCount, (x - bound.getWidth / 2).toInt + 1, (y + bound.getHeight + 8).toInt + 1)
      graphics.setColor(Color.WHITE)
      graphics.drawString(ipsCount, (x - bound.getWidth / 2).toInt, (y + bound.getHeight + 8).toInt)
    }
  }

  def writeTopCategories(topCats: Seq[(Int, (String, Int))], graphics: Graphics2D, imageWidth: Int, imageHeight: Int, groupColors: IndexedSeq[Color]) {
    val offset = 30
    var i = 0
    graphics.setFont(new Font("Arial Black", Font.BOLD, 20))

    for ((group, cat) <- topCats) {
      val color = groupColors(group)
      val string = "(" + group.toString + ") " + cat._1.replace("_", " ") + " " + cat._2.toString
      val currentHeight = if (topCats.length <= 15) imageHeight / 2 + offset * i else imageHeight / 3 + offset * i
      graphics.setColor(color)
      graphics.fillRect(10, currentHeight, 20, 20)
      graphics.setColor(Color.WHITE)
      graphics.drawRect(10, currentHeight, 20, 20)
      graphics.setColor(Color.BLACK)
      graphics.drawString(string, 38 + 1, currentHeight + 18 + 1)
      graphics.setColor(Color.WHITE)
      graphics.drawString(string, 38, currentHeight + 18)
      i = i + 1
    }
  }

  def writeSuperCategories(topCats: Seq[(String, List[Int])], graphics: Graphics2D, imageWidth: Int, imageHeight: Int, colors: IndexedSeq[Color]) {
    val offset = 30
    var i = 0
    graphics.setFont(new Font("Arial Black", Font.BOLD, 20))

    for ((cat, _) <- topCats) {
      val color = colors(i)
      //val string = "(" + group.toString + ") " + cat._1.replace("_", " ") + " " + cat._2.toString
      val string = cat.replace("_", " ")
      val currentHeight = if (topCats.length <= 15) imageHeight / 2 + offset*i else imageHeight / 3 + offset*i
      graphics.setColor(color)
      graphics.fillRect(10, currentHeight, 20, 20)
      graphics.setColor(Color.WHITE)
      graphics.drawRect(10, currentHeight, 20, 20)
      graphics.setColor(Color.BLACK)
      graphics.drawString(string, 38 + 1, currentHeight + 18 + 1)
      graphics.setColor(Color.WHITE)
      graphics.drawString(string, 38, currentHeight + 18)
      i = i + 1
    }

  }

  def drawInfo(k: Int, iterations: Int, epsilon: Double, words: Array[String], noWords: Array[String], graphics: Graphics, imageWidth: Int, imageHeight: Int, groupColors: IndexedSeq[Color]) {
    graphics.setFont(new Font("Arial Black", Font.BOLD, 20))
    graphics.setColor(Color.BLACK)
    graphics.drawString("Filter words: " + words.mkString(" "), imageWidth / 2, imageHeight.toInt - 60)
    graphics.drawString("k: %d,  \tit: %d,  \teps: %.5f".format(k, iterations, epsilon), imageWidth / 2, imageHeight.toInt - 40)
  }


}
