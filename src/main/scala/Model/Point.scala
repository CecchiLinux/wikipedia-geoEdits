package Model

@SerialVersionUID(100L)
class Point(val x: Double, val y: Double) extends Serializable {
  def + (that: Point) = new Point(this.x + that.x, this.y + that.y)
  def - (that: Point) = this + (-that)
  def unary_- () = new Point(-this.x, -this.y)
  def / (d: Double) = new Point(this.x / d, this.y / d)
  def magnitude = math.sqrt(x * x + y * y)
  def distance(that: Point) = (that - this).magnitude
  override def equals(that: Any): Boolean = {
    that match {
      case that : Point => this.x == that.x && this.y == that.y
      case _ => false
    }
  }
  override def hashCode(): Int = {
    (this.x, this.y).hashCode()
  }
  override def toString = "(" + x.toString + "," + y.toString + ")"
}

object Point {
  def random() = {
    import java.text.DecimalFormat
    val minLat = -90.00
    val maxLat = 90.00
    val latitude = minLat + (Math.random * ((maxLat - minLat) + 1)).toDouble
    val minLon = 0.00
    val maxLon = 180.00
    val longitude = minLon + (Math.random * ((maxLon - minLon) + 1)).toDouble
    val df = new DecimalFormat("#.#####")
    new Point(
      longitude,
      latitude
    )
  }
}