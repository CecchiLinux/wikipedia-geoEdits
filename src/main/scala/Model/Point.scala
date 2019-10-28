package Model

@SerialVersionUID(100L)
case class Point(val x: Double, val y: Double) extends Serializable {
  def + (that: Point) = new Point(this.x + that.x, this.y + that.y)
  def - (that: Point) = this + (-that)
  def unary_- () = new Point(-this.x, -this.y)
  def / (d: Double) = new Point(this.x / d, this.y / d)
  def magnitude = math.sqrt(x * x + y * y)
  def distance(that: Point) = (that - this).magnitude
  override def toString = String.format("(%.2f,%.2f)", x, y)
}

object Point {
  def random() = {
    new Point(
      math.random * 50,
      math.random * 50)
  }
}