import scala.io.Source

/**
  * Created by Administrator on 2016/5/28.
  */
object test {
  def main(args: Array[String]) {
    val g2 = Source.fromFile("C:\\Users\\Administrator\\Desktop\\g22").getLines()
    g2.foreach{ line =>
      val arr = line.split(",")
      val size = arr.length
      val size2 = arr.distinct.length
      if(size != size2)
        println(line)
    }
  }
}
