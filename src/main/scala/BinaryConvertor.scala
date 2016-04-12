import scala.io.Source
import java.io._
/**
  * Created by greghuang on 4/12/16.
  */
object BinaryConvertor extends  App {
//  def main(args: Array[String]) {
//    val source = Source.fromFile("data/train/0a0e2f77")
//    val byteArray = source.map(_.toByte).toArray
//
//    source.close()
//  }

  var in = None: Option[FileInputStream]

  try {
    in = Some(new FileInputStream("data/train/000d18c1"))

    var c = 0
    val buf = scala.collection.mutable.ArrayBuffer.empty[Int]
    var cnt = 0
    while ({c = in.get.read; c != -1}) {
      buf += c
      //out.get.write(c)
    }
    val array = buf.toArray
    for (i <- 0 to 27) {
      for (j <- 0 to 27) {
        val index = i * 28 + j
        print(array.apply(index) + "\t")
      }
      println
    }
  } catch {
    case e: IOException => e.printStackTrace
  } finally {
    println("entered finally ...")
    if (in.isDefined) in.get.close
//    if (out.isDefined) out.get.close
  }
}
