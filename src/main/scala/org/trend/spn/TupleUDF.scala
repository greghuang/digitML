package org.trend.spn

import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
  * Created by greghuang on 5/5/16.
  */
object TupleUDF {
  import org.apache.spark.sql.functions.udf
  // type tag is required, as we have a generic udf
  import scala.reflect.runtime.universe.{TypeTag, typeTag}

  def toTuple2[S: TypeTag, T: TypeTag] =
    udf[(S, T), S, T]((x: S, y: T) => (x, y))

  def concatVector(v1:Vector, v2:Vector): Vector = {
    val agg = v1.toArray ++: v2.toArray
    Vectors.dense(agg)
  }


//  def mergeCol =
//    udf { (c: Vector*) =>
//      var agg = Seq.empty[Double]
//
//      c.foreach(v => {
//        agg = agg ++: v.toArray
//      })
//
//      Vectors.dense(agg.toArray)
//    }

  def mergeCol =
    udf {(c1: Vector, c2: Vector) =>
      val agg = c1.toArray ++: c2.toArray
      Vectors.dense(agg)
    }

  def merge3Col =
    udf {(c1: Vector, c2: Vector, c3: Vector) =>
      val agg = c1.toArray ++: c2.toArray ++: c3.toArray
      Vectors.dense(agg)
    }

  def merge4Col =
    udf {(c1: Vector, c2: Vector, c3: Vector, c4: Vector) =>
      val agg = c1.toArray ++: c2.toArray ++: c3.toArray ++: c4.toArray
      Vectors.dense(agg)
    }

  def merge8Col =
    udf {(c1: Vector, c2: Vector, c3: Vector, c4: Vector, c5: Vector, c6: Vector, c7: Vector, c8: Vector) =>
      val agg = c1.toArray ++: c2.toArray ++: c3.toArray ++: c4.toArray ++: c5.toArray ++: c6.toArray ++: c7.toArray ++: c8.toArray
      Vectors.dense(agg)
    }
}
