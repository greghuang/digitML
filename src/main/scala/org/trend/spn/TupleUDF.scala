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

  def mergeCol =
    udf {(c1: Vector, c2: Vector) =>
      val a1 = c1.toArray
      val a2 = c2.toArray
      val a3 = a1 ++: a2
      Vectors.dense(a3)
    }
}
