package org.trend.spn.convolution.filter

import org.apache.spark.mllib.linalg.{Matrix, Matrices}
import breeze.linalg.{DenseMatrix => BDM}

/**
  * Created by GregHuang on 5/5/16.
  */

trait Kernel {
  def matrix: Matrix
  def factor: Int
}

object Kernel {

  private class Sharp3x3 extends Kernel {
    val bm: BDM[Double] = BDM(
      (0.0, -1.0, 0.0),
      (-1.0, 5.0, -1.0),
      (0.0, -1.0, 0.0))
    override def matrix: Matrix = Matrices.dense(bm.rows, bm.cols, bm.toArray)
    override def factor: Int = 1
  }

  private class Blur3x3 extends Kernel {
    val bm: BDM[Double] = BDM(
      (1.0, 1.0, 1.0),
      (1.0, 1.0, 1.0),
      (1.0, 1.0, 1.0))
    override def matrix: Matrix = Matrices.dense(bm.rows, bm.cols, bm.toArray)
    override def factor: Int = 9
  }

  private class Edge3x3 extends Kernel {
    val bm: BDM[Double] = BDM(
      (0.0, -1.0, 0.0),
      (-1.0, 4.0, -1.0),
      (0.0, -1.0, 0.0))
    override def matrix: Matrix = Matrices.dense(bm.rows, bm.cols, bm.toArray)
    override def factor: Int = 1
  }

  private class EdgeV3x3 extends Kernel {
    val bm: BDM[Double] = BDM(
      (0.0, -1.0, 0.0),
      (0.0, 2.0, 0.0),
      (0.0, -1.0, 0.0))
    override def matrix: Matrix = Matrices.dense(bm.rows, bm.cols, bm.toArray)
    override def factor: Int = 1
  }

  private class EdgeH3x3 extends Kernel {
    val bm: BDM[Double] = BDM(
      (0.0, 0.0, 0.0),
      (-1.0, 2.0, -1.0),
      (0.0, 0.0, 0.0))
    override def matrix: Matrix = Matrices.dense(bm.rows, bm.cols, bm.toArray)
    override def factor: Int = 1
  }

  def apply(s: String): Kernel = {
    if (s == "sharp3x3") return new Sharp3x3
    if (s == "blur3x3") return new Blur3x3
    if (s == "edge3x3") return new Edge3x3
    if (s == "edgeH3x3") return new EdgeH3x3
    if (s == "edgeV3x3") return new EdgeV3x3
    else return null
  }
}
