package com.sourcelearning.datamlib

import breeze.linalg._
import breeze.numerics._

object matrix_vector {
  def main(args: Array[String]): Unit = {

    //全0矩阵 
    val m1 = DenseMatrix.zeros[Double](2, 3)
    println(m1)

    val v2 = DenseVector.ones[Double](3)
    println(v2)

  }
}