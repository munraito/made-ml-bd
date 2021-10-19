package utils

import breeze.linalg.{DenseMatrix, DenseVector, sum}
import breeze.numerics.{pow, sqrt}

class LinReg {
  // implementation of linear regression using gradient descent

  def fit(X: DenseMatrix[Double], y: DenseVector[Double],
          nIter: Int, learnRate: Double): (DenseVector[Double], Double) = {
    var (weights, bias) = (DenseVector.zeros[Double](X.cols), .0)
    for (_ <- 0 to nIter) {
      val yHat = (X * weights) + bias
      weights :-= learnRate * 2 * (X.t * (yHat - y))
      weights = weights.map(el => el / X.rows)
      bias -= learnRate * 2 * sum(yHat - y) / X.rows
    }
    (weights, bias)
  }

  def predict(X: DenseMatrix[Double], weights: DenseVector[Double],
              bias: Double): DenseVector[Double] = {
    (X * weights) + bias
  }

  def RMSE(yTrue: DenseVector[Double], yPred: DenseVector[Double]): Double = {
    val error = sum((yTrue - yPred).map(el => pow(el, 2))) / yTrue.length
    sqrt(error)
  }
}
