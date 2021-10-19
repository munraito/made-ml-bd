package utils

import breeze.linalg.{DenseMatrix, DenseVector, csvread, csvwrite}

import java.io.File

class io {
  def readAndSplit(dataPath: String, targetPath: String): (DenseMatrix[Double],
    DenseVector[Double], DenseMatrix[Double], DenseVector[Double]) = {
    val X = csvread(file=new File(dataPath), separator=',')
    val y = csvread(file=new File(targetPath), separator=',').toDenseVector
    val n = y.length
    val trainFraction = (n * .8).toInt

    val (yTrain, yTest) = (y(0 until trainFraction), y(trainFraction until n))
    val (xTrain, xTest) = (X(0 until trainFraction, ::), X(trainFraction until n, ::))
    (xTrain, yTrain, xTest, yTest)
  }

  def writePredictions(Ypred: DenseVector[Double], outPath: String): Unit = {
    csvwrite(new File(outPath), separator = ',', mat = Ypred.toDenseMatrix)
  }
}
