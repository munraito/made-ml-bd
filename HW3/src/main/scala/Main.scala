import utils.io
import utils.LinReg

object Main {
  def main(args: Array[String]): Unit = {
    // path to data
    val (targetPath, dataPath, outPath) = (args(0), args(1), args(2))
    // hyperparameters
    val (nIter, learnRate) = (args(3).toInt, args(4).toDouble) // 1_000 .01
    // declare classes
    val (io, model) = (new io, new LinReg)

    // main pipeline: get data, fit & predict
    val (xTrain, yTrain, xTest, yTest) = io.readAndSplit(dataPath, targetPath)
    println(s"Successfully read and split data, ${xTrain.rows + xTest.rows} rows, ${xTrain.cols} columns")
    val (weights, bias) = model.fit(xTrain, yTrain, nIter, learnRate)
    println(s"Linear Regression fitted. \nweights: ${weights} \nbias: ${bias}")
    val yPred = model.predict(xTest, weights, bias)
    println(s"Got predictions. RMSE on validation dataset: ${model.RMSE(yTest, yPred)}")
    io.writePredictions(yPred, outPath)
    println(s"All done. Predictions are saved to: ${outPath}")
  }
}
