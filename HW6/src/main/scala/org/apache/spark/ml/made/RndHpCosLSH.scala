package org.apache.spark.ml.made

import scala.util.Random
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.linalg.{Matrices, Matrix, Vector, VectorUDT, Vectors}
import org.apache.spark.ml.feature.{LSH, LSHModel}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasSeed
import org.apache.spark.ml.util.{DefaultParamsReader, DefaultParamsWriter, Identifiable, MLReadable, MLReader, MLWriter, SchemaUtils}
import org.apache.spark.sql.Row
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.types.StructType


class RndHpCosLSH(override val uid: String) extends LSH[RndHpCosLSHModel] with HasSeed {
  override def setInputCol(value: String): this.type = super.setInputCol(value)

  override def setOutputCol(value: String): this.type = super.setOutputCol(value)

  override def setNumHashTables(value: Int): this.type = super.setNumHashTables(value)

  def setSeed(value: Long): this.type = set(seed, value)

  def this() = {
    this(Identifiable.randomUID("rhc-lsh"))
  }

  override protected[ml] def createRawLSHModel(inputDim: Int): RndHpCosLSHModel = {
    val rand = new Random($(seed))
    val randHyperplanes: Array[Vector] = {
      Array.fill($(numHashTables)) {
        val randArray = Array.fill(inputDim)({if (rand.nextGaussian() > 0) 1.0 else -1.0})
        Vectors.fromBreeze(breeze.linalg.Vector(randArray))
      }
    }
    new RndHpCosLSHModel(uid, randHyperplanes)
  }
  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    validateAndTransformSchema(schema)
  }
  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

}

class RndHpCosLSHModel private[ml](
                                override val uid: String,
                                private[ml] val hyperPlanes: Array[Vector]
                                    ) extends LSHModel[RndHpCosLSHModel] {
  override protected[ml] def hashFunction(elems: Vector): Array[Vector] = {
      require(elems.numNonzeros > 0, "Must have at least 1 non zero entry.")
      val hashValues = hyperPlanes.map(hyperPlane => if (elems.dot(hyperPlane) >= 0) 1 else -1)
      hashValues.map(Vectors.dense(_))
  }
  override protected[ml] def hashDistance(x: Array[Vector], y: Array[Vector]): Double = {
    // distance between hashes (share of unequal hashes)
    x.zip(y).map(i => if (i._1 != i._2) 1.0 else 0.0).sum / x.length
  }

  override protected[ml] def keyDistance(x: Vector, y: Vector): Double = {
    // cosine similarity (1 - cos distance)
    if (Vectors.norm(x, 2) == 0 || Vectors.norm(y, 2) == 0) 1.0
    else 1 - (x.dot(y) / Vectors.norm(x, 2) * Vectors.norm(y, 2))
  }

  override def copy(extra: ParamMap): RndHpCosLSHModel = {
    val copied = new RndHpCosLSHModel(uid, hyperPlanes).setParent(parent)
    copyValues(copied, extra)
  }

  override def write: MLWriter = {
    new RndHpCosLSHModel.RndHpCosLSHModelWriter(this)
  }
}

object RndHpCosLSHModel extends MLReadable[RndHpCosLSHModel] {

  override def read: MLReader[RndHpCosLSHModel] = new MLReader[RndHpCosLSHModel] {
    /** Checked against metadata when loading model */
    private val className = classOf[RndHpCosLSHModel].getName

    override def load(path: String): RndHpCosLSHModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
      val Row(hyperPlanes: Matrix) = MLUtils.convertMatrixColumnsToML(data, "randHyperPlanes")
        .select("randHyperPlanes")
        .head()
      val model = new RndHpCosLSHModel(metadata.uid,
        hyperPlanes.rowIter.toArray)

      metadata.getAndSetParams(model)
      model
    }
  }

  override def load(path: String): RndHpCosLSHModel = super.load(path)

  private[RndHpCosLSHModel] class RndHpCosLSHModelWriter(instance: RndHpCosLSHModel) extends MLWriter {

    private case class Data(hyperPlanes: Matrix)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val numRows = instance.hyperPlanes.length
      require(numRows > 0)
      val numCols = instance.hyperPlanes.head.size
      val values = instance.hyperPlanes.map(_.toArray).reduce(Array.concat(_, _))
      val randMatrix = Matrices.dense(numRows, numCols, values)
      val data = Data(randMatrix)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }
}