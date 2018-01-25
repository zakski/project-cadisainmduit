package com.szadowsz.spark.ml

import com.szadowsz.common.reflection.ReflectionUtil
import org.apache.spark.ml._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.mutable.ListBuffer

/**
  * Extended [[Pipeline]] Class to keep track of columns and stages, provide enhanced debugging and optional optimisation
  * of the pipeline stages based on a user's expected/preferred output.
  *
  * Created on 09/05/2016.
  *
  * @param uid unique identifier for the constructed lineage
  */
class Lineage(uid: String) extends Pipeline(uid) {

  def this() = this(Identifiable.randomUID("lineage"))

  def addStage(stage: Class[_ <: PipelineStage], params: (String, Any)*): this.type = {
    addStage(stage, params.toMap)
  }

  def addStage(stage: Class[_ <: PipelineStage], params: Map[String, Any]): this.type = {
    val structr = ReflectionUtil.findConstructor(stage, classOf[String])
    val count = if (isDefined(stages)) $(stages).length else 0
    val id = s"$uid-$count"
    val inst = structr.newInstance(id).asInstanceOf[PipelineStage]
    params.foreach { case (k, v) => inst.set(inst.getParam(k), v) }
    addStage(inst)
  }

  def addPassThroughTransformer(stage: Class[_ <: Transformer], params: Map[String, Any]): this.type = {
    val structr = ReflectionUtil.findConstructor(stage, classOf[String])
    val count = if (isDefined(stages)) $(stages).length else 0
    val id = s"$uid-$count"
    val inst = structr.newInstance(id).asInstanceOf[Transformer]
    params.foreach { case (k, v) => inst.set(inst.getParam(k), v) }
    addPassThroughTransformer(inst)
  }

  def addStage(stage: PipelineStage): this.type = {
    val nstages: Array[PipelineStage] = if (isDefined(stages)) $(stages) else Array()
    setStages(nstages :+ stage)
  }

  def addPassThroughTransformer(stage: Transformer): this.type = {
    val count = if (isDefined(stages)) $(stages).length else 0
    val id = s"$uid-$count"
    val inst = PassThroughTransformer(id, stage)
    addStage(inst)
  }
  /**
    * Fits the pipeline to the input dataset with additional parameters. If a stage is an
    * [[Estimator]], its `Estimator.fit` method will be called on the input dataset to fit a model.
    * Then the model, which is a transformer, will be used to transform the dataset as the input to
    * the next stage. If a stage is a [[Transformer]], its `Transformer.transform` method will be
    * called to produce the dataset for the next stage. The fitted model from a [[Pipeline]] is an
    * [[PipelineModel]], which consists of fitted models and transformers, corresponding to the
    * pipeline stages. If there are no stages, the output model acts as an identity transformer.
    *
    * @param dataset input dataset
    * @return fitted pipeline
    */
  override def fit(dataset: Dataset[_]): PipelineModel = {
    transformSchema(dataset.schema, logging = true)
    val theStages = $(stages)
    // Search for the last estimator.
    var indexOfLastEstimator = -1
    theStages.view.zipWithIndex.foreach { case (stage, index) =>
      stage match {
        case _: Estimator[_] =>
          indexOfLastEstimator = index
        case _ : PassThroughTransformer =>
          indexOfLastEstimator = index
        case _ =>
      }
    }
    var curDataset = dataset
    val transformers = ListBuffer.empty[Transformer]
    theStages.view.zipWithIndex.foreach { case (stage, index) =>
      if (index <= indexOfLastEstimator) {
        val transformer = stage match {
          case estimator: Estimator[_] =>
            estimator.fit(curDataset)
          case t: Transformer =>
            t
          case _ =>
            throw new IllegalArgumentException(
              s"Does not support stage $stage of type ${stage.getClass}")
        }
        if (index <= indexOfLastEstimator) {
          curDataset = transformer.transform(curDataset)
        }
        if (!transformer.isInstanceOf[PassThroughTransformer]) {
          transformers += transformer
        }
      } else {
         transformers += stage.asInstanceOf[Transformer]
      }
    }
    new Pipeline().setStages(transformers.toArray).fit(dataset).setParent(this)
  }

  def fitAndTransform(dataset: Dataset[_]): (PipelineModel, DataFrame) = {
    transformSchema(dataset.schema, logging = true)
    val theStages = $(stages)

    var curDataset = dataset
    val transformers = ListBuffer.empty[Transformer]
    $(stages).view.foreach { case (stage) =>
      val transformer = stage match {
        case estimator: Estimator[_] => estimator.fit(curDataset)
        case t: Transformer => t
        case _ => throw new IllegalArgumentException(s"Does not support stage $stage of type ${stage.getClass}")
      }
      curDataset = transformer.transform(curDataset)
      if (!transformer.isInstanceOf[PassThroughTransformer]) {
        transformers += transformer
      }
    }

    (new Pipeline().setStages(transformers.toArray).fit(dataset).setParent(this), curDataset.toDF())
  }
}
