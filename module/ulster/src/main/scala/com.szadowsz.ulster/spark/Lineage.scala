package com.szadowsz.ulster.spark

import com.szadowsz.common.reflection.ReflectionUtil
import com.szadowsz.ulster.spark.transformers.util.PassThroughTransformer
import org.apache.spark.ml.param.{Param, ParamPair, ParamValidators}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, Transformer}
import org.apache.spark.sql.Dataset

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
}
