package com.szadowsz.spark.ml.feature

import com.szadowsz.common.lang.WordFormatter
import com.szadowsz.spark.ml.OutOptUnaryTransformer
import org.apache.spark.ml.param.{Param, ParamValidators}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{DataType, StringType}

/**
  * Created on 20/04/2016.
  */
class StringCapitaliser(id: String) extends OutOptUnaryTransformer[String, StringCapitaliser](id) {
  protected val delimiters: Param[List[Char]] = new Param(this, "delimiters", "word token separators")

  protected val removeExistingCapitals: Param[Boolean] = new Param(this, "removeExistingCapitals", "")

  protected val mode: Param[String] = new Param(this, "mode", "", ParamValidators.inArray(Array("default", "all")))

  setDefault(delimiters -> List(' '), removeExistingCapitals -> true, mode -> "default")

  def this() = this(Identifiable.randomUID("strCapitalise"))

  override protected def createTransformFunc: (String) => String = {
    $(mode) match {
      case "all" =>
        (n: String) => {
          Option(n) match {
            case Some(s) => s.toUpperCase
            case None => null
          }
        }
      case _ =>
        (n: String) => {
          Option(n) match {
            case Some(s) => WordFormatter.capitalise(s, $(delimiters), $(removeExistingCapitals))
            case None => null
          }
        }
    }
  }

  override protected def outputDataType: DataType = StringType

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }

  def setDelimiters(delims: List[Char]): this.type = set(delimiters, delims)

  def getDelimiters: List[Char] = $(delimiters)

  def setRmExistCaptsFlag(flag: Boolean): this.type = set(removeExistingCapitals, flag)

  def getRmExistCaptsFlag: Boolean = $(removeExistingCapitals)
}