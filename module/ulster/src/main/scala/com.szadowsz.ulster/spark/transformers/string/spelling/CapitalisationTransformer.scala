package com.szadowsz.ulster.spark.transformers.string.spelling

import com.szadowsz.common.lang.WordFormatter
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.{Param, ParamValidators}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{DataType, StringType}
import org.slf4j.LoggerFactory

/**
  * Created on 20/04/2016.
  */
class CapitalisationTransformer(override val uid: String) extends UnaryTransformer[String, String, CapitalisationTransformer] {
  protected val logger = LoggerFactory.getLogger("com.szadowsz.ulster.spark")
  protected val delimiters: Param[List[Char]] = new Param(this, "delimiters", "word token separators")

  protected val removeExistingCapitals: Param[Boolean] = new Param(this, "removeExistingCapitals", "")

  protected val mode: Param[String] = new Param(this, "mode", "",ParamValidators.inArray(Array("default","all")))

  setDefault(delimiters -> List(' '), removeExistingCapitals -> true, mode -> "default")

  def this() = this(Identifiable.randomUID("capT"))

  override protected def createTransformFunc: (String) => String = {
    $(mode) match {
      case "all" =>
        (n: String) => {
          Option(n) match {
            case Some(s) => s.toUpperCase
            case None => null
          }
        } case _ =>
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


    override def transform(dataset: Dataset[_]): DataFrame =
    {
      logger.info("Executing stage {}", uid)
      logger.debug("Processing dataset {}", dataset.schema.fieldNames.mkString("[", ",", "]"))
      super.transform(dataset)
    }
  }