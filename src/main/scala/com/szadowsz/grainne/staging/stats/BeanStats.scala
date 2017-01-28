package com.szadowsz.grainne.stats

import com.szadowsz.common.io.write.FWriter
import com.szadowsz.common.reflection.ReflectionUtil
import org.apache.spark.rdd.RDD._
import org.apache.spark.rdd.RDD

/**
  * Created by zakski on 30/11/2015.
  */
object BeanStats extends Serializable {

  private def mapLists(tag: String, returns: RDD[List[Any]]) = {
    val counts = returns.map(_.length)
    val all: RDD[List[String]] = returns.map(l => l.map(_.toString))
    val max = counts.max()
    var list = List[ColStats[Any]](
      new ColStats(tag + "_count", counts.countByValue.toMap),
      new ColStats(tag + "_all", all.flatMap(x => x).countByValue.toMap),
      new ColStats(tag + "_full",all.map(_.mkString(" ")).countByValue().toMap))

    for (i <- 0 until max) {
      list = list :+ new ColStats[Any](tag + "_" + i, all.filter(_.length > i).map(_ (i)).countByValue.toMap)
    }
    list
  }

  private def mapOpts(tag: String, returns: RDD[Option[Any]]) = {
    val all: RDD[Option[String]] = returns.map(l => l.map(_.toString))
    List[ColStats[Any]](new ColStats(tag, all.map(x => x.getOrElse("MISSING")).countByValue.toMap))
  }

  def calculateStats[T <: Serializable](beans: RDD[T]) = {
    val taggedVals = beans.map(b =>
      ReflectionUtil.findJavaStyleGetters(b.getClass).filter(_.getName != "getClass").map(m => (m.getName.substring(3), m.invoke(b).asInstanceOf[Any]))
    )

    val tags = taggedVals.first().map(_._1)
    val flatVals = taggedVals.flatMap(x => x)

    var results = List[ColStats[Any]]()

    tags.foreach(tag => {
      val input = flatVals.filter(_._1 == tag).map(_._2)
      input.first() match {
        case l: List[Any] => results = results ++ mapLists(tag, input.asInstanceOf[RDD[List[Any]]])
        case o: Option[Any] => results = results ++ mapOpts(tag, input.asInstanceOf[RDD[Option[Any]]])
        case a => println(tag)
      }
    })

    results
  }

  def writeHighToLow(folder : String, list : List[ColStats[Any]]): Unit = {
    list.foreach(s => {
      val writer = new FWriter(folder + s.id + ".csv",false)
      s.highToLow.foreach(l => writer.writeLine(l._1.toString + "," + l._2))
      writer.close()
    })
  }
}