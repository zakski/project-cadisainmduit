// Copyright 2016 zakski.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.szadowsz.common.io.read

import org.supercsv.io.CsvListReader
import org.supercsv.prefs.CsvPreference

import scala.collection.JavaConverters._

/**
  * Bundled csv file reader to simplify the reading of csvs.
  *
  * Created on 30/05/2016.
  *
  * @param path the path of the file to read.
  * @param encoding the encoding of the file.
  * @param pref the expected csv file settings.
  */
class CsvReader(path: String, encoding: String = "UTF-8", pref: CsvPreference = CsvPreference.STANDARD_PREFERENCE)
  extends CsvListReader(new FReader(path, encoding), pref) {

  /**
    * Reads the next line of the file
    *
    * @return the line that was read
    */
  def readLineOpt(): Option[Seq[String]] = {
    this.read() match {
      case jList: java.util.List[String] => Option(jList.asScala.toList)
      case _ =>
        close()
        None
    }
  }

  /**
    * Method to read all lines in the csv at once and return them in a list.
    *
    * @return list of csv rows.
    */
  def readAll(): Seq[Seq[String]] = {
    var result = List[Seq[String]]()
    var opt = readLineOpt()
    while (opt.isDefined) {
      result = result :+ opt.get
      opt = readLineOpt()
    }
    result
  }


}