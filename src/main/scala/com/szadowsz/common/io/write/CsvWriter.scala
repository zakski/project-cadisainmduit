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
package com.szadowsz.common.io.write

import org.supercsv.io.CsvListWriter
import org.supercsv.prefs.CsvPreference

import scala.collection.JavaConverters._

/**
  * Created on 30/05/2016.
  */
class CsvWriter(path: String, encoding: String, append: Boolean, pref: CsvPreference = CsvPreference.STANDARD_PREFERENCE)
  extends CsvListWriter(new FWriter(path, append, encoding), pref) {


  def write(columns: Seq[String]): Unit = write(columns.asJava)

  def writeAll(rows: Seq[Seq[String]]): Unit = rows.foreach(write)

}