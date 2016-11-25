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

import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}
import java.nio.charset.Charset

import scala.util.Properties

/**
  * Companion object to the file writer.
  *
  * Created on 07/0/7/16.
  */
object FWriter {


  /**
    * Convenience Method to create the sub directories that we want to write the file to, then
    * initialise an ouput stream writer.
    *
    * @param path     the path of the file to read.
    * @param encoding the encoding of the file.
    * @param append   true if data should be appended to the end of the file, false otherwise.
    * @return a File Ouput Stream Writer
    */
  private def init(path: String, encoding: String, append: Boolean) = {
    new File(path).getParentFile.mkdirs
    new OutputStreamWriter(new FileOutputStream(path, append), Charset.forName(encoding))
  }
}

/**
  * Simple packaged File Writer Class.
  *
  * Created on 07/0/7/16.
  *
  * @param path     the path of the file to read.
  * @param encoding the encoding of the file.
  * @param append   true if data should be appended to the end of the file, false otherwise.
  */
case class FWriter(path: String, append: Boolean, encoding: String = "UTF-8") extends BufferedWriter(FWriter.init(path, encoding, append)) {

  def writeLine(line: String) {
    this.write(line + Properties.lineSeparator)
  }
}