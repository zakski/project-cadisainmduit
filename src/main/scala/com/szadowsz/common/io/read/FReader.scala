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

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.nio.charset.Charset

/**
  * Simple packaged File Reader Class.
  *
  * Created on 04/07/16.
  *
  * @param path the path of the file to read.
  * @param encoding the encoding of the file.
  */
class FReader(path : String, encoding: String = "UTF-8")
  extends BufferedReader(new InputStreamReader(new FileInputStream(new File(path)), Charset.forName(encoding))){


  /**
    * Reads the next line of the file
    *
    * @return the line that was read
    */
  def readLineOpt(): Option[String] = {
    readLine match {
      case line : String => Some(line)
      case _ =>
        close()
        None
    }
  }
}