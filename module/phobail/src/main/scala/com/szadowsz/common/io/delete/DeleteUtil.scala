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
package com.szadowsz.common.io.delete

import java.io.File

import scala.util.{Success, Try}

/**
  * Created on 04/01/2017.
  */
object DeleteUtil {

  /**
    * Recursive deletion of a folder.
    *
    * @param file the file/folder to delete.
    */
  def delete(file: File): Unit = {
    val recurse = if (file.isDirectory) Try(file.listFiles.toList.foreach(delete)) else Success()

    if (recurse.isSuccess) {
      file.delete
    } else {
      throw recurse.failed.get
    }
  }
}
