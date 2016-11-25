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
package com.szadowsz.common.io.explore

import java.io.{File, FilenameFilter}


/**
  * Class is used to filter classnames by extensions
  *
  * @param ext the extension to filter by
  * @param recurse whether to look recursive through the folder
  */
class ExtensionFilter(ext: String, val recurse: Boolean) extends FilenameFilter {
  protected val extension: String = ext.substring(ext.lastIndexOf('.') + 1)


  /**
    * Tests if a specified file should be included in a file list.
    *
    * @param   directory the directory in which the file was found.
    * @param   fileName  the name of the file.
    * @return true if and only if the name should be included in the file list; false otherwise.
    */
  override def accept(directory: File, fileName: String): Boolean = {
    fileName.endsWith(extension) || (recurse && new File(directory, fileName).isDirectory)
  }
}