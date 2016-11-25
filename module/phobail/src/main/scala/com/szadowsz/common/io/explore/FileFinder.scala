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
  * Utility class to perform file finding drudgery tasks.
  *
  * @author Zakski : 16/09/2015.
  */
object FileFinder {

  /**
    * Method to get the list of files under the specified path, recursively if the filter allows it.
    *
    * @param dir    the directory to look at.
    * @param filter optional file filter, null if not needed.
    * @return an array of found files.
    */
  private def searchRecursively(dir: File, filter: FilenameFilter): Array[File] = {
    dir.listFiles(filter).flatMap(file => if (file.isDirectory) searchRecursively(file, filter) else List(file))
  }

  /**
    * Method to get the list of files under the specified path, recursively if possible.
    *
    * @param directoryPath the file path directory string to look at.
    * @param filter        optional file filter.
    * @return the list of found files.
    */
  def search(directoryPath: String, filter: Option[FilenameFilter]): Array[File] = search(new File(directoryPath), filter)


  /**
    * Method to get the list of files under the specified path, recursively if possible.
    *
    * @param directory the file directory to look at.
    * @param filter    optional file filter.
    * @return an array of found files.
    */
  def search(directory: File, filter: Option[FilenameFilter]): Array[File] = {
    if (directory.exists() && directory.isDirectory) {
      searchRecursively(directory, filter.orNull)
    } else {
      Array()
    }
  }
}