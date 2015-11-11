package com.szadowsz.util

import java.io.{File, FilenameFilter}

/**
 * Utility class to perform file finding drudgery tasks.
 *
 * @author Zakski : 16/09/2015.
 */
object FileFinder {

  /**
   * Method to recursively get the list of files under the specified path.
   *
   * @param directoryPath - the file path directory string to look at
   * @return an array of found files
   */
  def search(directoryPath: String): Array[File] = search(new File(directoryPath), null)

  /**
   * Method to recursively get the list of files under the specified path.
   *
   * @param directory - the file directory to look at
   * @return an array of found files
   */
  def search(directory: File): Array[File] = search(directory, null)

  /**
   * Method to get the list of files under the specified path, recursively if possible.
   *
   * @param directoryPath - the file path directory string to look at
   * @param filter - the file filter
   * @return the list of found files
   */
  def search(directoryPath: String, filter: FilenameFilter): Array[File] = search(new File(directoryPath), filter)
  
  
  /**
   * Method to get the list of files under the specified path, recursively if possible.
   *
   * @param directory - the file directory to look at
   * @param filter - the file filter
   * @return an array of found files
   */
  def search(directory: File, filter: FilenameFilter): Array[File] = {
    directory.listFiles(filter).flatMap(file => if (file.isDirectory) search(file, filter) else List(file))
  }
}
