package com.szadowsz.census.io

import java.io.{File, FilenameFilter}


object ExtensionFilter {
  
  def apply(extension: String, isRecursive : Boolean):ExtensionFilter = {
    if (isRecursive) new ExtensionRFilter(extension) else new ExtensionFilter(extension)
  } 
}

sealed class ExtensionFilter(extension: String) extends FilenameFilter {
  protected var _ext: String = extension.substring(extension.lastIndexOf('.') + 1)

  def accept(directory: File, fileName: String): Boolean = fileName.endsWith(_ext)
}

private class ExtensionRFilter(extension: String) extends ExtensionFilter(extension) {

  override def accept(directory: File, fileName: String): Boolean = {
    super.accept(directory, fileName) || new File(directory, fileName).isDirectory
  }
}