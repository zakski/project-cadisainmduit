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
package com.szadowsz.common.io.zip

import java.io.{BufferedOutputStream, File, FileInputStream, FileOutputStream}
import java.util.zip.{ZipEntry, ZipFile, ZipOutputStream}
import scala.collection.JavaConverters._

/**
  * Created on 29/10/2016.
  */
object ZipperUtil {


  private def zipRecurse(current : File, base : File, zos : ZipOutputStream): Unit = {
    val files = current.listFiles()


    files.foreach{f =>
      if (f.isDirectory) {
        zipRecurse(f, base, zos)
      } else {
        val reader = new FileInputStream(f)
        val writer = new ZipEntry(f.getPath.substring(base.getPath.length() + 1))
        zos.putNextEntry(writer)

        var buffer = new Array[Byte](8192)
        var redd = reader.read(buffer)
        do {
          zos.write(buffer, 0, redd)
          redd = reader.read(buffer)
        } while (redd != -1)

        reader.close()
      }
    }
  }


  def zip(directory: File, zipTo: File): Unit = {
    val zos = new ZipOutputStream(new FileOutputStream(zipTo))
    zipRecurse(directory, directory, zos)
    zos.close()
  }

  def unzip(zip: File, extractTo: File): Unit = {
    val archive = new ZipFile(zip)
    archive.entries().asScala.foreach { entry =>
      val file = new File(extractTo, entry.getName)
      if (entry.isDirectory && !file.exists()) {
        file.mkdirs()
      } else {
        if (!file.getParentFile.exists())file.getParentFile.mkdirs()
        val reader = archive.getInputStream(entry)
        val writer = new BufferedOutputStream(new FileOutputStream(file))

        var buffer = new Array[Byte](8192)
        var redd = reader.read(buffer)
        do {
          writer.write(buffer, 0, redd)
          redd = reader.read(buffer)
        } while (redd != -1)
        reader.close()
        writer.close()
      }
    }
  }

  def unzip(zip: String, extractTo: String): Unit = {
    unzip(new File(zip), new File(extractTo))
  }
}
