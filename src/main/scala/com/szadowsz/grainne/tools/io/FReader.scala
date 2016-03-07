package com.szadowsz.grainne.tools.io

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.nio.charset.Charset

/**
  * Simple Constructor that sets key variables of the reader
  *
  * @param file the filepath to read
  * @param encoding the encoding of the file
  */
class FReader(file: File, encoding: String = "UTF-8") {

  protected val _encoding: Charset = Charset.forName(encoding)

  protected val _file: FileInputStream = new FileInputStream(file)

  protected val _buffer: BufferedReader = new BufferedReader(new InputStreamReader(_file, _encoding))

  /**
    * Reads the next line of the file
    *
    * @return the line that was read
    */
  def readLine(): Option[String] = {
    _buffer.readLine match {
      case line : String => Some(line)
      case _ =>
        close()
        None
    }
  }

  /**
    * Closes the file reader connection
    */
  def close() = _buffer.close()

}