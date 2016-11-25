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
package com.szadowsz.common.net

import functions._

/**
  * Created on 15/05/2016.
  */
private[net] object PercentEncoder {
  private val alpha = "[a-zA-Z]"
  private val digit = "\\d"
  private val unreserved = "[" + alpha + digit + "\\.-_~]"
  private val reserved = "[\\?\\*\\+\\(\\)\\$:/#\\[\\]@!&',;=]"

  private val alphaR = alpha.r
  private val digitR = digit.r
  private val unreservedR = unreserved.r
  private val reservedR = reserved.r
  private val unencodeable = ("(" + unreserved + "|" + reserved + ")").r

  private def shouldEncode(ch: Char): Boolean = !unencodeable.matches(ch)

  private def encode(ch: Char): String = "%" + "%04X".format(ch.toInt).substring(2)

  def encode(s: String): String = {
    val chars = s.getBytes("UTF-8").map(_.toChar)

    val encChars = chars.flatMap(ch => {
      if (shouldEncode(ch)) {
        encode(ch).getBytes("UTF-8")
      } else {
        Array(ch.toByte)
      }
    })

    new String(encChars, "UTF-8")
  }
}
