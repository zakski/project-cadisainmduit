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
package com.szadowsz.common.lang

/**
  * Small Utility to capitalise characters in a string
  *
  * Created on 22/04/2016.
  */
object WordFormatter {

  /**
    * Private recursive function to capitalise each character in turn.
    *
    * @param s1 remaining list of characters to capitalise.
    * @param delims the list of deliminators to check for.
    * @param cap whether or not we are expecting to capitialise the next character.
    * @return the capitalised string.
    */
  private def capitalise(s1: List[Char], delims: List[Char], cap: Boolean): String = {
    s1 match {
      case Nil => ""
      case de :: tail if delims.contains(de) => de + capitalise(tail, delims, true)
      case _ => (if (cap) s1.head.toTitleCase else s1.head) + capitalise(s1.tail, delims, false)
    }
  }

  /**
    * Utility function to capitalise the string according to the optional list of deliminators.
    *
    * @param s1 the character sequence to capitalise.
    * @param delimiters the list of deliminators to check for, defaults to checking for spaces.
    * @param rmExistCaptsFlag flag to determine whether we should keep the existing capital letters.
    * @return the capitalised string.
    */
  def capitalise(s1: CharSequence, delimiters: List[Char] = List(' '), rmExistCaptsFlag: Boolean = true): String = {
    val chList = (if (rmExistCaptsFlag) s1.toString.toLowerCase else s1.toString).toList
    capitalise(chList, delimiters, true)
  }
}
