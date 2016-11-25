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
  * Created on 26/04/2016.
  */
object WordTokeniser {

  /**
    * Private recursive function to turn the character sequence into tokens.
    *
    * @param s1 remaining list of characters to parse.
    * @param buffer the accumulator for the current token.
    * @param delims the list of deliminators to check for.
    * @return the list of tokenised strings.
    */
  private def tokenise(s1: List[Char], buffer: StringBuilder, delims: List[Char]): List[String] = {
    s1 match {
      case Nil =>
        List[String](buffer.toString)

      case delimiter :: tail if delims.contains(delimiter) =>
        buffer.toString() +: tokenise(tail, new StringBuilder, delims)

      case head :: tail =>
        buffer.append(head)
        tokenise(tail, buffer, delims)
    }
  }

  /**
    * Utility function to tokenise the string according to the optional list of deliminators.
    *
    * @param s1 the character sequence to split into tokens.
    * @param delimiters the list of deliminators to check for, defaults to checking for spaces.
    * @return the list of tokenised strings.
    */
  def tokenise(s1: CharSequence, delimiters: List[Char] = List(' ')): List[String] = {
    tokenise(s1.toString.toList, new StringBuilder, delimiters)
  }
}
