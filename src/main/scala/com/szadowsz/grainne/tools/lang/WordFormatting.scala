/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.szadowsz.grainne.tools.lang

import java.util.regex.Pattern

import scala.util.Try

/**
  * <p>Operations on Strings that contain words.</p>
  *
  * <p>This class tries to handle <code>null</code> input gracefully.
  * An exception will not be thrown for a <code>null</code> input.
  * Each method documents its behaviour in more detail.</p>
  *
  * @since 2.0
  */
object WordFormatting {

  private val mac = "\\bMac[A-Za-z]{2,}[^aciozj]\\b".r
  private val mc = "\\bMc".r

  private val mcAndMacMatch = "\\b(Ma?c)([A-Za-z]+)\\b".r

  private val macExceptions = List(
    "MacEdo",
    "MacEvicius",
    "MacHado",
    "MacHar",
    "MacHin",
    "MacHlin",
    "MacIas",
    "MacIulis",
    "MacKie",
    "MacKle",
    "MacKlin",
    "MacKmin",
    "MacQuarie"
  ).mkString("\\b(", "|", ")").r

  /**
    * Is the character a delimiter.
    *
    * @param ch  the character to check
    * @param delimiters  the delimiters
    * @return true if it is a delimiter
    */
  private def isDelimiter(ch: Char, delimiters: List[Char]): Boolean = {
    if (delimiters == Nil) {
      Character.isWhitespace(ch)
    } else {
      delimiters.contains(ch)
    }
  }

  private def formatSplitSurnames(delimiters: List[Char], capitalized: String): String = {
    val words = capitalized.split(delimiters.mkString("[", "", "]")).reverse.foldLeft(List[String]())((l, s) => s match {
      case "O" => ("O'" + l.head) +: l.tail
      case "Mc" | "Mac" => (s + l.head) +: l.tail
      case "Fitz" => capitalizeFully(s + l.head) +: l.tail
      case _ => s +: l
    })

    words.mkString(" ")
  }

  def formatName(name: String): String = {
    val delimiters = List(' ', '-')

    val capitalized = capitalizeFully(name, '\'' +: delimiters)

    val merge = Try(formatSplitSurnames(delimiters, capitalized))
    if (merge.isFailure) {
      capitalized
    } else {

      var formatted: String = merge.get

      if (mac.findFirstIn(formatted).isDefined || mc.findFirstIn(formatted).isDefined) {
        formatted = mcAndMacMatch.replaceAllIn(formatted, m => m.group(1) + capitalize(m.group(2)))

        formatted = macExceptions.replaceAllIn(formatted, m => capitalizeFully(m.group(1)))
      }
      formatted = formatted.replaceAll("\\bAl(?=\\s+\\w)", "al")
      formatted = formatted.replaceAll("\\b(Bin|Binti|Binte)\\b", "bin")
      formatted = formatted.replaceAll("\\bAp\\b", "ap")
      formatted = formatted.replaceAll("\\bBen(?=\\s+\\w)", "ben")
      formatted = "\bDell([ae])\b".r.replaceAllIn(formatted, g => "dell" + g.group(1))
      formatted = "\\bD([aeiou])\\b".r.replaceAllIn(formatted, g => "d" + g.group(1))
      formatted = "\\bD([ao]s)\\b".r.replaceAllIn(formatted, g => "d" + g.group(1))
      formatted = "\\bDe([lr])\\b".r.replaceAllIn(formatted, g => "de" + g.group(1))
      formatted = formatted.replaceAll("\\bEl\\b", "el")
      formatted = formatted.replaceAll("\\bLa\\b", "la")
      formatted = "\\bL([eo])\\b".r.replaceAllIn(formatted, g => "l" + g.group(1))
      formatted = formatted.replaceAll("\\bVan(?=\\s+\\w)", "van")
      formatted = formatted.replaceAll("\\bVon\\b", "von")

      formatted
    }
  }

  /**
    * <p>Capitalizes all the delimiter separated words in a String.
    * Only the first character of each word is changed. To convert the
    * rest of each word to lowercase at the same time,
    * use {@link #capitalizeFully(String, char[])}.</p>
    *
    * <p>The delimiters represent a set of characters understood to separate words.
    * The first string character and the first non-delimiter character after a
    * delimiter will be capitalized. </p>
    *
    * <p>A <code>null</code> input String returns <code>null</code>.
    * Capitalization uses the Unicode title case, normally equivalent to
    * upper case.</p>
    *
    * <pre>
    * WordUtils.capitalize(null, *)            = null
    * WordUtils.capitalize("", *)              = ""
    * WordUtils.capitalize(*, new char[0])     = *
    * WordUtils.capitalize("i am fine", null)  = "I Am Fine"
    * WordUtils.capitalize("i aM.fine", {'.'}) = "I aM.Fine"
    * </pre>
    *
    * @param str  the String to capitalize, may be null
    * @param delimiters  set of characters to determine capitalization, null means whitespace
    * @return capitalized String, <code>null</code> if null String input
    * @see #uncapitalize(String)
    * @see #capitalizeFully(String)
    * @since 2.1
    */
  def capitalize(str: String, delimiters: List[Char] = Nil): String = {
    val buffer: Array[Char] = str.toCharArray
    var capitalizeNext: Boolean = true
    str.map(ch => {
      if (isDelimiter(ch, delimiters)) {
        capitalizeNext = true
        ch
      } else if (capitalizeNext) {
        capitalizeNext = false
        Character.toTitleCase(ch)
      } else {
        ch
      }
    })
  }

  /**
    * <p>Converts all the delimiter separated words in a String into capitalized words,
    * that is each word is made up of a titlecase character and then a series of
    * lowercase characters. </p>
    *
    * <p>The delimiters represent a set of characters understood to separate words.
    * The first string character and the first non-delimiter character after a
    * delimiter will be capitalized. </p>
    *
    * <p>A <code>null</code> input String returns <code>null</code>.
    * Capitalization uses the Unicode title case, normally equivalent to
    * upper case.</p>
    *
    * <pre>
    * WordUtils.capitalizeFully(null, *)            = null
    * WordUtils.capitalizeFully("", *)              = ""
    * WordUtils.capitalizeFully(*, null)            = *
    * WordUtils.capitalizeFully(*, new char[0])     = *
    * WordUtils.capitalizeFully("i aM.fine", {'.'}) = "I am.Fine"
    * </pre>
    *
    * @param str  the String to capitalize, may be null
    * @param delimiters  set of characters to determine capitalization, null means whitespace
    * @return capitalized String, <code>null</code> if null String input
    * @since 2.1
    */
  def capitalizeFully(str: String, delimiters: List[Char] = Nil): String = {
    capitalize(str.toLowerCase, delimiters)
  }

  /**
    * <p>Extracts the initial characters from each word in the String.</p>
    *
    * <p>All first characters after the defined delimiters are returned as a new string.
    * Their case is not changed.</p>
    *
    * <p>If the delimiters array is null, then Whitespace is used.
    * Whitespace is defined by {@link Character#isWhitespace(char)}.
    * A <code>null</code> input String returns <code>null</code>.
    * An empty delimiter array returns an empty String.</p>
    *
    * <pre>
    * WordUtils.initials(null, *)                = null
    * WordUtils.initials("", *)                  = ""
    * WordUtils.initials("Ben John Lee", null)   = "BJL"
    * WordUtils.initials("Ben J.Lee", null)      = "BJ"
    * WordUtils.initials("Ben J.Lee", [' ','.']) = "BJL"
    * WordUtils.initials(*, new char[0])         = ""
    * </pre>
    *
    * @param str  the String to get initials from, may be null
    * @param delimiters  set of characters to determine words, null means whitespace
    * @return String of initial characters, <code>null</code> if null String input
    * @see #initials(String)
    * @since 2.2
    */
  def initials(str: String, delimiters: List[Char] = Nil): String = {
    var lastWasGap: Boolean = true
    var i: Int = 0
    str.filter(ch => {
      if (isDelimiter(ch, delimiters)) {
        lastWasGap = true
        false
      } else if (lastWasGap) {
        lastWasGap = false
        true
      } else {
        false
      }
    })
  }

  /**
    * <p>Checks if the String contains all words in the given array.</p>
    *
    * <p>
    * A {@code null} String will return {@code false}. A {@code null, zero
     * length search array or if one element of array is null will return {@code false}.
    * </p>
    *
    * <pre>
    * WordUtils.containsAllWords(null, *)            = false
    * WordUtils.containsAllWords("", *)              = false
    * WordUtils.containsAllWords(*, null)            = false
    * WordUtils.containsAllWords(*, [])              = false
    * WordUtils.containsAllWords("abcd", "ab", "cd") = false
    * WordUtils.containsAllWords("abc def", "def", "abc") = true
    * </pre>
    *
    *
    * @param word The str to check, may be null
    * @param words The array of String words to search for, may be null
    * @return { @code true} if all search words are found, { @code false} otherwise
    */
  def containsAllWords(word: String, words: String*): Boolean = {
    if (word.isEmpty || words.isEmpty) {
      false
    } else {
      for (w <- words) {
        if (w.isEmpty) {
          return false
        }
        val p: Pattern = Pattern.compile(".*\\b" + w + "\\b.*")
        if (!p.matcher(word).matches) {
          return false
        }
      }
      true
    }
  }
}