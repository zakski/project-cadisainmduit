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
package com.szadowsz.grainne.distance

/**
  * A similarity algorithm indicating the percentage of matched characters between two character sequences.
  *
  * <p>
  * The Jaro measure is the weighted sum of percentage of matched characters
  * from each file and transposed characters. Winkler increased this measure
  * for matching initial characters.
  * </p>
  *
  * <p>
  * This implementation is based on the Jaro Winkler similarity algorithm
  * from <a href="http://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance">
  * http://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance</a>.
  * </p>
  *
  * <p>
  * This code has been adapted from Apache Commons Lang 3.3.
  * </p>
  */
object JaroWrinklerDistance {
  val defaultScalingFactor: Double = 0.1
  val percentageRoundValue: Double = 100.0

  /**
    * The default prefix length limit set to four.
    */
  private val PREFIX_LENGTH_LIMIT: Int = 4
  /**
    * Represents a failed index search.
    */
  val INDEX_NOT_FOUND: Int = -1

  /**
    * Calculates the number of characters from the beginning of the strings
    * that match exactly one-to-one, up to a maximum of four (4) characters.
    *
    * @param first The first string.
    * @param second The second string.
    * @return A number between 0 and 4.
    */
  private def getCommonPrefixLength(first: String, second: String): Int = {
    val result = first.zip(second).takeWhile(Function.tupled(_ == _)).map(_._1).mkString.length

    // Limit the result to 4.
    Math.min(result, PREFIX_LENGTH_LIMIT)
  }

  /**
    * Method to find the Jaro-Winkler distance between two Strings.
    *
    * @param first - the first CharSequence to check, must not be null
    * @param second - the second CharSequence to check, must not be null
    * @throws IllegalArgumentException if either String input { @code null}
    */
  def distance(first: CharSequence, second: CharSequence): Double = {
    if (first == null || second == null) {
      throw new IllegalArgumentException("Arguments must not be null")
    }

    val jaro: Double = JaroDistance.distance(first, second)
    val cl: Int = getCommonPrefixLength(first.toString, second.toString)

    (jaro + defaultScalingFactor * cl * (1.0 - jaro)) * percentageRoundValue.round / percentageRoundValue
  }
}