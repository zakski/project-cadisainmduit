package com.szadowsz.matching

/**
 * Levenshtein Automaton ported from Python Code found here http://julesjacobs.github.io/2015/06/17/disqus-levenshtein-simple-and-fast.html
 *
 * @author Zakski : 10/08/2015.
 */
class LevenshteinAutomaton(private val seq: String, private val maxEdits: Int) {

  /**
   * Method to Generate the initial starting state
   *
   * @return the initial state
   */
  def start() = (List.range(0, maxEdits + 1), List.range(0, maxEdits + 1))


  /**
   * Method to step to the next state given the current state and the next character. It basically returns the
   * next row of the Levenshtein matrix filtered by the max edit distance.
   *
   * @param state the current state
   * @param token the next character
   * @return the next state
   */
  def step(state: (List[Int], List[Int]), token: Char) = {
    val indices = state._1
    val values = state._2

    var newIndices = List[Int]()
    var newValues = List[Int]()

    if (indices.nonEmpty && indices.head == 0 && values.head < maxEdits) {
      newIndices = List(0)
      newValues = List(values.head + 1)
    }

    // TODO turn into tail recursive statement
    for ((i,j) <- indices.zipWithIndex) {
      if (i < seq.length) {
        val cost = if (seq(i) == token) 0 else 1
        var value = values(j) + cost

        if (newIndices.nonEmpty && newIndices.last == i) {
          value = Math.min(value, newValues.last + 1)
        }

        if (j + 1 < indices.length && indices(j + 1) == i + 1) {
          value = Math.min(value, values(j + 1) + 1)
        }

        if (value <= maxEdits) {
          newIndices = newIndices :+ (i + 1)
          newValues = newValues :+ value
        }
      }
    }
    (newIndices, newValues)
  }

  /**
   * Method to check if the state matches
   *
   * @param state the current state
   * @return true if it is a match, false otherwise
   */
  def isMatch(state: (List[Int], List[Int])) =  state._1.nonEmpty && state._1.last == seq.length

  /**
   * Method to check if the state can still match
   *
   * @param state the current state
   * @return true if it still possible to have a match, false otherwise
   */
  def canMatch(state: (List[Int], List[Int])) = state._1.nonEmpty

}
