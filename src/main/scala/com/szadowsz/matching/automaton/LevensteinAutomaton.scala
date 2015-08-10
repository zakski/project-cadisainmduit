package com.szadowsz.matching.automaton

/**
 * @author Zakski : 10/08/2015.
 */
class LevenshteinAutomaton(private val seq: String, private val maxEdits: Int) {

  /**
   * Method to Generate the initial starting state
   */
  def start() = (List.range(0, maxEdits + 1), List.range(0, maxEdits + 1))


  /**
   * Method to check if the state matches
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
   */
  def is_match(state: (List[Int], List[Int])) =  state._1.nonEmpty && state._1.last == seq.length

  /**
   * Method to check if the state matches
   */
  def can_match(state: (List[Int], List[Int])) = state._1.nonEmpty

}
