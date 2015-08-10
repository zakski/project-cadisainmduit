package com.szadowsz.matching

import com.szadowsz.matching.LevenshteinAutomaton

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.immutable.List
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Taken from Maurício Linhares blog example and modified for String Metric Matching
 *
 * @author Maurício Linhares : 06/01/15
 * @author Zakski : 10/08/2015.
 */
object Trie {
  def apply(): Trie = new TrieNode()
}

/**
 * Taken from Maurício Linhares blog example and modified for String Metric Matching
 *
 * @author Maurício Linhares : 06/01/15
 * @author Zakski : 10/08/2015.
 */
sealed trait Trie extends Traversable[String] {

  def append(key: String)

  def findByPrefix(prefix: String): Seq[String]

  def findWithMaxDistance(word: String, maxDist: Int): Seq[(String, Int)]

  def contains(word: String): Boolean

  def remove(word: String): Boolean

}

/**
 * Taken from Maurício Linhares blog example and modified for String Metric Matching
 *
 * @author Maurício Linhares : 06/01/15
 * @author Zakski : 10/08/2015.
 */
private[collection] class TrieNode(val char: Option[Char] = None, var word: Option[String] = None) extends Trie {

  private[collection] val children: mutable.Map[Char, TrieNode] = new java.util.TreeMap[Char, TrieNode]().asScala

  override def append(key: String) = {

    @tailrec def appendHelper(node: TrieNode, currentIndex: Int): Unit = {
      if (currentIndex == key.length) {
        node.word = Some(key)
      } else {
        val char = key.charAt(currentIndex).toLower
        val result = node.children.getOrElseUpdate(char, {
          new TrieNode(Some(char))
        })

        appendHelper(result, currentIndex + 1)
      }
    }

    appendHelper(this, 0)
  }

  override def foreach[U](f: String => U): Unit = {

    @tailrec def foreachHelper(nodes: TrieNode*): Unit = {
      if (nodes.size != 0) {
        nodes.foreach(node => node.word.foreach(f))
        foreachHelper(nodes.flatMap(node => node.children.values): _*)
      }
    }

    foreachHelper(this)
  }

  override def findByPrefix(prefix: String): scala.collection.Seq[String] = {

    @tailrec def helper(currentIndex: Int, node: TrieNode, items: ListBuffer[String]): ListBuffer[String] = {
      if (currentIndex == prefix.length) {
        items ++ node
      } else {
        node.children.get(prefix.charAt(currentIndex).toLower) match {
          case Some(child) => helper(currentIndex + 1, child, items)
          case None => items
        }
      }
    }

    helper(0, this, new ListBuffer[String]())
  }

  override def contains(word: String): Boolean = {

    @tailrec def helper(currentIndex: Int, node: TrieNode): Boolean = {
      if (currentIndex == word.length) {
        node.word.isDefined
      } else {
        node.children.get(word.charAt(currentIndex).toLower) match {
          case Some(child) => helper(currentIndex + 1, child)
          case None => false
        }
      }
    }

    helper(0, this)
  }

  override def remove(word: String): Boolean = {

    pathTo(word) match {
      case Some(path) => {
        var index = path.length - 1
        var continue = true

        path(index).word = None

        while (index > 0 && continue) {
          val current = path(index)

          if (current.word.isDefined) {
            continue = false
          } else {
            val parent = path(index - 1)

            if (current.children.isEmpty) {
              parent.children.remove(word.charAt(index - 1).toLower)
            }

            index -= 1
          }
        }

        true
      }
      case None => false
    }

  }

  private[collection] def pathTo(word: String): Option[ListBuffer[TrieNode]] = {

    def helper(buffer: ListBuffer[TrieNode], currentIndex: Int, node: TrieNode): Option[ListBuffer[TrieNode]] = {
      if (currentIndex == word.length) {
        node.word.map(word => buffer += node)
      } else {
        node.children.get(word.charAt(currentIndex).toLower) match {
          case Some(found) => {
            buffer += node
            helper(buffer, currentIndex + 1, found)
          }
          case None => None
        }
      }
    }

    helper(new ListBuffer[TrieNode](), 0, this)
  }

  override def toString(): String = s"Trie(char=${char},word=${word})"

  override def findWithMaxDistance(word: String, maxDist: Int): Seq[(String, Int)] = {
    val automaton = new LevenshteinAutomaton(word, maxDist)

    def helper(state: (List[Int], List[Int]), node: TrieNode, items: ListBuffer[(String, Int)]): ListBuffer[(String, Int)] = {
      if (automaton.isMatch(state) && node.word.isDefined) {
        val matched = (node.word.get, state._2.last)
        items += matched
      }

      val states = node.children.map(keyValue => keyValue._1 -> automaton.step(state, keyValue._1))
      val valid = states.filter(keyState => automaton.canMatch(keyState._2))

      valid.foreach(keyState => helper(keyState._2, node.children.get(keyState._1).orNull, items))
      items
    }

    helper(automaton.start(), this, ListBuffer[(String, Int)]()).sortBy(_._2)
  }
}