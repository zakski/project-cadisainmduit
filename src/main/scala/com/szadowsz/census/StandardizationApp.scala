package com.szadowsz.census

import _root_.akka.actor.{ActorSystem, Props}
import com.szadowsz.census.io.{FileOutputActor, FileInputActor}
import com.szadowsz.census.mapping.SurnameOrigins
import org.slf4j.LoggerFactory

/**
 * @author Zakski : 16/09/2015.
 */
object StandardizationApp extends App {
  private val _logger = LoggerFactory.getLogger(StandardizationApp.getClass)

  _logger.info("Initialising Standardization")
  SurnameOrigins.init

  // census files are stored as csvs so focus on that
  val filter = ExtensionFilter(".csv",true)

  // Create the 'census' actor system
  val system = ActorSystem("census")


  _logger.info("Preparing Actor System")
  val reader = system.actorOf(Props(classOf[FileInputActor],"./data/census/", "UTF-8", filter).withMailbox("akka.actor.priority-mailbox"))
  val writer = system.actorOf(Props(classOf[FileOutputActor]))
  val workers = Vector.fill(4)(system.actorOf(Props(classOf[CensusActor])))
  reader ! writer
  workers.foreach {_ ! (reader,writer)}
  _logger.info("Started Actor System")
}
