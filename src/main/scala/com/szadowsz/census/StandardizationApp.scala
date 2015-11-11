package com.szadowsz.census

import _root_.akka.actor.{ActorSystem, Props}
import com.szadowsz.census.io.{FileOutputActor, FileInputActor}
import com.szadowsz.census.mapping.SurnameOrigins

/**
 * @author Zakski : 16/09/2015.
 */
object StandardizationApp extends App {
  SurnameOrigins.init

  // census files are stored as csvs so focus on that
  val filter = ExtensionFilter(".csv",true)

  // Create the 'census' actor system
  val system = ActorSystem("census")


  val reader = system.actorOf(Props(new FileInputActor("./data/census/", "UTF-8", filter)).withMailbox("akka.actor.priority-mailbox"))
  val writer = system.actorOf(Props(new FileOutputActor()))
  val workers = Vector.fill(4)(system.actorOf(Props(new CensusActor(reader,writer))))
  workers.foreach {_ ! BeginProcessing}
}
