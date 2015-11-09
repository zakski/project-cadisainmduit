package com.szadowsz.census

import _root_.akka.actor.{ActorSystem, Props}
import com.szadowsz.census.akka.io.FileActor
import com.szadowsz.census.akka.process.{BeginProcessing, CensusWorker}
import com.szadowsz.census.io.ExtensionFilter

/**
 * @author Zakski : 16/09/2015.
 */
object CensusApp extends App {

  // census files are stored as csvs so focus on that
  val filter = ExtensionFilter(".csv",true)

  // Create the 'census' actor system
  val system = ActorSystem("census")


  val reader = system.actorOf(Props(new FileActor("./data/census/", "UTF-8", filter)).withMailbox("akka.actor.priority-mailbox"))
  val workers = Vector.fill(4)(system.actorOf(Props(new CensusWorker(reader))))
  workers.foreach {_ ! BeginProcessing}
}
