package com.szadowsz.util

import _root_.akka.actor.{ActorSystem, PoisonPill}
import _root_.akka.dispatch.{PriorityGenerator, UnboundedStablePriorityMailbox}
import com.typesafe.config.Config


object PriorityMailbox {

  /**
    * Creates a new PriorityGenerator, the lower the number, the more important the message.
    *
    * @return a new Priority Generator Instance.
    */
  private def initPriorityGen() = {
    PriorityGenerator {
      // PoisonPill when no other messages are left.
      case PoisonPill => 1

      // We default to 0
      case otherwise => 0
    }
  }
}

/**
  * We inherit, in this case, from UnboundedStablePriorityMailbox and seed it with the priority generator
  *
  * @author Zakski : 17/09/2015.
  */
class PriorityMailbox(settings: ActorSystem.Settings, config: Config)
  extends UnboundedStablePriorityMailbox(PriorityMailbox.initPriorityGen())

