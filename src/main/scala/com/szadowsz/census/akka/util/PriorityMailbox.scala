package com.szadowsz.census.akka.util

import akka.actor.{PoisonPill, ActorSystem}
import akka.dispatch.PriorityGenerator
import akka.dispatch.UnboundedStablePriorityMailbox
import com.typesafe.config.Config

/**
 *  We inherit, in this case, from UnboundedStablePriorityMailbox and seed it with the priority generator
 *
 * @author Zakski : 17/09/2015.
 */
class PriorityMailbox(settings: ActorSystem.Settings, config: Config)
  extends UnboundedStablePriorityMailbox(
    // Create a new PriorityGenerator, lower prio means more important
    PriorityGenerator {
      // PoisonPill when no other left
      case PoisonPill    => 1

      // We default to 0
      case otherwise     => 0
    })

