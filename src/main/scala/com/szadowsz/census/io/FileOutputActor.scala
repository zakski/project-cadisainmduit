package com.szadowsz.census.io

import akka.actor.Actor
import com.szadowsz.census.CensusDataBean

/**
  * Created by zakski on 11/11/2015.
  */
class FileOutputActor extends Actor {

    

  def receive = {
    case bean : CensusDataBean =>     println(bean.toString)

  }
}
