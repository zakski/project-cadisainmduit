package com.szadowsz.cadisainmduit.places.nga.commonwealth

object CarribbeanCommonwealthPreparer extends BaseNGACommonwealthPreparer {

  def main(args: Array[String]): Unit = {
    transform("./data/data/nga/carribbean","./data/results/car_placenames.csv", f => true)
  }
}
