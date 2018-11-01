package com.szadowsz.cadisainmduit.places.nga.commonwealth

object AfricaCommonwealthPreparer extends BaseNGACommonwealthPreparer {

  def main(args: Array[String]): Unit = {
    transform("./data/data/nga/africa","./data/results/af_placenames.csv", f => true)
  }
}
