package com.szadowsz.cadisainmduit.places.nga.commonwealth

object OceaniaCommonwealthPreparer extends BaseNGACommonwealthPreparer {

  def main(args: Array[String]): Unit = {
    transform("./data/data/nga/oceania","./data/results/oa_placenames.csv", f => true)
  }
}
