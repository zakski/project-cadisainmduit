package com.szadowsz.cadisainmduit.places.nga.commonwealth

object AsiaCommonwealthPreparer extends BaseNGACommonwealthPreparer {

  def main(args: Array[String]): Unit = {
    transform("./data/data/nga/asia","./data/results/as_placenames.csv", f => true)
  }
}
