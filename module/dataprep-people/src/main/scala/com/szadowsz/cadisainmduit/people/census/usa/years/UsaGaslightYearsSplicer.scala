package com.szadowsz.cadisainmduit.people.census.usa.years

object UsaGaslightYearsSplicer extends UsaCensusYearSplicer {

  def main(args: Array[String]): Unit = {
    loadData(true,1800,1918)
  }
}
