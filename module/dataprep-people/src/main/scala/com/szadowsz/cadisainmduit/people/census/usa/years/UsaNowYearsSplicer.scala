package com.szadowsz.cadisainmduit.people.census.usa.years

object UsaNowYearsSplicer extends UsaCensusYearSplicer {

  def main(args: Array[String]): Unit = {
    loadData(true,1990,2050)
  }
}
