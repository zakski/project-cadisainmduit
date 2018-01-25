package com.szadowsz.cadisainmduit.people.census.usa.years

object UsaColdWarYearsSplicer extends UsaCensusYearSplicer {

  def main(args: Array[String]): Unit = {
    loadData(true,1945,1990)
  }
}
