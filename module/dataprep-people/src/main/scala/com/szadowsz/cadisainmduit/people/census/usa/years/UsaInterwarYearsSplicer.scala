package com.szadowsz.cadisainmduit.people.census.usa.years

object UsaInterwarYearsSplicer extends UsaCensusYearSplicer {

  def main(args: Array[String]): Unit = {
    loadData(true,1918,1945)
  }
}
