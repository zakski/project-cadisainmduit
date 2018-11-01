package com.szadowz.naming.stellaris

import com.szadowz.naming.stellaris.armies.ArmyList
import com.szadowz.naming.stellaris.characters.CharList
import com.szadowz.naming.stellaris.planets.PlanList
import com.szadowz.naming.stellaris.ships.ShipList

class NameListTemplate(name : String, by : String, alias : String, rand : Boolean, ships: ShipList, fleets : FleetList, armies : ArmyList, planets : PlanList, chars : CharList) {

  def construct : String =
    s"""
      |### $name
      |### by $by
      |${alias.toUpperCase} = {
      |	randomized = $rand
      | alias = "${alias}" 
      |
      |${ships.toString}
      |
      |${fleets.toString}
      |
      |${armies.toString}
      |
      |${planets.toString}
      |
      |${chars.toString}
      |}""".stripMargin
}
