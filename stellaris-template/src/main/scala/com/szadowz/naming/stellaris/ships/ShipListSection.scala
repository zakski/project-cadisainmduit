package com.szadowz.naming.stellaris.ships

case class ShipListSection(name: ShipType, random: List[String]) {
  
  private def processList(names: List[String]): String = {
    names.map(n => if (n.matches("\\s")) {
      '"' + n + '"'
    } else {
      n
    })
      .foldLeft("|\t") { (list, n) =>
        if (list.length == 2) {
          list + n
        } else if (list.substring(Math.max(list.lastIndexOf('\n'), 0)).length + n.length + 1 > 108) {
          list + "\n|\t" + n
        } else {
          list + ' ' + n
        }
      }
  }
  
  override def toString: String = {
    s"""$name = {
       |${processList(random)}
       |}""".stripMargin
  }
}
