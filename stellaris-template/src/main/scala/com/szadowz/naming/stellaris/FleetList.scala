package com.szadowz.naming.stellaris

case class FleetList(random: List[String], sequential : String) {
  
  private def processList(names: List[String]): String = {
    names.map(n => if (n.matches("\\s")) {
      '"' + n + '"'
    } else {
      n
    })
      .foldLeft("|\t\t\t") { (list, n) =>
        if (list.length == 4) {
          list + n
        } else if (list.substring(Math.max(list.lastIndexOf('\n'), 0)).length + n.length + 1 > 107) {
          list + "\n|\t\t\t" + n
        } else {
          list + ' ' + n
        }
      }
  }
  
  override def toString: String = {
    s"""	fleet_names = {
       |		random_names = {
       ${processList(random)}
       |		}
       |		sequential_name = $sequential
       |	}""".stripMargin
  }
}
