package com.szadowz.naming.stellaris.armies

case class ArmyListSection(name: ArmyType, random: List[String], sequential : String) {
  
  private def processList(names: List[String]): String = {
    names.map(n => if (n.matches("\\s")) {
      '"' + n + '"'
    } else {
      n
    })
      .foldLeft("|\t\t") { (list, n) =>
        if (list.length == 3) {
          list + n
        } else if (list.substring(Math.max(list.lastIndexOf('\n'), 0)).length + n.length + 1 > 108) {
          list + "\n|\t\t" + n
        } else {
          list + ' ' + n
        }
      }
  }
  
  override def toString: String = {
    s"""$name = {
       |	random_names = {
       ${processList(random)}
       |	}
       |	sequential_name = $sequential
       |}""".stripMargin
  }
}
