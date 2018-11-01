package com.szadowz.naming.stellaris.characters

case class CharListSection(
                                    weight : Int,
                                    first_names_male : List[String],
                                    first_names_female : List[String],
                                    second_names : List[String],
                                    regnal_first_names_male : List[String],
                                    regnal_first_names_female : List[String],
                                    regnal_second_names : List[String]
                                  ) {

  private def processList(names: List[String]) : String = {
    names.map(n => if (n.matches("\\s")){ '"' + n + '"'} else { n} )
      .foldLeft("|\t\t"){ (list,n) =>
        if (list.length == 3) {
          list + n
        } else if (list.substring(Math.max(list.lastIndexOf('\n'),0)).length + n.length + 1 > 108){
          list + "\n|\t\t" + n
        } else {
          list + ' ' + n
        }
      }
  }

  override def toString: String = {
    s"""	weight = $weight
      |	first_names_male = {
      ${processList(first_names_male)}
      |	}
      |	first_names_female = {
      ${processList(first_names_female)}
      |	}
      |	second_names = {
      ${processList(second_names)}
      |	}
      |	regnal_first_names_male = {
      ${processList(regnal_first_names_male)}
      |	}
      |	regnal_first_names_female = {
      ${processList(regnal_first_names_female)}
      |	}
      |	regnal_second_names = {
      ${processList(regnal_second_names)}
      |	}""".stripMargin
  }

}
