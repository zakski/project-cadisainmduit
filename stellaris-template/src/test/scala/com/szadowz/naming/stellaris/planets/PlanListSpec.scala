package com.szadowz.naming.stellaris.planets

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class PlanListSpec extends FlatSpec with Matchers with BeforeAndAfter {
  
  behavior of "NameListCharListSection"
  
  private val genList = List("Achlys")
  private val desertList = List("Chaos")
  private val tropList = List("Erebus")
  private val aridList = List("Gaia")
  private val contList = List("Hemera")
  private val gaiaList = List("Nemesis")
  private val oceaList = List("Ourea")
  private val tundList = List("Phanes")
  private val arctList = List("Tartaros")
  private val savaList = List("Uranus")
  private val alpiList = List("Aether")
  
 it should "format planets in-order correctly" in {
    val sections = List(
      PlanListSection(PlanType.GENERIC, genList),
      PlanListSection(PlanType.DESERT, desertList),
      PlanListSection(PlanType.TROPICAL, tropList),
      PlanListSection(PlanType.ARID, aridList),
      PlanListSection(PlanType.CONTINENTAL, contList),
      PlanListSection(PlanType.GAIA, gaiaList),
      PlanListSection(PlanType.OCEAN, oceaList),
      PlanListSection(PlanType.TUNDRA, tundList),
      PlanListSection(PlanType.ARCTIC, arctList),
      PlanListSection(PlanType.SAVANNAH, savaList),
      PlanListSection(PlanType.ALPINE, alpiList)
    )    
    
    val planets = PlanList(sections)
   
    val expected =
      s"""	### PLANETS
         |
         |	planet_names = {
         |		generic = {
         |			names = {
         |				Achlys
         |			}
         |		}
         |		pc_desert = {
         |			names = {
         |				Chaos
         |			}
         |		}
         |		pc_tropical = {
         |			names = {
         |				Erebus
         |			}
         |		}
         |		pc_arid = {
         |			names = {
         |				Gaia
         |			}
         |		}
         |		pc_continental = {
         |			names = {
         |				Hemera
         |			}
         |		}
         |		pc_gaia = {
         |			names = {
         |				Nemesis
         |			}
         |		}
         |		pc_ocean = {
         |			names = {
         |				Ourea
         |			}
         |		}
         |		pc_tundra = {
         |			names = {
         |				Phanes
         |			}
         |		}
         |		pc_arctic = {
         |			names = {
         |				Tartaros
         |			}
         |		}
         |		pc_savannah = {
         |			names = {
         |				Uranus
         |			}
         |		}
         |		pc_alpine = {
         |			names = {
         |				Aether
         |			}
         |		}
         |	}""".stripMargin
    
    planets.toString shouldBe expected
  }
  
  it should "format planets out-of-order correctly" in {
    val sections = List(
      PlanListSection(PlanType.ARID, aridList),
      PlanListSection(PlanType.CONTINENTAL, contList),
      PlanListSection(PlanType.DESERT, desertList),
      PlanListSection(PlanType.GENERIC, genList),
      PlanListSection(PlanType.TROPICAL, tropList),
      PlanListSection(PlanType.GAIA, gaiaList),
      PlanListSection(PlanType.OCEAN, oceaList),
      PlanListSection(PlanType.SAVANNAH, savaList),
      PlanListSection(PlanType.ALPINE, alpiList),
      PlanListSection(PlanType.TUNDRA, tundList),
      PlanListSection(PlanType.ARCTIC, arctList)
    )
    
    val planets = PlanList(sections)
    
    val expected =
      s"""	### PLANETS
         |
         |	planet_names = {
         |		generic = {
         |			names = {
         |				Achlys
         |			}
         |		}
         |		pc_desert = {
         |			names = {
         |				Chaos
         |			}
         |		}
         |		pc_tropical = {
         |			names = {
         |				Erebus
         |			}
         |		}
         |		pc_arid = {
         |			names = {
         |				Gaia
         |			}
         |		}
         |		pc_continental = {
         |			names = {
         |				Hemera
         |			}
         |		}
         |		pc_gaia = {
         |			names = {
         |				Nemesis
         |			}
         |		}
         |		pc_ocean = {
         |			names = {
         |				Ourea
         |			}
         |		}
         |		pc_tundra = {
         |			names = {
         |				Phanes
         |			}
         |		}
         |		pc_arctic = {
         |			names = {
         |				Tartaros
         |			}
         |		}
         |		pc_savannah = {
         |			names = {
         |				Uranus
         |			}
         |		}
         |		pc_alpine = {
         |			names = {
         |				Aether
         |			}
         |		}
         |	}""".stripMargin
    
    planets.toString shouldBe expected
  }

}
