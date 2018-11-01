package com.szadowz.naming.stellaris.planets

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class PlanListSectionSpec extends FlatSpec with Matchers with BeforeAndAfter {
  
  behavior of "NameListCharListSection"
  
  private val nameList = List(
    "Achlys", "Aether", "Aion", "Ananke", "Chaos", "Chronos", "Erebus", "Eros", "Gaia", "Hemera", "Hypnos", "Nemesis", "Nesoi",
    "Nyx", "Ourea", "Phanes", "Pontus", "Tartaros", "Thalassa", "Thanatos", "Uranus")
  
  it should "format generic planets correctly" in {
    val section = PlanListSection(PlanType.GENERIC, nameList)
    
    
    val expected =
      s"""generic = {
         |	names = {
         |		Achlys Aether Aion Ananke Chaos Chronos Erebus Eros Gaia Hemera Hypnos Nemesis Nesoi Nyx Ourea Phanes
         |		Pontus Tartaros Thalassa Thanatos Uranus
         |	}
         |}""".stripMargin
    
    section.toString shouldBe expected
  }
  
  it should "format desert planets correctly" in {
    val section = PlanListSection(PlanType.DESERT, nameList)
    
    
    val expected =
      s"""pc_desert = {
         |	names = {
         |		Achlys Aether Aion Ananke Chaos Chronos Erebus Eros Gaia Hemera Hypnos Nemesis Nesoi Nyx Ourea Phanes
         |		Pontus Tartaros Thalassa Thanatos Uranus
         |	}
         |}""".stripMargin
  
  
    section.toString shouldBe expected
  }
  
  it should "format tropical planets correctly" in {
    val section = PlanListSection(PlanType.TROPICAL, nameList)
    
    
    val expected =
      s"""pc_tropical = {
         |	names = {
         |		Achlys Aether Aion Ananke Chaos Chronos Erebus Eros Gaia Hemera Hypnos Nemesis Nesoi Nyx Ourea Phanes
         |		Pontus Tartaros Thalassa Thanatos Uranus
         |	}
         |}""".stripMargin
  
  
    section.toString shouldBe expected
  }
  
  it should "format arid planets correctly" in {
    val section = PlanListSection(PlanType.ARID, nameList)
    
    
    val expected =
      s"""pc_arid = {
         |	names = {
         |		Achlys Aether Aion Ananke Chaos Chronos Erebus Eros Gaia Hemera Hypnos Nemesis Nesoi Nyx Ourea Phanes
         |		Pontus Tartaros Thalassa Thanatos Uranus
         |	}
         |}""".stripMargin
  
  
    section.toString shouldBe expected
  }
  
  it should "format continental planets correctly" in {
    val section = PlanListSection(PlanType.CONTINENTAL, nameList)
    
    
    val expected =
      s"""pc_continental = {
         |	names = {
         |		Achlys Aether Aion Ananke Chaos Chronos Erebus Eros Gaia Hemera Hypnos Nemesis Nesoi Nyx Ourea Phanes
         |		Pontus Tartaros Thalassa Thanatos Uranus
         |	}
         |}""".stripMargin
  
  
    section.toString shouldBe expected
  }
  
  it should "format gaia planets correctly" in {
    val section = PlanListSection(PlanType.GAIA, nameList)
    
    
    val expected =
      s"""pc_gaia = {
         |	names = {
         |		Achlys Aether Aion Ananke Chaos Chronos Erebus Eros Gaia Hemera Hypnos Nemesis Nesoi Nyx Ourea Phanes
         |		Pontus Tartaros Thalassa Thanatos Uranus
         |	}
         |}""".stripMargin
  
  
    section.toString shouldBe expected
  }
  
  it should "format ocean planets correctly" in {
    val section = PlanListSection(PlanType.OCEAN, nameList)
    
    
    val expected =
      s"""pc_ocean = {
         |	names = {
         |		Achlys Aether Aion Ananke Chaos Chronos Erebus Eros Gaia Hemera Hypnos Nemesis Nesoi Nyx Ourea Phanes
         |		Pontus Tartaros Thalassa Thanatos Uranus
         |	}
         |}""".stripMargin
  
  
    section.toString shouldBe expected
  }
  
  it should "format tundra planets correctly" in {
    val section = PlanListSection(PlanType.TUNDRA, nameList)
    
    
    val expected =
      s"""pc_tundra = {
         |	names = {
         |		Achlys Aether Aion Ananke Chaos Chronos Erebus Eros Gaia Hemera Hypnos Nemesis Nesoi Nyx Ourea Phanes
         |		Pontus Tartaros Thalassa Thanatos Uranus
         |	}
         |}""".stripMargin
  
  
    section.toString shouldBe expected
  }
  
  it should "format arctic planets correctly" in {
    val section = PlanListSection(PlanType.ARCTIC, nameList)
    
    
    val expected =
      s"""pc_arctic = {
         |	names = {
         |		Achlys Aether Aion Ananke Chaos Chronos Erebus Eros Gaia Hemera Hypnos Nemesis Nesoi Nyx Ourea Phanes
         |		Pontus Tartaros Thalassa Thanatos Uranus
         |	}
         |}""".stripMargin
  
  
    section.toString shouldBe expected
  }
  
  it should "format savannah planets correctly" in {
    val section = PlanListSection(PlanType.SAVANNAH, nameList)
    
    
    val expected =
      s"""pc_savannah = {
         |	names = {
         |		Achlys Aether Aion Ananke Chaos Chronos Erebus Eros Gaia Hemera Hypnos Nemesis Nesoi Nyx Ourea Phanes
         |		Pontus Tartaros Thalassa Thanatos Uranus
         |	}
         |}""".stripMargin
  
  
    section.toString shouldBe expected
  }
  
  it should "format alpine planets correctly" in {
    val section = PlanListSection(PlanType.ALPINE, nameList)
    
    
    val expected =
      s"""pc_alpine = {
         |	names = {
         |		Achlys Aether Aion Ananke Chaos Chronos Erebus Eros Gaia Hemera Hypnos Nemesis Nesoi Nyx Ourea Phanes
         |		Pontus Tartaros Thalassa Thanatos Uranus
         |	}
         |}""".stripMargin
  
  
    section.toString shouldBe expected
  }
}
