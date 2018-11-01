package com.szadowz.naming.stellaris.characters

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class CharListSpec extends FlatSpec with Matchers with BeforeAndAfter {

  behavior of "CharList"

  private val nameList = List(
    "Achlys","Aether","Aion","Ananke","Chaos","Chronos","Erebus","Eros","Gaia","Hemera","Hypnos","Nemesis","Nesoi",
    "Nyx","Ourea","Phanes","Pontus","Tartaros","Thalassa","Thanatos","Uranus")

  it should "format one section with first_names_male correctly" in {
    val section = CharListSection(10,nameList,List(),List(),List(),List(),List())
    val list = CharList(List(section))
      
    val expected =  s"""	### CHARACTERS
                       |
                       |	character_names = {
                       |		names0 = {
                       |				weight = 10
                       |				first_names_male = {
                       |					Achlys Aether Aion Ananke Chaos Chronos Erebus Eros Gaia Hemera Hypnos Nemesis Nesoi Nyx Ourea Phanes
                       |					Pontus Tartaros Thalassa Thanatos Uranus
                       |				}
                       |				first_names_female = {
                       |					
                       |				}
                       |				second_names = {
                       |					
                       |				}
                       |				regnal_first_names_male = {
                       |					
                       |				}
                       |				regnal_first_names_female = {
                       |					
                       |				}
                       |				regnal_second_names = {
                       |					
                       |				}
                       |		}
                       |	}""".stripMargin

    list.toString shouldBe expected
  }

  it should "format one section with first_names_female correctly" in {
    val section = CharListSection(10,List(),nameList,List(),List(),List(),List())
    val list = CharList(List(section))

    val expected =  """	### CHARACTERS
                      |
                      |	character_names = {
                      |		names0 = {
                      |				weight = 10
                      |				first_names_male = {
                      |					
                      |				}
                      |				first_names_female = {
                      |					Achlys Aether Aion Ananke Chaos Chronos Erebus Eros Gaia Hemera Hypnos Nemesis Nesoi Nyx Ourea Phanes
                      |					Pontus Tartaros Thalassa Thanatos Uranus
                      |				}
                      |				second_names = {
                      |					
                      |				}
                      |				regnal_first_names_male = {
                      |					
                      |				}
                      |				regnal_first_names_female = {
                      |					
                      |				}
                      |				regnal_second_names = {
                      |					
                      |				}
                      |		}
                      |	}""".stripMargin
    list.toString shouldBe expected
  }

  it should "format one section with second_names correctly" in {
    val section = CharListSection(10,List(),List(),nameList,List(),List(),List())
    val list = CharList(List(section))
      
    val expected =  """	### CHARACTERS
                      |
                      |	character_names = {
                      |		names0 = {
                      |				weight = 10
                      |				first_names_male = {
                      |					
                      |				}
                      |				first_names_female = {
                      |					
                      |				}
                      |				second_names = {
                      |					Achlys Aether Aion Ananke Chaos Chronos Erebus Eros Gaia Hemera Hypnos Nemesis Nesoi Nyx Ourea Phanes
                      |					Pontus Tartaros Thalassa Thanatos Uranus
                      |				}
                      |				regnal_first_names_male = {
                      |					
                      |				}
                      |				regnal_first_names_female = {
                      |					
                      |				}
                      |				regnal_second_names = {
                      |					
                      |				}
                      |		}
                      |	}""".stripMargin
  
    list.toString shouldBe expected
  }

  it should "format one section with regnal_first_names_male correctly" in {
    val section = CharListSection(10,List(),List(),List(),nameList,List(),List())
    val list = CharList(List(section))
   
    val expected =  """	### CHARACTERS
                      |
                      |	character_names = {
                      |		names0 = {
                      |				weight = 10
                      |				first_names_male = {
                      |					
                      |				}
                      |				first_names_female = {
                      |					
                      |				}
                      |				second_names = {
                      |					
                      |				}
                      |				regnal_first_names_male = {
                      |					Achlys Aether Aion Ananke Chaos Chronos Erebus Eros Gaia Hemera Hypnos Nemesis Nesoi Nyx Ourea Phanes
                      |					Pontus Tartaros Thalassa Thanatos Uranus
                      |				}
                      |				regnal_first_names_female = {
                      |					
                      |				}
                      |				regnal_second_names = {
                      |					
                      |				}
                      |		}
                      |	}""".stripMargin
  
    list.toString shouldBe expected
  }

  it should "format one section with regnal_first_names_female correctly" in {
    val section = CharListSection(10,List(),List(),List(),List(),nameList,List())
    val list = CharList(List(section))
      
    val expected =  """	### CHARACTERS
                      |
                      |	character_names = {
                      |		names0 = {
                      |				weight = 10
                      |				first_names_male = {
                      |					
                      |				}
                      |				first_names_female = {
                      |					
                      |				}
                      |				second_names = {
                      |					
                      |				}
                      |				regnal_first_names_male = {
                      |					
                      |				}
                      |				regnal_first_names_female = {
                      |					Achlys Aether Aion Ananke Chaos Chronos Erebus Eros Gaia Hemera Hypnos Nemesis Nesoi Nyx Ourea Phanes
                      |					Pontus Tartaros Thalassa Thanatos Uranus
                      |				}
                      |				regnal_second_names = {
                      |					
                      |				}
                      |		}
                      |	}""".stripMargin
  
    list.toString shouldBe expected
  }

  it should "format one section with regnal_second_names correctly" in {
    val section = CharListSection(10,List(),List(),List(),List(),List(),nameList)
   val list = CharList(List(section))
  
    val expected =  """	### CHARACTERS
                      |
                      |	character_names = {
                      |		names0 = {
                      |				weight = 10
                      |				first_names_male = {
                      |					
                      |				}
                      |				first_names_female = {
                      |					
                      |				}
                      |				second_names = {
                      |					
                      |				}
                      |				regnal_first_names_male = {
                      |					
                      |				}
                      |				regnal_first_names_female = {
                      |					
                      |				}
                      |				regnal_second_names = {
                      |					Achlys Aether Aion Ananke Chaos Chronos Erebus Eros Gaia Hemera Hypnos Nemesis Nesoi Nyx Ourea Phanes
                      |					Pontus Tartaros Thalassa Thanatos Uranus
                      |				}
                      |		}
                      |	}""".stripMargin
  
    list.toString shouldBe expected
  }

}
