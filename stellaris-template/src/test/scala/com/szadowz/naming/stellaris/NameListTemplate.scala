package com.szadowz.naming.stellaris

class NameListTemplate {

  def construct : String =
    """
      |### Children of God
      |childrenofgod = {
      |	randomized = yes
      |
      |	ship_names = {
      |		generic = {
      | Abihu
      | Apollos
      | Asa
      | Asher
      | Bartholomew
      | Gabriel
      | Gideon Goliath
      | Herod
      | Hushai
      | Ira
      | Jesse
      | Jonah
      | Jordan
      | Levi
      | Lucas
      | Lucifer
      | Luke
      | Magi
      | Malachi
      | Marcus
      | Meshach
      | Micah
      | Moses
      | Nekoda
      | Noah
      | Nebo
      | Omar
      | Pharaoh
      | Philip
      | Phineas
      | Pontius
      | Rufus
      | Samson
      | Satan
      | Seth
      | Shadrach
      | Silas
      | Simon
      | Solomon
      | Titus
      | Tobiah
      | Tobias
      | Victor Abishai Atarah Bathsheba Carmel Delilah
      | Dinah Dorcas Eden Glory Hannah Jael Jasper Jewel Jordan Leah Lois Magdalene Mercy Michal Myra Olive Ophrah Paula Phoebe Prisca Rachel Rhoda Rose Ruby Sarah Serah Sharon Sherah Shiloh Shiphrah Tamar Terah Zina }
      |
      |		corvette = { }
      |
      |		constructor = { }
      |
      |		colonizer = { }
      |
      |		science = { }
      |
      |		destroyer = { }
      |
      |		cruiser = { }
      |
      |		battleship = { }
      |
      |		orbital_station = { }
      |		mining_station = { }
      |		research_station = { }
      |		wormhole_station = { }
      |		terraform_station = { }
      |		observation_station = { }
      |		outpost_station = {
      |			random_names = { Agrius Alcyoneus Chthonius Clytius Enceladus Ephialtes Eurymedon Eurytus Gration Hippolytus Lion Mimas Pallas Polybotes Porphyrion Thoas }
      |			sequential_name = "%O% Frontier Outpost"
      |		}
      |
      |		transport = { }
      |
      |		military_station_small = { }
      |
      |		military_station_medium = { }
      |
      |		military_station_large = { }
      |
      |	}
      |
      |	fleet_names = {
      |		random_names = {
      |			"Achaia Savior" "Archevite Savior" "Babel Savior" "Berea Savior" "Bethlehem Savior" "Damascus Savior" "Eden Savior" "Jerusalem Savior" "Gibeon Savior" "Hebron Savior" "Jericho Savior" "Judea Savior" "Lyvia Savior"
      |		}
      |		sequential_name = "%O% Fleet"
      |	}
      |
      |	army_names = {
      |		defense_army = {
      |			sequential_name = "%O% God's Guard"
      |		}
      |
      |		assault_army = {
      |			sequential_name = "%O% Peace Bringer"
      |		}
      |
      |		slave_army = {
      |			sequential_name = "%O% Unsullied"
      |		}
      |
      |		clone_army = {
      |			sequential_name = "%O% Clone Army"
      |		}
      |
      |		robotic_army = {
      |			sequential_name = "%O% Faithful"
      |		}
      |
      |		android_army = {
      |			sequential_name = "%O% Believers"
      |		}
      |
      |		psionic_army = {
      |			sequential_name = "%O% Psi Prophets"
      |		}
      |
      |		xenomorph_army = {
      |			sequential_name = "%O% Giants"
      |		}
      |
      |		gene_warrior_army = {
      |			random_names = {
      |				"SARC-A 'Gladiators'" "SARC-B 'Widowmakers'" "SARC-C 'Immortals'" "SARC-D 'Berserkers'" "SARC-E 'Assassins'"
      |				"SARC-F 'Reapers'" "SARC-G 'Nighthawks'" "SARC-H 'Desperados'" "SARC-I 'Grey Knights'" "SARC-J 'Roughnecks'"
      |			}
      |			sequential_name = "%O% Bio-Engineered Squadron"
      |		}
      |	}
      |
      |	planet_names = {
      |
      |		generic = {
      |			names = {
      |				Aphrodite Apollo
      | Ares
      | Artemis
      | Athena
      | Demeter
      | Dionysus
      | Hades
      | Hephaestus
      | Hera
      | Hermes
      | Hestia
      | Poseidon
      | Zeus
      | Aether
      | Ananke
      | Chaos
      | Chronos
      | Erebus
      | Eros
      | Hypnos
      | Nesoi
      | Uranus
      | Gaia
      | Ourea
      | Phanes
      | Pontus
      | Tartarus
      | Thalassa
      | Thanatos
      | Hemera
      | Nyx
      | Nemesis
      |Coeus
      | Crius
      | Cronus
      | Hyperion
      | Iapetus
      | Mnemosyne
      | Oceanus
      | Phoebe
      | Rhea
      | Tethys
      | Theia
      | Themis
      | Asteria
      | Astraeus
      | Atlas
      | Aura
      | Clymene
      | Dione
      | Helios
      | Selene
      | Eos
      | Epimetheus
      | Eurybia
      | Eurynome
      | Lelantos
      | Leto
      | Menoetius
      | Metis
      | Ophion
      | Pallas
      | Perses
      | Prometheus
      | Styx
      |			}
      |		}
      |
      |		pc_desert = {
      |			names = { }
      |		}
      |
      |		pc_arid = {
      |			names = { }
      |		}
      |
      |		pc_tropical = {
      |			names = { }
      |		}
      |
      |		pc_continental = {
      |			names = { }
      |		}
      |
      |		pc_gaia = {
      |			names = { }
      |		}
      |
      |		pc_ocean = {
      |			names = { }
      |		}
      |
      |		pc_tundra = {
      |			names = { }
      |		}
      |
      |		pc_arctic = {
      |			names = { }
      |		}
      |	}
      |
      |
      |	### CHARACTERS
      |
      |	character_names = {
      |
      |		names1 = {
      |			weight = 40
      |			first_names_male = { Aaron
      | Abel
      | Abiathar
      | Abihu Adam
      | Alexander
      | Amaziah
      | Amos
      | Ananias
      | Andrew
      | Apollos
      | Aquila
      | Asa
      | Asaph
      | Asher
      | Azariah
      | Barak
      | Benjamin
      | Bildad
      | Boaz
      | Cain
      | Caleb
      | Christian
      | Claudius
      | Cornelius
      | Dan
      | Daniel
      | David
      | Demetrius
      | Ebenezer
      | Elah
      | Eleazar
      | Eli
      | Ephraim
      | Esau
      | Ethan
      | Ezekiel
      | Ezra
      | Gabriel
      | Gera
      | Gershon
      | Gideon
      | Habakkuk
      | Haggai
      | Ira
      | Isaac
      | Isaiah
      | Ishmael
      | Issachar
      | Ithamar
      | Jabez
      | Jacob
      | Jair
      | Jairus
      | Joash
      | Job
      | Joel
      | John
      | Jonah
      | Jonathan
      | Jordan
      | Joseph
      | Joses
      | Joshua
      | Jude
      | Justus
      | Laban
      | Lazarus
      | Lemuel
      | Levi
      | Lot
      | Lucas
      | Luke
      | Malachi
      | Manasseh
      | Marcus
      | Mark
      | Matthew
      | Matthias
      | Melchizedek
      | Micah
      | Micaiah
      | Michael
      | Mishael
      | Mordecai
      | Moses
      | Nadab
      | Nahum
      | Naphtali
      | Nathan
      | Othniel
      | Paul
      | Peter
      | Philemon
      | Philip
      | Phineas
      | Phinehas
      | Reuben
      | Rufus
      | Samson
      | Samuel
      | Silas
      | Simeon
      | Simon
      | Solomon
      | Stephen
      | Thaddaeus
      | Theophilus
      | Thomas
      | Timothy
      | Zacchaeus
      | Zachariah
      | Zebadiah
      | Zebedee
      | Zebulun
      | Zechariah
      | Zedekiah
      | Zephaniah
      | Zerubbabel
      | Odin }
      |			first_names_female = { Abigail
      | Abihail
      | Abishai
      | Adah
      | Bernice
      | Bethany
      | Bethel
      | Beulah
      | Bilhah
      | Candace
      | Carmel
      | Charity
      | Chloe
      | Claudia
      | Damaris
      | Dorcas
      | Drusilla
      | Eden Eva Eve
      | Edna
      | Elisha
      | Elizabeth
      | Esther
      | Hannah
      | Honey
      | Hope
      | Huldah
      | Jael
      | Jasper
      | Jemimah
      | Jewel
      | Joanna
      | Lois
      | Lydia
      | Magdalene
      | Mara
      | Marah
      | Martha
      | Mary
      | Mercy
      | Merry
      | Michal
      | Miriam
      | Myra
      | Naomi
      | Neriah
      | Olive
      | Ophrah
      | Rebecca
      | Rebekah
      | Rhoda
      | Rose
      | Ruby
      | Ruth
      | Sapphira
      | Sarah
      | Sarai
      | Selah
      | Serah
      | Sharon
      | Sherah
      | Shiloh
      | Talitha
      | Tamar
      | Tamara
      | Zemira
      | Zilpah
      | Zina
      | Zipporah
      |
      |			}
      |			second_names = { Abad Abbas Abbasi Abdalla Abdallah Abdella Abdelnour Abdelrahman Abdi Abdo Abdoo Abdou Abdul Abdulla Abdullah Abed Abid Abood Aboud Abraham Abu Adel Afzal Agha Ahmad Ahmadi Ahmed Ahsan Akbar Akbari Akel Akhtar Akhter Akram Alam Ali Allam Allee Alli Ally Aly Aman Amara Amber Ameen Amen Amer Amin Amini Amir Amiri Ammar Ansari Anwar Arafat Arif Arshad Asad Ashraf Aslam Asmar Assad Assaf Atallah Attar Awan Aydin Ayoob Ayoub Ayub Azad Azam Azer Azimi Aziz Azizi Azzam Azzi Bacchus Baccus Bacho Baddour Badie Badour Bagheri Bahri Baig Baksh Baluch Bangura Barakat Bari Basa Basha Bashara Basher Bashir Baten Begum Ben Beshara Bey Beydoun Bilal Bina Burki Can Chahine Dada Dajani Dallal Daoud Dar Darwish Dawood Demian Dia Diab Dib Din Doud Ebrahim Ebrahimi Edris Eid Elamin Elbaz El-Sayed Emami Fadel Fahmy Fahs Farag Farah Faraj Fares Farha Farhat Farid Faris Farman Farooq Farooqui Farra Farrah Farran Fawaz Fayad Firman Gaber Gad Galla Ghaffari Ghanem Ghani Ghattas Ghazal Ghazi Greiss Guler Habeeb Habib Habibi Hadi Hafeez Hai Haidar Haider Hakeem Hakim Halaby Halim Hallal Hamad Hamady Hamdan Hamed Hameed Hamid Hamidi Hammad Hammoud Hana Hanif Hannan Haq Haque Hares Hariri Harron Harroun Hasan Hasen Hashem Hashemi Hashim Hashmi Hassan Hassen Hatem Hoda Hoque Hosein Hossain Hosseini Huda Huq Husain Hussain Hussein Ibrahim Idris Imam Iman Iqbal Irani Ishak Ishmael Islam Ismael Ismail Jabara Jabbar Jabbour Jaber Jabour Jafari Jaffer Jafri Jalali Jalil Jama Jamail Jamal Jamil Jan Javed Javid Kaba Kaber Kabir Kader Kaiser Kaleel Kalil Kamal Kamali Kamara Kamel Kanan Karam Karim Karimi Kassem Kazemi Kazi Kazmi Khalaf Khalid Khalifa Khalil Khalili Khan Khatib Khawaja Koroma Laham Latif Lodi Lone Madani Mady Mahdavi Mahdi Mahfouz Mahmood Mahmoud Mahmud Majeed Majid Malak Malek Malik Mannan Mansoor Mansour Mansouri Mansur Maroun Masih Masood Masri Massoud Matar Matin Mattar Meer Meskin Miah Mian  Mina Minhas Mir Mirza Mitri Moghaddam Mohamad Mohamed Mohammad Mohammadi Mohammed Mohiuddin Molla Momin Mona Morad Moradi Mostafa Mourad Mousa Moussa Moustafa Mowad Muhammad Muhammed Munir Murad Musa Mussa Mustafa Naderi Nagi Naim Naqvi Nasir Nasr Nasrallah Nasser Nassif Nawaz Nazar Nazir Neman Niazi Noor Noorani Noori Nour Nouri Obeid Odeh Omar Omer Othman Ozer Parsa Pasha Pashia Pirani Popal Pour Qadir Qasim Qazi Quadri Raad Rabbani Rad Radi Radwan Rafiq Rahaim Rahaman Rahim Rahimi Rahman Rahmani Rais Ramadan Ramin Rashed Rasheed Rashid Rassi Rasul Rauf Rayes Rehman Rehmann Reza Riaz Rizk Saab Saad Saade Saadeh Saah Saba Saber Sabet Sabir Sadek Sader Sadiq Sadri Saeed Safar Safi Sahli Saidi Sala Salaam Saladin Salah Salahuddin Salam Salama Salame Salameh Saleem Saleh Salehi Salek Salem Salih Salik Salim Salloum Salman Samaan Samad Samara Sami Samra Sani Sarah Sarwar Sattar Satter Sawaya Sayed Selim Semaan Sesay Shaban Shabazz Shad Shaer Shafi Shah Shahan Shaheed Shaheen Shahid Shahidi Shahin Shaikh Shaker Shakir Shakoor Sham Shams Sharaf Shareef Sharif Shariff Sharifi Shehadeh Shehata Sheikh Siddiqi Siddique Siddiqui Sinai Soliman Soltani Srour Sulaiman Suleiman Sultan Sultana Syed Sylla Tabatabai Tabet Taha Taheri Tahir Tamer Tariq Tawil Toure Turay Uddin Ullah Usman Vaziri Vohra Wahab Wahba Waheed Wakim Wali Yacoub Yamin Yasin Yassin Younan Younes Younis Yousef Yousif Youssef Yousuf Yusuf Zadeh Zafar Zaher Zahra Zaidi Zakaria Zaki Zaman Zamani Zia
      |			}
      |			regnal_first_names_male = {
      |
      |			}
      |			regnal_first_names_female = {
      |
      |			}
      |			regnal_second_names = {
      |
      |			}
      |		}
      |
      |	}
      |}
      |
    """.stripMargin
}
