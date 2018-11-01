package com.szadowsz.cadisainmduit.places.nga.commonwealth

import java.io.File

import com.szadowsz.cadisainmduit.places.Country
import com.szadowsz.common.io.explore.{ExtensionFilter, FileFinder}
import org.apache.spark.sql.SparkSession

object Commonwealth {

  val countries: List[Country] = List(
    Country("Anguilla","carribbean"),
    Country("Antigua and Barbuda","carribbean"),
    Country("Akrotiri","europe"),
    Country("Australia","oceania"),
    Country("Bahamas","carribbean"),
    Country("Bangladesh","asia"),
    Country("Barbados","carribbean"),
    Country("Belize","americas"),
    Country("Bermuda","carribbean"),
    Country("Botswana","africa"),
    Country("British Indian Ocean Territory","asia"),
    Country("British Virgin Islands","carribbean"),
    Country("Brunei","asia"),
    Country("Cameroon","africa"),
    Country("Canada","americas"),
    Country("Cayman Islands","carribbean"),
    Country("Dhekelia","europe"),
    Country("Malta","europe"),
    Country("Dominica","carribbean"),
    Country("Falkland Islands","americas"),
    Country("Fiji","oceani"),
    Country("Gambia","africa"),
    Country("Ghana","africa"),
    Country("Gibraltar","europe"),
    Country("Grenada","carribbean"),
    Country("Guernsey","europe"),
    Country("Guyana","South America"),
    Country("Hong Kong","asia"),
    Country("India","asia"),
    Country("Ireland","europe"),
    Country("Isle Of Man","europe"),
    Country("Jamaica","carribbean"),
    Country("Jersey","europe"),
    Country("Kenya","africa"),
    Country("Kiribati","oceania"),
    Country("Lesotho","africa"),
    Country("Malawi","africa"),
    Country("Malaysia","asia"),
    Country("Malta","europe"),
    Country("Mauritius","africa"),
    Country("Montserrat","carribbean"),
    Country("Mozambique","africa"),
    Country("Namibia","africa"),
    Country("Nauru","oceania"),
    Country("New Zealand","oceania"),
    Country("Nigeria","africa"),
    Country("Pakistan","asia"),
    Country("Papua New Guinea","oceania"),
    Country("Pitcairn Islands","asia"),
    Country("Rwanda","africa"),
    Country("Saint Helena","americas"),
    Country("Saint Kitts and Nevis","carribbean"),
    Country("Saint Lucia","carribbean"),
    Country("Saint Vincent and the Grenadines","carribbean"),
    Country("Samoa","oceania"),
    Country("Seychelles","africa"),
    Country("Sierra Leone","africa"),
    Country("Singapore","asia"),
    Country("Solomon Islands","oceania"),
    Country("South Africa","africa"),
    Country("South Georgia and South Sandwich Islands","americas"),
    Country("Sri Lanka","asia"),
    Country("Swaziland","africa"),
    Country("Tanzania","africa"),
    Country("Tonga","oceania"),
    Country("Trinidad and Tobago","carribbean"),
    Country("Turks and Caicos Islands","americas"),
    Country("Tuvalu","oceania"),
    Country("Uganda","africa"),
    Country("United Kingdom","europe"),
    Country("Vanuatu","oceania"),
    Country("Zambia","africa"),
    Country("Zimbabwe","africa")
  )

  def getCountryFiles(path : String, sess : SparkSession, customFilter : (File) => Boolean):Array[File] = {
    FileFinder.search(path, Some(new ExtensionFilter(".txt", true)))
      .filter(f => commonwealthFilter(f) && customFilter(f))
  }

  def gerCountryCodes(path : String, customFilter : (File) => Boolean) = {
   FileFinder.search(path, Some(new ExtensionFilter(".txt", true))).filter(f => commonwealthFilter(f) && customFilter(f))
     .map(_.getName.substring(0, 2))
  }

  private def commonwealthFilter(f: File) = {
    f.getName.endsWith("_populatedplaces_p.txt") && countries.exists(c => f.getAbsolutePath.contains(c.name))
  }
}
