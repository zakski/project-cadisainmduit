package com.szadowsz.grainne.validation

import com.szadowsz.grainne.tools.lang.WordFormatting
import org.junit.runner.RunWith
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner

/**
  * Created by zakski on 26/11/2015.
  */
@RunWith(classOf[JUnitRunner])
class WordFormattingFunSpec extends FunSpec {

    describe("The WordFormatting object") {

      it("should capitialize names fully and correctly") {
        val word = "kIeRaN o'BrIeN"
        val delimiters = List(' ', '-', '\'')

        assertResult("Kieran O'Brien")(WordFormatting.capitalizeFully(word, delimiters))
      }

      it("should initialise names fully and correctly") {
        val word = "Kieran O'Brien"
        val delimiters = List(' ', '-', '\'')

        assertResult("KOB")(WordFormatting.initials(word, delimiters))
      }

      it("should format surnames beginning with Fitz correctly (Test I)") {
        val word = "Fitz Patrick"
        assertResult("Fitzpatrick")(WordFormatting.formatName(word))
      }

      it("should format surnames beginning with Fitz correctly (Test II)") {
        val word = "FitzPatrick"
        assertResult("Fitzpatrick")(WordFormatting.formatName(word))
      }

      it("should format surnames beginning with O correctly (Test I)") {
        val word = "o'BrIeN"
        assertResult("O'Brien")(WordFormatting.formatName(word))
      }

      it("should format surnames beginning with O correctly (Test II)") {
        val word = "o BrIeN"
        assertResult("O'Brien")(WordFormatting.formatName(word))
      }

      it("should format surnames beginning with Mc correctly (Test I)") {
        val word = "mCdOnAlD"
        assertResult("McDonald")(WordFormatting.formatName(word))
      }

      it("should format surnames beginning with Mc correctly (Test II)") {
        val word = "mC dOnAlD"
        assertResult("McDonald")(WordFormatting.formatName(word))
      }

      it("should format surnames beginning with Mac correctly (Test I)") {
        val word = "maCdOnAlD"
        assertResult("MacDonald")(WordFormatting.formatName(word))
      }

      it("should format surnames beginning with Mac correctly (Test II)") {
        val word = "maC dOnAlD"
        assertResult("MacDonald")(WordFormatting.formatName(word))
      }

      it("should format names with multiple surnames beginning with Mac/Mc correctly") {
        val word = "Andrew maCdOnAlD mC dOnAlD maC dOnAlD"
        assertResult("Andrew MacDonald McDonald MacDonald")(WordFormatting.formatName(word))
      }

      it("should format Mackie correctly") {
        val word = "Mackie"
        assertResult("Mackie")(WordFormatting.formatName(word))
      }

      it("should format du Maulin correctly") {
        val word = "Du Maulin"
        assertResult("du Maulin")(WordFormatting.formatName(word))
      }
    }
}
