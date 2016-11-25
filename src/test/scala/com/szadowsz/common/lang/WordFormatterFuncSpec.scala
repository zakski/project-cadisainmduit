// Copyright 2016 zakski.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.szadowsz.common.lang

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSpec, Matchers}

/**
  * Simple Unit Tests for the word formatter.
  *
  * Created on 28/07/2016.
  */
@RunWith(classOf[JUnitRunner])
class WordFormatterFuncSpec extends FunSpec with Matchers {

  describe("Capitalisation Function"){

    it("should remove existing capitals by default"){
      WordFormatter.capitalise("NASA") shouldBe "Nasa"
    }

    it("should only use spaces as a delimiter by default"){
      WordFormatter.capitalise("Anti-Aircraft Gun") shouldBe "Anti-aircraft Gun"
    }

    it("should be able to use user-defined delimiters"){
      WordFormatter.capitalise("Anti-Aircraft Gun",List(' ','-')) shouldBe "Anti-Aircraft Gun"
    }


    it("should be able to ignore existing capitals"){
      WordFormatter.capitalise("AA gun",List(' ','-'),false) shouldBe "AA Gun"
    }

  }
}
