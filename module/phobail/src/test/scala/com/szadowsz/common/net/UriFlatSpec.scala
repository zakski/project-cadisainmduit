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
package com.szadowsz.common.net

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class UriFlatSpec extends FlatSpec with Matchers {

  behavior of "Uri"

  it should "handle a simple URL" in {
    val url: String = "http://www.example.com"

    val uri = Uri(url)

    uri.toString should be(url)

    uri.schemeOpt should be(Some("http"))
    uri.scheme should be("http")

    uri.hostOpt should be(Some("www.example.com"))
    uri.host should be("www.example.com")

    uri.userInfoOpt should be (None)
    uri.portOpt should be(None)
    uri.pathOpt should be(None)
    uri.queryOpt should be(None)
    uri.fragmentOpt should be(None)
  }

  it should "handle percent encoding a URL" in {
    val url: String = "http://www.example.com/test/my first url.html"
    val percentUrl: String = "http://www.example.com/test/my%20first%20url.html"

    val uri = Uri(url)

    uri.toString should be(percentUrl)

    uri.schemeOpt should be(Some("http"))
    uri.scheme should be("http")

    uri.hostOpt should be(Some("www.example.com"))
    uri.host should be("www.example.com")

    uri.path should be("/test/my first url.html")


    uri.userInfoOpt should be (None)
    uri.portOpt should be(None)
    uri.queryOpt should be(None)
    uri.fragmentOpt should be(None)
  }

  it should "handle a non-ascii URL" in {
    val url: String = "http://www.example.com/Is maíth líom/caca.html"
    val percentUrl: String = "http://www.example.com/Is%20maíth%20líom/caca.html"

    val uri = Uri(url)

    uri.toString should be(percentUrl)
    uri.path should be("/Is maíth líom/caca.html")
  }

    it should "handle a relative path URL" in {
      val url: String = "/test"

      val uri = Uri(url)

      uri.schemeOpt should be(None)
      uri.userOpt should be(None)
      uri.passwordOpt should be(None)
      uri.hostOpt should be(None)
      uri.portOpt should be(None)
      uri.path should be("/test")
      uri.queryOpt should be(None)
      uri.fragmentOpt should be(None)

      uri.toString should be (url)
    }

    it should "handle a relative scheme URL" in {
      val url: String = "//www.example.com/test"
      val uri = Uri(url)

      uri.schemeOpt should be(None)
      uri.userOpt should be(None)
      uri.passwordOpt should be(None)
      uri.host should be("www.example.com")
      uri.portOpt should be(None)
      uri.path should be("/test")
      uri.queryOpt should be(None)
      uri.fragmentOpt should be(None)

      uri.toString should be (url)
    }

    it should "handle authenticated URL" in {
      val url: String = "https://user:password@www.example.com/test"
      val uri = Uri(url)

      uri.scheme should be("https")
      uri.user should be("user")
      uri.password should be("password")
      uri.host should be("www.example.com")
      uri.portOpt should be(None)
      uri.path should equal("/test")
      uri.queryOpt should be(None)
      uri.fragmentOpt should be(None)

      uri.toString should be (url)
    }

    it should "handle authenticated URL with a fragment" in {
      val url: String = "https://user:password@www.example.com/test#frag"
      val uri = Uri(url)

      uri.scheme should be("https")
      uri.user should be("user")
      uri.password should be("password")
      uri.host should be("www.example.com")
      uri.portOpt should be(None)
      uri.path should be("/test")
      uri.queryOpt should be(None)
      uri.fragment should be("frag")
    }

    it should "handle a Uri with query parameters" in {
      val url: String = "/test.html?param_one=hello&param_one=goodbye&param_two=false"
      val uri = Uri(url)

      uri.schemeOpt should be(None)
      uri.userOpt should be(None)
      uri.passwordOpt should be(None)
      uri.hostOpt should be(None)
      uri.portOpt should be(None)
      uri.path should be("/test.html")
      uri.queryComp should be(List("param_one" -> Some("hello"), "param_one" -> Some("goodbye"), "param_two" -> Some("false")))
      uri.fragmentOpt should be(None)

      uri.toString should be (url)
    }

    it should "handle scheme replacement" in {
      val url: String = "http://www.example.com"
      val url2: String = "https://www.example.com"

      val uri = Uri(url).withScheme("https")

      uri.scheme should be("https")

      uri.toString should be (url2)
    }

    it should "handle host replacement" in {
      val url: String = "http://www.example.com"
      val url2: String = "http://www.test.com"

      val uri = Uri(url).withHost("www.test.com")

      uri.host should be("www.test.com")

      uri.toString should be (url2)
    }

    it should "handle port replacement" in {
      val url: String = "http://www.example.com:80"
      val url2: String = "http://www.example.com:8080"

      val uri = Uri(url).withPort(8080)

      uri.port should be(8080)

      uri.toString should be (url2)
    }

    it should "handle path replacement" in {
      val url: String = "http://www.example.com/ref"
      val url2: String = "http://www.example.com/test"

      val uri = Uri(url).withPath("/test")

      uri.path should be("/test")

      uri.toString should be (url2)
    }

    it should "handle path amendment" in {
      val path =  "class/1.html"
      val url: String = "http://uboat.net/allies/warships/"
      val url2: String = "http://uboat.net/allies/warships/class/1.html"

      val uri = Uri(url).appendPath(path)

      uri.path should be("/allies/warships/class/1.html")

      uri.toString should be (url2)
    }

    it should "handle fragment replacement" in {
      val url: String = "http://www.example.com#ref"
      val url2: String = "http://www.example.com#test"

      val uri = Uri(url).withFragment("test")

      uri.fragment should be("test")

      uri.toString should be (url2)
    }

    it should "handle user replacement" in {
      val url: String = "http://usr@www.example.com"
      val url2: String = "http://test@www.example.com"

      val uri = Uri(url).withUser("test")

      uri.user should be("test")

      uri.toString should be (url2)
    }
}