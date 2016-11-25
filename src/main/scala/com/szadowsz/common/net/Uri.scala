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

import java.net.URI


/**
  * Companion object to handle some of the trickier maneuverings need to wrap the [[URI]] correctly.
  */
object Uri {

  private def parseUserInfo(userInfoOpt: Option[String]): (Option[String], Option[String]) = {
    userInfoOpt.map(userInfo => {
      val userEnd = userInfo.indexOf(':')
      val user = if (userEnd > 0) Option(userInfo.substring(0, userEnd)) else Option(userInfo)
      val pass = if (userEnd > 0) Option(userInfo.substring(userEnd + 1)) else None
      (user, pass)
    }).getOrElse((None, None))
  }

  private def parsePath(opt: Option[String]): Option[List[String]] = {
    val nOpt = opt.map(path => path.split('/').toList)
    if (opt.exists(path => path.endsWith("/"))) nOpt.map(l => l :+ "") else nOpt
  }

  private def parseQuery(opt: Option[String]): Option[List[(String, Option[String])]] = {
    opt.map(path => path.split('&').map(pair => {
      val split = pair.indexOf('=')
      split match {
        case x if x == pair.length - 1 => (pair.substring(0, split), Option(""))
        case -1 => (pair, None)
        case _ => (pair.substring(0, split), Option(pair.substring(split + 1)))
      }
    }).toList)
  }

  /**
    * Default Companion Object Functional Constructor.
    *
    * @param str the uri string
    * @return the wrapped URI
    */
  def apply(str: String): Uri = {
    val jURI = new URI(PercentEncoder.encode(str))
    val scheme = Option(jURI.getScheme)
    val (user, pass) = parseUserInfo(Option(jURI.getUserInfo))
    val host = Option(jURI.getHost)
    val port = if (jURI.getPort < 0) None else Option(jURI.getPort)
    val jPath = jURI.getPath
    val path = parsePath(if (jPath != null && jPath.length > 0) Option(jPath) else None)
    val query = parseQuery(Option(jURI.getQuery))
    val frag = Option(jURI.getFragment)
    Uri(scheme, user, pass, host, port, path, query, frag)
  }
}

/**
  * Scala Wrapper for [[URI]], built with URL Webscraping functions in mind.
  *
  * Created on 17/05/2016. Rewritten on 29/07/2016.
  *
  */
case class Uri(
                schemeOpt: Option[String],
                userOpt: Option[String],
                passwordOpt: Option[String],
                hostOpt: Option[String],
                portOpt: Option[Int],
                pathCompOpt: Option[List[String]],
                queryCompOpt: Option[List[(String, Option[String])]],
                fragmentOpt: Option[String]
              ) extends Serializable {

  def userInfoOpt: Option[String] = userOpt.map(_ + passwordOpt.map(":" + _).getOrElse(""))

  def pathOpt: Option[String] = pathCompOpt.map(_.mkString("/"))

  def queryOpt: Option[String] = queryCompOpt.map(_.map { case (k, v) => k + v.map("=" + _).getOrElse("") }.mkString("&"))

  def queryComp: List[(String, Option[String])] = queryCompOpt.getOrElse(Nil)

  def scheme: String = schemeOpt.orNull

  def user: String = userOpt.orNull

  def password: String = passwordOpt.orNull

  def userInfo: String = userInfoOpt.orNull

  def host: String = hostOpt.orNull

  def port: Int = portOpt.getOrElse(-1)

  def path: String = pathOpt.orNull

  def query : String = queryOpt.orNull

  def fragment : String = fragmentOpt.orNull

  /**
    * Copies this URL but with the scheme set as the given value.
    *
    * @param scheme the new scheme to set
    * @return a new URL with the specified scheme
    */
  def withScheme(scheme: String): Uri = copy(schemeOpt = Option(scheme))

  /**
    * Copies this URL but with the user set as the given value.
    *
    * @param user the new user to set
    * @return a new URL with the specified user
    */
  def withUser(user: String): Uri = copy(userOpt = Option(user))

  /**
    * Copies this URL but with the host set as the given value.
    *
    * @param host the new host to set
    * @return a new URL with the specified host
    */
  def withHost(host: String): Uri = copy(hostOpt = Option(host))

  /**
    * Copies this URL but with the port set as the given value.
    *
    * @param port the new port to set
    * @return a new URL with the specified port
    */
  def withPort(port: Int): Uri = copy(portOpt = Option(port))

  def appendRelative(rel: Uri): Uri = copy(pathCompOpt = rel.pathCompOpt, queryCompOpt = rel.queryCompOpt, fragmentOpt = rel.fragmentOpt)

  /**
    * Copies this URL but with the fragment set as the given value.
    *
    * @param path the new fragment to set
    * @return a new URL with the specified fragment
    */
  def withPath(path: String): Uri = copy(pathCompOpt = Uri.parsePath(Option(path)))


  def appendPath(pathPart: String): Uri = {
    val parts = Option((pathCompOpt.map(l => if(l.last.length == 0)l.dropRight(1)else l) ++ Uri.parsePath(Option(pathPart))).flatten.toList)
    copy(pathCompOpt = parts)
  }

  /**
    * Copies this URL but with the fragment set as the given value.
    *
    * @param query the new fragment to set
    * @return a new URL with the specified fragment
    */
  def withQuery(query: List[(String, Option[String])]): Uri = copy(queryCompOpt = Option(query))

  /**
    * Copies this URL but with the fragment set as the given value.
    *
    * @param query the new fragment to set
    * @return a new URL with the specified fragment
    */
  def withQuery(query: String): Uri = copy(queryCompOpt = Uri.parseQuery(Option(query)))

  def appendQuery(key: String, value: String): Uri = copy(queryCompOpt = Option(queryComp :+ (key -> Option(value))))

  def containsQueryKey(key: String): Boolean = {
    queryCompOpt.exists(l => l.toMap.contains(key))
  }

  /**
    * Copies this URL but with the fragment set as the given value.
    *
    * @param fragment the new fragment to set
    * @return a new URL with the specified fragment
    */
  def withFragment(fragment: String): Uri = {
    if (fragment.startsWith("#")) {
      copy(fragmentOpt = Option(fragment.substring(1)))
    } else {
      copy(fragmentOpt = Option(fragment))
    }
  }


  override def toString: String = new java.net.URI(scheme, userInfo, host, port, path, query, fragment).toString
}
