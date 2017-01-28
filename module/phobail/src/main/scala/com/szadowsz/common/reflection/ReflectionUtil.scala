package com.szadowsz.common.reflection

import java.lang.reflect.Modifier

/**
  * Created by zakski on 02/03/2016.
  */
object ReflectionUtil {

  def findConstructor(c: Class[_], params: Class[_]*) = c.getConstructor(params: _*)

  def findPublicMethods(c: Class[_]) = c.getMethods.filter(m => Modifier.isPublic(m.getModifiers))

  def findJavaStyleGetters(c: Class[_]) = findPublicMethods(c).filter(_.getName.startsWith("get"))
}
