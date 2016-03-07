package com.szadowsz.grainne.tools.reflection

import java.lang.reflect.Modifier

/**
  * Created by zakski on 02/03/2016.
  */
object ReflectionUtil {

  def findPublicMethods(c: Class[_]) = c.getMethods.filter(m => Modifier.isPublic(m.getModifiers))

  def findJavaStyleGetters(c: Class[_]) = findPublicMethods(c).filter(_.getName.startsWith("get" ))
}
