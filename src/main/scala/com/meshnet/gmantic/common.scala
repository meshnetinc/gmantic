package com.meshnet

import org.openrdf.model.URI
import com.mysema.rdfbean.`object`.{SessionImpl, ConstructorVisitor}
import org.objectweb.asm.ClassReader
import com.mysema.rdfbean.model.{RDF, UID, Format, NODE}
import com.mysema.rdfbean.`object`.MappedClass
import com.mysema.rdfbean.sesame.SesameRepository
import scala.collection.JavaConversions._

package object gmantic {

  def percentsToURIs(s:String, ns:String) = {
    s.split(" ").map(f => if (f.startsWith("%")) "<" + ns + f.replace("%","") + ">" else f).mkString(" ")
  }

  def findClass(d:String, mappedClasses:java.util.Set[MappedClass], ns:String) = {
    import collection.JavaConversions._
    mappedClasses.find(f => f.getUID.toString.replaceAllLiterally(ns, "") == d)
  }

  def getIndicies(c:Class[_]*):Seq[String] = {
    c.map(f => f.getSimpleName.toLowerCase)
  }

  def resultsList[B](l:java.util.Map[java.lang.Float, B]):List[(java.lang.Float, B)] = {
    l.toSeq.sortBy(_._1).toList.reverse
  }

  def resultsListJ[B](l:java.util.Map[java.lang.Float, B]):java.util.List[(java.lang.Float, B)] = {
    l.toSeq.sortBy(_._1).toList.reverse
  }

  def getIndicies(c:Array[Class[_]]):Seq[String] = getIndicies(c:_*)

  def getIndiciesJ(c:Array[Class[_]]):Array[String] = getIndicies(c:_*).toArray

}