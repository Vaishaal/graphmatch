/*
Copyright (c) 2011, 2012, 2013 The Regents of the University of
California (Regents). All Rights Reserved.  Redistribution and use in
source and binary forms, with or without modification, are permitted
provided that the following conditions are met:

 * Redistributions of source code must retain the above
 copyright notice, this list of conditions and the following
 two paragraphs of disclaimer.
 * Redistributions in binary form must reproduce the above
 copyright notice, this list of conditions and the following
 two paragraphs of disclaimer in the documentation and/or other materials
 provided with the distribution.
 * Neither the name of the Regents nor the names of its contributors
 may be used to endorse or promote products derived from this
 software without specific prior written permission.

 IN NO EVENT SHALL REGENTS BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT,
 SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS,
 ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF
 REGENTS HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 REGENTS SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT
 LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 A PARTICULAR PURPOSE. THE SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF
 ANY, PROVIDED HEREUNDER IS PROVIDED "AS IS". REGENTS HAS NO OBLIGATION
 TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR
 MODIFICATIONS.
 */

package com.finder.graphmatch
import Implicits._
import scala.util.parsing.json._
import eu.fakod.neo4jscala._
import sys.ShutdownHookThread
import scala.collection.JavaConversions.{asScalaIterator=>_,_}
import org.neo4j.graphdb.Traverser
import org.neo4j.graphdb.Node
import org.neo4j.graphalgo.GraphAlgoFactory
import org.neo4j.graphdb._
import org.neo4j.kernel.Traversal._
import scala.language.implicitConversions
import sys.process._
import java.io.File
import com.mongodb.casbah.Imports._
import org.json4s.native.JsonMethods._
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.native.Serialization




case class Feature(nodeType: Int,
  key: Int,
  x: Option[Double],
  y: Option[Double],
  height: Option[Int],
  length: Option[Int],
  degree: Option[Int],
  roadClass: Option[Int],
  edges: Option[List[Int]])

case class FeatureDefaults(nodeType: Int,
  key:Int,
  x:Double,
  y:Double,
  height:Int,
  length:Int,
  roadClass:Int,
  degree:Int)

case class Attributes(nodeType: Int, height: Int, length: Int, roadClass: Int, degree: Int) {
  def === (that: FeatureDefaults) : Boolean = {
    val tmp = Attributes(that.nodeType,
      that.height,
      that.length,
      that.roadClass,
      that.degree)
    this == tmp
  }

  def === (that: Feature) : Boolean = {
    val tmp = f2f(that)
    this === tmp
  }
}

object Attributes {
  def fromFeature(f: Feature) : Attributes = {
    val tmp = f2f(f)
    Attributes(tmp.nodeType,
      tmp.height,
      tmp.length,
      tmp.roadClass,
      tmp.degree)
  }
}

class GenDb(db_path: String, json_path: String) extends Neo4jWrapper with EmbeddedGraphDatabaseServiceProvider with Neo4jIndexProvider with TypedTraverser {


  override def NodeIndexConfig = ("keyIndex", Some(Map("provider" -> "lucene", "type" -> "fulltext"))) ::
    ("degreeIndex", Some(Map("provider" -> "lucene", "type" -> "fulltext"))) ::
    ("heightIndex", Some(Map("provider" -> "lucene", "type" -> "fulltext"))) ::
    ("roadClassIndex", Some(Map("provider" -> "lucene", "type" -> "fulltext"))) ::
    Nil

    def neo4jStoreDir = db_path
    val graph_json = scala.io.Source.fromFile(json_path).mkString

    implicit val formats = Serialization.formats(NoTypeHints)
    val decode = Serialization.read[List[Feature]](graph_json)

    val node_map = (for (p <- decode) yield (p.key, p)).toMap
    val N = 11
    val nodeIndex = getNodeIndex("keyIndex").get
    val degreeIndex = getNodeIndex("degreeIndex").get
    val heightIndex = getNodeIndex("heightIndex").get
    val roadClassIndex = getNodeIndex("roadClassIndex").get
    val LABELSIZE = 3
    val SINKKEY = -1
    val SOURCEKEY = -2

    val (source, sink) =
      withTx {
        implicit neo =>
          val source = createNode
          val sink = createNode
          source("nodeType") = -1
          source("height") = -1
          source("degree") = -1
          source("roadClass") = -1
          source("key") = SOURCEKEY
          sink("nodeType") = -1
          sink("degree") = -1
          sink("height") = -1
          sink("roadClass") = -1
          sink("key") = SINKKEY
          val nodes = (for ((k, v) <- node_map) yield (k, createNode(f2f(v))))
          for ((k,v) <- node_map) nodeIndex += (nodes(k), "key", k.toString)
          for ((k,v) <- node_map) {
            val node = nodes(k)
            source --> "NEXT_TO" --> node
            node --> "NEXT_TO" --> sink
            v.nodeType match {
              case 0 => processBuilding(v, node)
              case 1 => processIntersection(v, node)
              case 2 => processRoad(v, node)
}
            }
            (source, sink)
          }
    def processBuilding(v:Feature, n:Node) = {
      /* TODO: Use monads here */
      heightIndex += (n, "height", v.height.getOrElse(-1).toString)
      for (e:Int <- v.edges.get) {
        val node = nodeIndex.get("key",e.toString).getSingle()
        val node_f = node_map.get(e)
        if (node_f.nonEmpty) {
          node_f.get.nodeType match {
            case 0 => n --> "NEXT_TO" --> node
            case 1 => n --> "NEXT_TO" --> node
            case 2 => n --> "ON" --> node
      }
          }
        }
      }
    def processRoad(v:Feature, n:Node) = {
      roadClassIndex += (n, "roadClass", v.roadClass.getOrElse(-1).toString)
    }
    def processIntersection(v:Feature, n:Node) = {
      /* TODO: Use monads here */
      degreeIndex += (n, "degree", v.degree.getOrElse(-1).toString)
      for (e:Int <- v.edges.get) {
        val node = nodeIndex.get("key",e.toString).getSingle()
        val node_f = node_map.get(e)
        if (node_f.nonEmpty) {
          node_f.get.nodeType match {
            case 0 => n --> "NEXT_TO" --> node
            case 1 => //
            case 2 => n --> "CONNECTS" --> node
    }
          }
        }
      }
    // Find all paths
    val expander = pathExpanderForTypes("NEXT_TO", Direction.OUTGOING)
    val finder = GraphAlgoFactory.allSimplePaths(expander, N);
    val paths:List[List[List[Int]]]  = finder.findAllPaths(source,sink).map(x => x.filter({
      y => y match {
        case y:Node => true
        case _ => false
    }
    // Apparentely asInstanceOf is bad practice in Scala but no known workaround
    // TODO: Implement implicits so this isn't so ugly
    }).map(z => z.getProperty("key").asInstanceOf[Int]::
                z.getProperty("nodeType").asInstanceOf[Int]::
                z.getProperty("height").asInstanceOf[Int]::
                z.getProperty("degree").asInstanceOf[Int]::
                z.getProperty("roadClass").asInstanceOf[Int]::
                Nil).drop(1).dropRight(1).toList).toList
    // Create a histogram of paths store in mongoDB
    val mongoClient = MongoClient()
    val map = new collection.mutable.HashMap[List[List[Int]],Int]

    /* TODO: Change the value of this HM to a mutable collection */

    val pathMap = new collection.mutable.HashMap[List[List[Int]], List[List[Int]]]

    val db = mongoClient("graphmatch")
    val histogramCol = db("histogram")
    val pathCol = db("paths")
    for (p <- paths) {
      val key = p.map(_.tail)
      val value = p.map(_.dropRight(LABELSIZE).head)
      map(key) = map.getOrElse(key,0) + 1
      pathMap(key) = pathMap.getOrElse(key, Nil) ::: (value :: Nil)
    }
    for ((k,v) <- map) {
      val kJson = compact(render(k))
      val o = MongoDBObject("_id" -> kJson, "count" -> v)
      histogramCol.insert(o)
      val po = MongoDBObject("_id" -> kJson, "paths" -> compact(render(pathMap(k))))
      pathCol.insert(po)
    }

    println(histogramCol.count() + " unique paths entered in DB")
    shutdown(ds)
}

