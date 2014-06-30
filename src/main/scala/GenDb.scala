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

import scala.util.parsing.json._
import eu.fakod.neo4jscala._
import sys.ShutdownHookThread
import argonaut._, Argonaut._
import scala.collection.JavaConversions.{asScalaIterator=>_,_}
import org.neo4j.graphdb.Traverser
import org.neo4j.graphdb.Node
import org.neo4j.graphalgo.GraphAlgoFactory
import scala.language.implicitConversions
import org.neo4j.graphdb._
import org.neo4j.kernel.Traversal._
import sys.process._

case class Feature(`type`: Int,
                    key: Int,
                    x: Option[Double],
                    y: Option[Double],
                    height: Option[Int],
                    length: Option[Int],
                    degree: Option[Int],
                    edges: Option[List[Int]])

case class Feature2(`type`:Int, key:Int, x:Double, y:Double, height:Int, length:Int,
                    degree:Int)

object Feature {
  implicit def featureCodecJson: CodecJson[Feature] =
  casecodec8(Feature.apply, Feature.unapply)("type","key","x", "y","height","length","degree", "edges")
}

class GenDb extends Neo4jWrapper with EmbeddedGraphDatabaseServiceProvider with Neo4jIndexProvider with TypedTraverser {
   ShutdownHookThread {
       shutdown(ds)
    }

    implicit def f2f(f:Feature) = {
        f match {
          case Feature(t,k,x,y,h,l,d,e) => Feature2(t,k,x.getOrElse(0),y.getOrElse(0),h.getOrElse(-1),l.getOrElse(-1),d.getOrElse(-1))
        }
    }


   override def NodeIndexConfig = ("keyIndex", Some(Map("provider" -> "lucene", "type" -> "fulltext"))) ::
                                  ("degreeIndex", Some(Map("provider" -> "lucene", "type" -> "fulltext"))) :: Nil
   def neo4jStoreDir = "/tmp/test.db"
   val graph_json = io.Source.fromFile("bg2.json").mkString
   val decode = graph_json.decodeOption[List[Feature]].getOrElse(Nil)
   val node_map = (for (p <- decode) yield (p.key, p)).toMap
   val N = 10
   val nodeIndex = getNodeIndex("keyIndex").get
   val degreeIndex = getNodeIndex("degreeIndex").get

   val (source, sink) =
   withTx {
      implicit neo =>
        val source = createNode
        val sink = createNode
        source("type") = -1
        sink("type") = -1
        val nodes = (for ((k, v) <- node_map) yield (k, createNode(f2f(v))))
        for ((k,v) <- node_map) nodeIndex += (nodes(k), "key", k.toString)
        for ((k,v) <- node_map) {
          val node = nodes(k)
          source --> "NEXT_TO" --> node
          node --> "NEXT_TO" --> sink
          v.`type` match {
            case 0 => processBuilding(v, node)
            case 1 => processIntersection(v, node)
            case 2 => processRoad(v, node)
          }
        }
        (source, sink)
  }
   def processBuilding(v:Feature, n:Node) = {
      for (e:Int <- v.edges.get) {
        val node = nodeIndex.get("key",e.toString).getSingle()
        val node_f = node_map.get(e)
        if (node_f.nonEmpty) {
          node_f.get.`type` match {
            case 0 => n --> "NEXT_TO" --> node
            case 1 => n --> "NEXT_TO" --> node
            case 2 => n --> "ON" --> node
          }
        }
      }
    }
   def processRoad(v:Feature, n:Node) = {}
   def processIntersection(v:Feature, n:Node) = {
      degreeIndex += (n, "degree", v.degree.get.toString)
      for (e:Int <- v.edges.get) {
        val node = nodeIndex.get("key",e.toString).getSingle()
        val node_f = node_map.get(e)
        if (node_f.nonEmpty) {
          node_f.get.`type` match {
            case 0 => n --> "NEXT_TO" --> node
            case 1 => //
            case 2 => n --> "CONNECTS" --> node
          }
        }
      }
   }
}
