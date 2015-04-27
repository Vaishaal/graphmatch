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
import collection.mutable.{ HashMap, MultiMap, Set }
import org.neo4j.graphdb.Traverser
import org.neo4j.graphdb.Node
import org.neo4j.graphalgo.GraphAlgoFactory
import org.neo4j.graphdb._
import org.neo4j.graphdb.traversal.Evaluator
import org.neo4j.kernel.Traversal._
import scala.language.implicitConversions
import sys.process._
import java.io.File
import com.mongodb.casbah.Imports._
import org.json4s.native.JsonMethods._
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.native.Serialization
import scala.collection.mutable.ListBuffer
import math._

case class GraphNode(
  key:Long,
  attr:Attributes,
  x:Double = -1,
  y:Double = -1
)
/* Definite Attributes are attributes that can be measured with
very low uncertainty and have a finite discrete set of acceptable values
e.g nodeType, intersection degree. KEEP THIS CLASS SMALL */

case class DefiniteAttributes(
  nodeType:Int,
  degree:Int
)

/* Attributes are values that are transferable from query to DB */
case class Attributes(
  nodeType:Int,
  height:Int = -1,
  length:Int = -1,
  roadClass:Int = -1,
  degree:Int = -1,
  angle:Double = -1
)

case class GraphPath(index:Int, road:Long = -1, weight:Double = 1)

class GenDb(db_path: String, nodes_path: String, edges_path: String, weights_path: String)
extends Neo4jWrapper
with EmbeddedGraphDatabaseServiceProvider
with Neo4jIndexProvider
with TypedTraverser {


  override def NodeIndexConfig = ("keyIndex", Some(Map("provider" -> "lucene", "type" -> "fulltext"))) ::
    ("degreeIndex", Some(Map("provider" -> "lucene", "type" -> "fulltext"))) ::
    ("heightIndex", Some(Map("provider" -> "lucene", "type" -> "fulltext"))) ::
    ("roadClassIndex", Some(Map("provider" -> "lucene", "type" -> "fulltext"))) ::
    ("kPartiteRoadIndex", Some(Map("provider" -> "lucene", "type" -> "fulltext"))) ::
    Nil
    def neo4jStoreDir = db_path
    val nodes_json = scala.io.Source.fromFile(nodes_path).mkString
    val edges_json = scala.io.Source.fromFile(edges_path).mkString
    val weights_json = scala.io.Source.fromFile(weights_path).mkString

    implicit val formats = Serialization.formats(NoTypeHints)
    val decodeNodes = Serialization.read[List[GraphNode]](nodes_json)
    println("NODES SIZE " + decodeNodes.size)
    val decodeEdges = Serialization.read[Map[String, List[Long]]](edges_json)
    val decodeWeights = Serialization.read[Map[String, List[Double]]](weights_json)
    val node_map = (for (p <- decodeNodes) yield (p.key, p)).toMap
    val N = 7
    val nodeIndex = getNodeIndex("keyIndex").get
    val degreeIndex = getNodeIndex("degreeIndex").get
    val heightIndex = getNodeIndex("heightIndex").get
    val roadClassIndex = getNodeIndex("roadClassIndex").get
    val roadIndex = getNodeIndex("kPartiteRoadIndex").get

    val LABELSIZE = 3
    val SENTINEL = -1
    val SINKKEY = -1
    val SOURCEKEY = -2
    val SOURCENODE = GraphNode(SOURCEKEY, Attributes(-1))
    val SINKNODE = GraphNode(SINKKEY, Attributes(-1))
    val (source, sink) =
      withTx {
        implicit neo =>
          val source = bindNode(SOURCENODE, createNode())
          val sink = bindNode(SINKNODE, createNode())
          val nodes = (for ((k, v) <- node_map) yield (k, bindNode(v, createNode()))).toMap
          for ((k,v) <- node_map) nodeIndex += (nodes(k), "key", k.toString)
          for ((k,v) <- node_map) {
            val node = nodes(k)
            source --> "NEXT_TO" --> node
            node --> "NEXT_TO" --> sink
            v.attr.nodeType match {
              case 0 => processBuilding(v, node)
              case 1 => processIntersection(v, node)
              case 2 => processRoad(v, node) } }
            (source, sink)
          }
    /* Binds everything in gn to neo4j node instance in node */
    def bindNode(gn: GraphNode, n:Node):Node = {
      n("key") = gn.key
      n("x") = gn.x
      n("y") = gn.y
      n("height") = gn.attr.height
      n("length") = gn.attr.length
      n("nodeType") = gn.attr.nodeType
      n("degree") = gn.attr.degree
      n("roadClass") = gn.attr.roadClass
      n("angle") = toDegrees(gn.attr.angle)
      n
    }

    def processBuilding(v:GraphNode, n:Node) = {
      heightIndex += (n, "height", v.attr.height.toString)
      for ((e:Long,i) <- decodeEdges.getOrElse(v.key.toString, Nil).zipWithIndex){
        val node = nodeIndex.get("key",e.toString).getSingle()
        val node_f = node_map.getOrElse(e,SOURCENODE)
        if (node_f != SOURCENODE){
          node_f.attr.nodeType match {
            case 0 => {
                       val rel:PropertyContainer = n --> "NEXT_TO" --> node <
                       val weight:Double = (decodeWeights.get(v.key.toString).get)(i)
                       rel("weight") = weight
                      }
            case 1 => {
                      val rel:PropertyContainer = n --> "NEXT_TO" --> node <
                      val weight:Double = (decodeWeights.get(v.key.toString).get)(i)
                       rel("weight") = weight
                      }
            case 2 => {
                      val rel:PropertyContainer = n --> "ON" --> node <
                      }
          }
        }
      }
    }

    def processRoad(v:GraphNode, n:Node) = {
      roadClassIndex += (n, "roadClass", v.attr.roadClass.toString)
      for (e:Long <- decodeEdges.getOrElse(v.key.toString, Nil)){
        val node = nodeIndex.get("key",e.toString).getSingle()
        val node_f = node_map.getOrElse(e,SOURCENODE)
        if (node_f != SOURCENODE){
          node_f.attr.nodeType match {
            case 0 => //
            case 1 => //
            case 2 => assert(false,"Blocks should not touch blocks")
          }
        }
      }
    }

    def processIntersection(v:GraphNode, n:Node) = {
      degreeIndex += (n, "degree", v.attr.degree.toString)
      for ((e:Long,i) <- decodeEdges.getOrElse(v.key.toString, Nil).zipWithIndex) {
        val node = nodeIndex.get("key",e.toString).getSingle()
        val node_f = node_map.getOrElse(e,SOURCENODE)
        if (node_f != SOURCENODE){
          node_f.attr.nodeType match {
            case 0 =>
            {
              val rel = n --> "NEXT_TO" --> node <
              val weight:Double = (decodeWeights.get(v.key.toString).get)(i)
              rel("weight") = weight
            }
            case 1 => assert(false, "Intersections should not be connected to other intersections")
            case 2 =>
            {
              val rel:PropertyContainer = n --> "ON" --> node <
            }
          }
          }
        }
      }
    // Find all paths
    val expander = pathExpanderForTypes("NEXT_TO", Direction.OUTGOING)
    val finder = GraphAlgoFactory.allSimplePaths(expander, N)
    val NODETYPE = 1
    val allPaths:List[Path] = finder.findAllPaths(source,sink).toList
    val pathEdges:Vector[List[Double]] =
        (allPaths map {path =>
          (path.drop(2).dropRight(2) filter { pathElem =>
            pathElem match {
              case y:Node => false
              case y:Relationship => true
            }
          } map { pathElem =>
            pathElem.asInstanceOf[Relationship]("weight").get.asInstanceOf[Double]
          }).toList
        }).to[Vector]
    val paths =
        (allPaths map {path =>
          (path filter { pathElem =>
            pathElem match {
              case y:Node => true
              case _ => false
            }
          } map { pathElem =>
            pathElem.asInstanceOf[Node]
          }).toList.drop(1).dropRight(1)
        })
    paths map (path => path.map(node =>
          if (node.attr.nodeType == Matcher.ROAD) {
            //assert(false, "ROADS CANT SHOW UP ON PATHS")
          }))
    // TODO: The traversal library currently used by Neo4j-Scala is deprecated
    // Fix in forked repo (it is a quite simple fix)
    println(paths.size)
    val singleRoadPathsSet =
    (paths.zipWithIndex map { pathAndIndex =>
      val path = pathAndIndex._1;
      (path,
        (path filter {node =>
          node.getRelationships("ON", Direction.OUTGOING).size == 1 &&
          node.attr.nodeType != Matcher.INTERSECTION
        }
        map {node =>
        node.traverse(Traverser.Order.BREADTH_FIRST,
                     {tp: TraversalPosition =>
                       tp.depth > 0
                     },
                     {tp:TraversalPosition =>
                        tp.currentNode().attr.nodeType == Matcher.ROAD
                     },
                     "ON",
                     Direction.OUTGOING
                     )
        } map (t => t.toList)).toList, pathEdges(pathAndIndex._2).foldLeft(1.0)
            {(b,a) =>
              b*a
            })
    } filter {
      pathR =>
      (pathR._2.flatten.toSet.size == 1 &&
         pathR._1.head.attr.nodeType != Matcher.ROAD)
    } map {
      pathR => (pathR._1, pathR._2.flatten.head, pathR._3)
    }).toSet
    val singleRoadPaths =
      (singleRoadPathsSet filter (path => path._1.size > 0)
      map { path =>
        if (path._1.head.key > path._1.last.key) (path._1.reverse, path._2, path._3) else (path._1, path._2, path._3)
      }).toList
    /* TODO: Find out why there are duplicates in the above list */
    println(singleRoadPathsSet.size)
    println(singleRoadPaths.size)
    // Create a histogram of paths store in mongoDB
    val mongoClient = MongoClient()
    val db = mongoClient("graphmatch")
    // This collection maps from a integer (a path id) to a list of node_ids
    val pathLookup = db("pathLookup")
    val index2Path = new collection.mutable.HashMap[Int, GraphPath]

    // Histogram of definite attributes
    val histMap = new collection.mutable.HashMap[List[DefiniteAttributes],Int]

    // Map from definite attributes to a list of realizations of those attributes.
    // Each value in the int corresponds to a path in the collection pathLookup
    val pathMap = new collection.mutable.HashMap[List[DefiniteAttributes], List[Int]]
    val histogramCol = db("histogram")
    val pathCol = db("paths")

    for ((p,i) <- singleRoadPaths.zipWithIndex) {
      val pathKeys = p._1 map (_.key)
      val key = p._1 map (node => Attribute2DefiniteAttribute(node.attr))
      val path = GraphPath(i,p._2.key, p._3)
      val path_obj = MongoDBObject("_id"->i,"path"->(p._1 map (_.key)), "road" -> p._2.key, "weight" -> p._3)
      pathLookup.insert(path_obj)
      index2Path(i) = path
      histMap(key) = histMap.getOrElse(key,0) + 1
      pathMap(key) = pathMap.getOrElse(key, Nil) ::: (i :: Nil)
    }
    for ((k,v) <- histMap) {
      val kJson = Serialization.write(k)
      histogramCol.insert(MongoDBObject("_id" -> kJson, "count" -> v))
      pathCol.insert(MongoDBObject("_id" -> kJson, "paths" -> pathMap(k)))
    }
    println(histogramCol.count() + " unique paths entered in DB")
    shutdown(ds)
}
