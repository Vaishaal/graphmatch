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
import scala.collection.mutable.{Map => MMap}
import scala.collection.mutable.Stack
import scala.collection.mutable.Set
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.Range
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions.{asScalaIterator=>_,_}
import scala.language.implicitConversions
import scala.io.Source.fromFile

import com.mongodb.casbah.Imports._
import sys.ShutdownHookThread
import scala.io.Source.fromFile

import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization

import org.json4s.JsonDSL._

import org.neo4j.graphdb.Traverser
import org.neo4j.graphdb.Node
import org.neo4j.graphalgo.GraphAlgoFactory
import org.neo4j.graphdb._
import org.neo4j.kernel.Traversal._
import eu.fakod.neo4jscala._

object Matcher {
  val BUILDING = 0
  val INTERSECTION = 1
  val ROAD = 2
  val MAX_PATH_LENGTH = 10

  def query(query:String, dbPath:String, maxLength:Int=MAX_PATH_LENGTH):Unit =  {

    val source = fromFile(query)

    val maxDepth = 20

    val lines = source.mkString
    source.close()

    val nodes = Serialization.read[List[Feature]](lines)

    val matcher = new Matcher(nodes, 0.5, dbPath)

    // Decompose the query into all possible paths of a given length.
    val paths = matcher.getPaths(maxDepth)

    // Compute the costs of each path.
    val costs = matcher.computeCost(paths)

    // Use greedy set cover to compute the best paths decomposition.
    val coveringPaths = matcher.coverQuery(paths, costs)


    // Use the union of all possible paths to get preliminary candidate nodes.
    val prelimNodes = matcher.nodesByPath(coveringPaths)

    // Compute node level statistics and get candidate nodes.
    val candidateNodes = matcher.getCandidateNodes(prelimNodes)

    // Compute path level statistics and get candidate paths.
    val candidatePaths = matcher.getCandidatePaths(candidateNodes, coveringPaths)

    // Build joint search space graph.
  }

}

class Matcher (nodeList: List[Feature], alpha: Double, dbPath: String)
  extends Neo4jWrapper with EmbeddedGraphDatabaseServiceProvider with Neo4jIndexProvider with TypedTraverser {
  ShutdownHookThread {
    shutdown(ds)
  }
  val DEFAULTCARDINALITY = 100

  val db = MongoClient()("graphmatch")
  val hist_col = db("histogram")
  val path_col = db("paths")
  override def NodeIndexConfig = ("keyIndex", Some(Map("provider" -> "lucene", "type" -> "fulltext"))) ::
    ("degreeIndex", Some(Map("provider" -> "lucene", "type" -> "fulltext"))) ::
    ("heightIndex", Some(Map("provider" -> "lucene", "type" -> "fulltext"))) ::
 Nil
  def neo4jStoreDir = dbPath
  val nodes = MMap[Int, Feature]()
  for (node <- nodeList) {
    nodes(node.key) = node
  }
  val minProb = alpha // query threshold

  private def computeCost (paths: List[List[Feature]]) : List[Double] = {
    val costs = ListBuffer[Double]()
    for ((path, i) <- paths.view.zipWithIndex) {
      costs += this.pIndexHist(path)/(this.degree(path)*this.density(path))
    }
    costs.toList
  }

  private def coverQuery (paths: List[List[Feature]], costs: List[Double])
    : List[List[Feature]] = {
    val efficiency = ListBuffer[Double]()
    for ((cost, i) <- costs.view.zipWithIndex) {
      efficiency += paths(i).length/cost
    }
    val pathEfficiency = (paths, efficiency.toList).zipped.toList
    val sortedPaths = pathEfficiency.sortWith(
      (x: (List[Feature], Double), y: (List[Feature], Double)) => x._2 > y._2)
    val pathsIter = sortedPaths.toIterator
    val coveringPaths = ListBuffer[List[Feature]]()
    val covered = Set[Set[Int]]()
    var target = this.nodes.keySet.map(x => (for (e <- this.nodes(x).edges.getOrElse(Nil))
      yield Set[Int](this.nodes(x).key, e)).toSet).foldLeft(covered.empty)((x,y) => x.union(y))
      .filter(_.map(this.nodes(_).nodeType != Matcher.ROAD).reduce((x,y) => x && y))

    while (covered != target && pathsIter.hasNext) {
      val nextPath = pathsIter.next
      var include = false
      var prev = nextPath._1.head.key
      for (node <- nextPath._1.tail) {
        val curr = node.key
        if (!covered.contains(Set[Int](prev, curr))) {
          include = true
        }
        prev = curr
      }
      if (include) {
        coveringPaths += nextPath._1
        val nodes = nextPath._1.map(_.key)
        covered ++= (nodes.drop(1),nodes.dropRight(1)).zipped.toList.map(x => Set[Int](x._1,x._2))
      }
    }
    coveringPaths.toList
  }

  private def pIndexHist(path: List[Feature]) : Int =
  {
    val key = path.map(x => (x.nodeType::x.height.getOrElse(-1)::x.degree.getOrElse(-1)::x.roadClass.getOrElse(-1)::Nil))
    val kJson = compact(render(key))
    val o = MongoDBObject("_id" -> kJson)
    val result = hist_col.findOne(o)
    /* TODO: DO NOT USE asInstanceOf here */
    val count = result.map(x => x.getAs[Int]("count")).getOrElse(Some(0)).getOrElse(0)
    if (count == 1) { println(getNodeNeighborInfo(pIndex(path, minProb)(0)(0)))}
    if (count == 0) {
      Int.MaxValue
    } else {
      count
    }
  }

  private def pIndex(path: List[Feature], minProb: Double) : List[List[Int]] = {
    val key = path.map(x => (x.nodeType::x.height.getOrElse(-1)::x.degree.getOrElse(-1)::x.roadClass.getOrElse(-1)::Nil))
    val kJson = compact(render(key))
    val o = MongoDBObject("_id" -> kJson)
    val result = path_col.findOne(o)
    val paths = result.map(_.getAs[String]("paths")).flatten.getOrElse("")
    implicit val formats = Serialization.formats(NoTypeHints)
    val parsedPaths = Serialization.read[List[List[Int]]](paths)
    parsedPaths
  }

  private def getPaths (maxLength: Int) : List[List[Feature]] = {

    val visited = MMap[Int, Boolean]().withDefaultValue(false)
    val paths = ListBuffer[ListBuffer[Feature]]()
    for ((key, node) <- this.nodes) {
      paths ++= getPathsHelper(maxLength, node, 0, visited)
    }
    val output = ListBuffer[List[Feature]]()
    for (path <- paths) {
      output += path.toList
    }
    output.toList
  }

  private def getPathsHelper (maxLength: Int, current: Feature, depth: Int, visited: MMap[Int, Boolean])
    : ListBuffer[ListBuffer[Feature]] = {
    var paths = ListBuffer[ListBuffer[Feature]]()
    if (depth < maxLength && current.edges.nonEmpty ) {
      visited(current.key) = true
      for (edge <- current.edges.getOrElse(Nil)) {
        if (!visited(edge) && this.nodes(edge).nodeType != Matcher.ROAD) {
          paths ++= getPathsHelper(maxLength, this.nodes(edge), depth + 1, visited)
        }
      }
      for (path <- paths) {
        current +=: path
      }
      paths += ListBuffer(current)
      visited(current.key) = false
    } else {
      paths = ListBuffer(ListBuffer(current))
    }
    paths
  }


  private def degree (path: List[Feature]) : Int = {
    var total = 0
    for (node <- path) {
      total += node.edges.map(_.length).getOrElse(0)
    }
    total - 2*(path.length - 1)
  }

  private def density (path: List[Feature]) : Double = {
    var internalEdges = 0
    //TODO: If the graph is undirected we'll have to divide this by 2.
    for (node <- path) {
      for (edge <- node.edges.getOrElse(List[Int]())) {
        if (path.contains(this.nodes(edge))) {
          internalEdges += 1
        }
      }
    }
    var result = 0.0
    if (path.length > 1) {
      // Shoulve be 2*internalEdges but they get doulble counted in a UDGraph.
      result = internalEdges/(path.length*(path.length - 1.0))
    } else {
      result = 1.0
    }
    result
  }

  /***********************
   * Context Information *
   ***********************/
  // All of the below are w.r.t. N(v, sigma) the number of neighbors of
  // v that have label sigma.
  private def cardinality(node: Int, label: Feature) : Int = {
    DEFAULTCARDINALITY
  }

  private def ppu(node: Int, label: Feature) : Double = {
    // Only the edge probabilities.
    1.0
  }

  private def fpu(node: Int, label: Feature) : Double = {
    // Uses the probability of the feature as well.
    1.0
  }

  /***************************************
   * Node and Path Level Pruning Methods *
   ***************************************/

  private def nodesByPath(coveringPaths: List[List[Feature]]) : MMap[Feature, ListBuffer[Int]] = {
    // Returns the list of node IDs in the database that are in some
    // path that corresponds to a set cover path.
    val prelimNodes = MMap[Feature, ListBuffer[Int]]()
    for (path <- coveringPaths) {
      val fromDB = pIndex(path, this.minProb)
      for (prelimPath <- fromDB) {
        for (i <- Range(0, prelimPath.length)) {
          if (prelimNodes.contains(path(i))) {
            prelimNodes(path(i)) += prelimPath(i)
          } else {
            prelimNodes(path(i)) = ListBuffer[Int](prelimPath(i))
          }
        }
      }
    }
    prelimNodes
  }

  private def getCandidateNodes(prelim: MMap[Feature, ListBuffer[Int]]) : List[Int] = {
    // Returns the list of node IDs in the database that are the
    // remaining nodes from the node level pruning in the paper.
    val output = ListBuffer[Int]()
    for ((queryNode, candidates) <- prelim) {
      val queryNeighborStats = MMap[List[Int], Int]().withDefaultValue(0)
      for (queryNeighbor <- queryNode.edges.getOrElse(List[Int]())) {
        val qNN = this.nodes(queryNeighbor) // query neighbor node
        val key = List[Int](qNN.nodeType, qNN.height.getOrElse(-1), qNN.degree.getOrElse(-1), qNN.roadClass.getOrElse(-1))
        queryNeighborStats(key) += 1
      }
      for (candidate <- candidates) {
        val neighborStats = this.getNodeNeighborInfo(candidate)
        var passed = true
        for ((neighborLabel, count) <- neighborStats) {
          if (neighborStats(neighborLabel) < queryNeighborStats(neighborLabel)){
            passed = false
          }
        }
        if (passed) {
          output += candidate
        }
      }
    }
    output.toList
  }

  private def getNodeNeighborInfo(nodeId: Int) : HashMap[List[Int],Int] = {
    val neighborMap = new HashMap[List[Int],Int]
    val nodeIndex = getNodeIndex("keyIndex").get
    val obj = nodeIndex.get("key", nodeId)
    val node = obj.getSingle()
    val neighbors =
      node.doTraverse[FeatureDefaults](follow(BREADTH_FIRST) ->- "NEXT_TO" ->- "CONNECTS" ->- "")
    {
      case(_, tp) => tp.depth >= 1
    }
    {
    ALL_BUT_START_NODE
    }.toList
    val neighborProps =
        neighbors.map(x =>
                           x.nodeType::
                           x.height::
                           x.degree::
                           x.roadClass::
                         Nil)
    for (n <- neighborProps) {
      neighborMap(n) = neighborMap.getOrElse(n, 0) + 1
    }
    neighborMap
  }

  private def getCandidatePaths(candidateNodes: List[Int], coveringPaths: List[List[Feature]])
    : Map[List[Feature], List[List[Int]]] = {
    // Returns a map from the paths in the set cover to the list of paths (by node ID)
    // in the database that correspond to the set cover paths, after path level pruning.
    val out = MMap[List[Feature], List[List[Int]]]()
    for (setPath <- coveringPaths) {
      val prelim = pIndex(setPath, this.minProb)
      val passed = ListBuffer[List[Int]]()
      for (path <- prelim) {
        if (checkPath(path, candidateNodes)) {
          passed += path
        }
      }
      //out += (setPath, passed)
    }
    Map[List[Feature], List[List[Int]]]()
  }

  private def checkPath(path: List[Int], candidateNodes: List[Int]) : Boolean = {
    if (candidateNodes.contains(path(0))) {
      if (path.length == 1) {
        true
      } else {
        checkPath(path.slice(1, path.length), candidateNodes)
      }
    } else {
      false
    }
  }

  private def pathPU(path: List[Int]) : Double = {
    1.0
  }

}


