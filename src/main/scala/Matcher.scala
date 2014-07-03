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
import scala.util.parsing.json._
import scala.collection.mutable.{Map => MMap}
import scala.collection.mutable.Stack
import scala.collection.mutable.Set
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.Range
import com.mongodb.casbah.Imports._
import scala.io.Source.fromFile
import argonaut._, Argonaut._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.json4s._

object Matcher {
  val BUILDING = 0
  val INTERSECTION = 1
  val ROAD = 2
  val MAX_PATH_LENGTH = 10
  def query(query:String, maxLength:Int=MAX_PATH_LENGTH):Unit =  {

    val source = fromFile(query)

    val maxDepth = 20

    val lines = source.mkString
    source.close()
    val nodes = lines.decodeOption[List[Feature]].getOrElse(Nil)
    val matcher = new Matcher(nodes)

    // Decompose the query into all possible paths of a given length.
    val paths = matcher.getPaths(maxDepth)

    // Compute the costs of each path.
    val costs = matcher.computeCost(paths, 0.5)

    // Use greedy set cover to compute the best paths decomposition.
    val coveringPaths = matcher.coverQuery(paths, costs)

    // Compute node level statistics and get candidate nodes.
    val candidateNodes = matcher.getCandidateNodes()

    // Compute path level statistics and get candidate paths.
    val candidatePaths = matcher.getCandidatePaths(candidateNodes, coveringPaths)

    // Build joint search space graph.
  }

}

class Matcher (nodeList: List[Feature]) {
  val nodes = MMap[Int, Feature]()
  for (node <- nodeList) {
    nodes(node.key) = node
  }

  private def computeCost (paths: List[List[Feature]], minProb: Double) : List[Double] = {
    val costs = ListBuffer[Double]()
    for ((path, i) <- paths.view.zipWithIndex) {
      costs += this.pIndexHist(path, minProb)/(this.degree(path)*this.density(path))
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
    val covered = Set[Int]()
    var target = this.nodes.keySet
    for ((key, node) <- this.nodes) {
      if (node.nodeType == Matcher.ROAD) {
        target -= key
      }
    }
    while (covered != target && pathsIter.hasNext) {
      val nextPath = pathsIter.next
      var include = false
      for (node <- nextPath._1) {
        if (!covered.contains(node.key)) {
          include = true
        }
      }
      if (include) {
        coveringPaths += nextPath._1
        covered ++= (for (node <- nextPath._1) yield node.key)
      }
    }
    coveringPaths.toList
  }

  private def pIndexHist(path: List[Feature], minProb: Double) : Int =
  {
    val key = path.map(x => (x.nodeType::x.height.getOrElse(-1)::x.degree.getOrElse(-1)::Nil))
    val db = MongoClient()("graphmatch")
    val col = db("histogram")
    val kJson = compact(render(key))
    val o = MongoDBObject("_id" -> kJson)
    val result = col.findOne(o)
    /* TODO: DO NOT USE asInstanceOf here */
    val count = result.map(x => x.getAs[Int]("count")).getOrElse(Some(0)).getOrElse(0)
    if (count == 0) {
      Int.MaxValue
    } else {
      count
    }
  }

  private def pIndex(path: List[Feature], minProb: Double) : List[List[Feature]] =
  {
    List[List[Feature]]()
  }

  private def getCandidateNodes() : List[Feature] = {
    List[Feature]()
  }

  private def getCandidatePaths(candidateNodes: List[Feature], coveringPaths: List[List[Feature]]) : List[List[Feature]] = {
    List[List[Feature]]()
  }

  private def getPaths (maxLength: Int) : List[List[Feature]] = {

    val visited = MMap[Int, Boolean]().withDefaultValue(false)
    val paths = ListBuffer[ListBuffer[Feature]]()
    for ((key, node) <- this.nodes) {
      if (node.nodeType != Matcher.ROAD) {
        paths ++= getPathsHelper(maxLength, node, 0, visited)
      }
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
      total += node.edges.get.length
    }
    total - 2*(path.length - 1)
  }

  private def density (path: List[Feature]) : Double = {
    var internalEdges = 0
    //TODO: If the graph is undirected we'll have to divide this by 2.
    for (node <- path) {
      for (edge <- node.edges.get) {
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

}


