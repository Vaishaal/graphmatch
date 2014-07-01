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
import scala.io.Source.fromFile
import argonaut._, Argonaut._

object Matcher {
  val BUILDING = 0
  val INTERSECTION = 1
  val ROAD = 2
  def query(query:String, maxLength:Int):Unit =  {

    val source = fromFile(query)
    val db = new Database()
    //TODO: Read in the attributes.

    val maxDepth = 20

    val lines = source.mkString
    source.close()

    val nodes = lines.decodeOption[List[QueryNode]].getOrElse(Nil)
    val matcher = new Matcher(db, nodes)

    // Decompose the query into all possible paths of a given length.
    val paths = matcher.getPaths(maxDepth)

    // Compute the costs of each path.
    val costs = matcher.computeCost(paths, 0.5)

    // Use greedy set cover to compute the best paths decomposition.
    val coveringPaths = matcher.coverQuery(paths, costs)

    // Compute node level statistics.

    // Compute path level statistics.

    // Build joint search space graph.
  }

}

class Matcher (database: Database, nodeList: List[QueryNode]) {
  val db = database
  val nodes = MMap[Int, QueryNode]()
  for (node <- nodeList) {
    nodes(node.key) = node
  }

  private def computeCost (paths: List[List[QueryNode]], minProb: Double) : List[Double] = {
    val costs = ListBuffer[Double]()
    for ((path, i) <- paths.view.zipWithIndex) {
      costs += this.db.pIndexHist(this.labels(path), minProb)/(this.degree(path)*this.density(path))
    }
    costs.toList
  }

  private def coverQuery (paths: List[List[QueryNode]], costs: List[Double])
    : List[List[QueryNode]] = {
    val efficiency = ListBuffer[Double]()
    for ((cost, i) <- costs.view.zipWithIndex) {
      efficiency += paths(i).length/cost
    }
    val pathEfficiency = (paths, efficiency.toList).zipped.toList
    val sortedPaths = pathEfficiency.sortWith(
      (x: (List[QueryNode], Double), y: (List[QueryNode], Double)) => x._2 > y._2)
    val pathsIter = sortedPaths.toIterator
    val coveringPaths = ListBuffer[List[QueryNode]]()
    val covered = Set[Int]()
    var target = this.nodes.keySet
    for ((key, node) <- this.nodes) {
      if (node.nodeType == Matcher.ROAD) {
        target -= key
      }
    }
    while (covered != target && pathsIter.hasNext) {
      val nextPath = pathsIter.next
      coveringPaths += nextPath._1
      covered ++= (for (node <- nextPath._1) yield node.key)
    }
    coveringPaths.toList
  }

  private def getPaths (maxLength: Int) : List[List[QueryNode]] = {

    val visited = MMap[Int, Boolean]().withDefaultValue(false)
    val paths = ListBuffer[ListBuffer[QueryNode]]()
    for ((key, node) <- this.nodes) {
      paths ++= getPathsHelper(maxLength, node, 0, visited)
    }
    val output = ListBuffer[List[QueryNode]]()
    for (path <- paths) {
      output += path.toList
    }
    output.toList
  }

  private def getPathsHelper (maxLength: Int, current: QueryNode, depth: Int, visited: MMap[Int, Boolean])
    : ListBuffer[ListBuffer[QueryNode]] = {
    var paths = ListBuffer[ListBuffer[QueryNode]]()
    if (depth < maxLength && current.edges.length > 0) {
      visited(current.key) = true
      for (edge <- current.edges) {
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

  private def labels (path: List[QueryNode]) : Array[Double] = {
    val stub = new Array[Double](1)
    stub(0) = 0.0
    stub
  }

  private def degree (path: List[QueryNode]) : Int = {
    var total = 0
    for (node <- path) {
    }
    total - 2*(path.length - 1)
  }

  private def density (path: List[QueryNode]) : Double = {
    var internalEdges = 0
    //TODO: If the graph is undirected we'll have to divide this by 2.
    for (node <- path) {
      for (edge <- node.edges) {
        if (path.contains(edge)) {
          internalEdges += 1
        }
      }
    }
    var result = 0.0
    if (path.length > 1) {
      result = 2.0*internalEdges/(path.length*(path.length - 1))
    } else {
      result = 1.0
    }
    result
  }
}


class Database () {
  def pIndexHist (pathLabels: Array[Double], minProb: Double) : Double = {
    1.0
  }
}

case class QueryNode(key: Int, nodeType: Int, edges: List[Int])

object QueryNode {
  implicit def QueryNodeCodecJson: CodecJson[QueryNode] =
    casecodec3(QueryNode.apply, QueryNode.unapply)("key", "nodeType", "edges")
}
