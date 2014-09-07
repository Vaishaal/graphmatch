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
import collection.mutable.{ HashMap, MultiMap, Set}
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
import math._
import org.geoscript.layer._
import org.geoscript.projection._
import org.geoscript.feature._
import org.geoscript.workspace._
import org.geoscript.geometry._
import org.geoscript.feature.builder._
import org.geoscript.geometry.builder._

import com.thesamet.spatial._

object Matcher {
  type KPartiteGraph = (HashMap[List[GraphNode],Node], HashMap[(Node,GraphPath), Node])
  val BUILDING = 0
  val INTERSECTION = 1
  val ROAD = 2
  val MAX_PATH_LENGTH = 10

  def distance(p1:(Double, Double), p2:(Double,Double)) = {
    val dx = p1._1 - p2._1
    val dy = p1._2 - p2._2
    val d = sqrt((pow((p1._1 - p2._1),2) + pow((p1._2 - p2._2),2)))
    d
  }


  def query(queryNodes:String, queryEdges:String, dbPath:String, maxLength:Int=MAX_PATH_LENGTH):Unit =  {
    val maxDepth = 20
    val nodes_json = scala.io.Source.fromFile(queryNodes).mkString
    val edges_json = scala.io.Source.fromFile(queryEdges).mkString
    val nodes = Serialization.read[List[GraphNode]](nodes_json)
    val edges = Serialization.read[Map[String,List[Int]]](edges_json)
    val matcher = new Matcher(nodes, edges, 0.5, dbPath)
    // Decompose the query into all possible paths of a given length.
    val paths = matcher.getPaths(maxDepth)
    val singleRoadPaths =
    paths filter {path =>
      (path filter {node: GraphNode =>
        node.attr.nodeType != Matcher.INTERSECTION
      } map {node:GraphNode =>
        (edges(node.key.toString) filter { edge:Int =>
          matcher.nodes(edge).attr.nodeType == Matcher.ROAD
        })
      }).flatten.toSet.size == 1
    }
    // Compute the costs of each path.
    val costs = matcher.computeCost(singleRoadPaths)
    // Use greedy set cover to compute the best paths decomposition.
    val coveringPaths = matcher.coverQuery(singleRoadPaths, costs)
    val roadPaths =
    coveringPaths map {path =>
      (path map {node:GraphNode =>
        (edges(node.key.toString) filter{ edge:Int =>
          matcher.nodes(edge).attr.nodeType == Matcher.ROAD
        }).head
      }).toSet.toList.head
    }
    // Map from  path to road
    val roadMap:Map[List[GraphNode], Int] = (coveringPaths zip roadPaths).toMap
    matcher.roadMap  = roadMap;
    // Use the union of all possible paths to get preliminary candidate nodes.
    val prelimNodes = matcher.nodesByPath(coveringPaths)
    // Compute node level statistics and get candidate nodes.
    val candidateNodes = matcher.getCandidateNodes(prelimNodes)
    // Compute path level statistics and get candidate paths.
    val candidatePaths:Map[List[GraphNode], List[GraphPath]] = matcher.getCandidatePaths(candidateNodes, coveringPaths)
    println("Candidate paths " + candidatePaths.keySet.toList.map(candidatePaths(_).size).toList)
    println("Candidate paths size " + candidatePaths.keySet.toList.map(candidatePaths(_).size).toList.sum)
    val kpg:KPartiteGraph = matcher.createKPartite(candidatePaths);
    matcher.addCloseEdges(kpg);
    matcher.addRoadEdges(kpg);
    matcher.prune(kpg, candidatePaths);
    val newCandidatePaths = matcher.kPartite2CandidatePaths(kpg, candidatePaths);
    println("Pruned Candidate paths " + newCandidatePaths.keySet.toList.map(newCandidatePaths(_).size).toList)
    matcher.generateShapefile(newCandidatePaths)
  }
}

class Matcher (nodeList: List[GraphNode], edges:Map[String,List[Int]], alpha: Double, dbPath: String, queryDistance:Int = 1, val utmZone:String = "EPSG:32637", val angleL:Double=357.287, val angleH:Double=194.614, val mainRoad:Int=(-1)) // scalastyle:ignore
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
  ShutdownHookThread {
    shutdown(ds)
  }
  val DENSITY = 10
  val DEFAULTCARDINALITY = 100
  val nodeIndex = getNodeIndex("keyIndex").get
  val roadIndex = getNodeIndex("kPartiteRoadIndex").get
  val db = MongoClient()("graphmatch")
  val hist_col = db("histogram")
  val path_col = db("paths")
  val path_lookup_col = db("pathLookup")
  val road_col = db("roads")
  val r_road_col = db("rRoads")
  var roadMap:Map[List[GraphNode], Int] = Map.empty
  def neo4jStoreDir = dbPath
  val nodes = MMap[Long, GraphNode]()
  for (node <- nodeList) {
    nodes(node.key) = node
  }
  val minProb = alpha // query threshold

  private def computeCost (paths: List[List[GraphNode]]) : List[Double] = {
    val costs = ListBuffer[Double]()
    for ((path, i) <- paths.view.zipWithIndex) {
      costs += this.pIndexHist(path)/(this.degree(path)*this.density(path))
    }
    costs.toList
  }

  private def coverQuery (paths: List[List[GraphNode]], costs: List[Double])
    : List[List[GraphNode]] = {
    val efficiency = ListBuffer[Double]()
    for ((cost, i) <- costs.view.zipWithIndex) {
      efficiency += paths(i).length/cost
    }
    val pathEfficiency = (paths, efficiency.toList).zipped.toList
    val sortedPaths = pathEfficiency.sortWith(
      (x: (List[GraphNode], Double), y: (List[GraphNode], Double)) => x._2 > y._2)
    val pathsIter = sortedPaths.toIterator
    val coveringPaths = ListBuffer[List[GraphNode]]()
    val covered = Set[Set[Long]]()
    var target = this.nodes.keySet.map(x => (for (e <- edges(this.nodes(x).key.toString))
      yield Set[Long](this.nodes(x).key, e)).toSet).foldLeft(covered.empty)((x,y) => x.union(y))
      .filter(_.map(this.nodes(_).attr.nodeType != Matcher.ROAD).reduce((x,y) => x && y))

    while (covered != target && pathsIter.hasNext) {
      val nextPath = pathsIter.next
      var include = false
      var prev = nextPath._1.head.key
      for (node <- nextPath._1.tail) {
        val curr = node.key
        if (!covered.contains(Set[Long](prev, curr))) {
          include = true
        }
        prev = curr
      }
      if (include) {
        coveringPaths += nextPath._1
        val nodes = nextPath._1.map(_.key)
        covered ++= (nodes.drop(1),nodes.dropRight(1)).zipped.toList.map(x => Set[Long](x._1,x._2))
      }
    }
    coveringPaths.toList
  }

  private def pIndexHist(path: List[GraphNode]) : Int =
  {
    val key = path.map(x => (DefiniteAttributes(degree=x.attr.degree,nodeType=x.attr.nodeType)))
    val kJson = Serialization.write(key)
    val o = MongoDBObject("_id" -> kJson)
    val result = hist_col.findOne(o)
    /* TODO: DO NOT USE asInstanceOf here */
    val count = result.map(x => x.getAs[Int]("count")).getOrElse(Some(0)).getOrElse(0)
    if (count == 0) {
      Int.MaxValue
    } else {
      count
    }
  }

  private def pIndex(path: List[GraphNode], minProb: Double) : List[GraphPath] =
  {
     val key = path.map(x => (DefiniteAttributes(degree=x.attr.degree,nodeType=x.attr.nodeType)))
     val kJson = Serialization.write(key)
     val o = MongoDBObject("_id" -> kJson)
     val result = path_col.findOne(o)
     val paths = result.map(_.getAs[List[Int]]("paths")).flatten.getOrElse(Nil)
     paths map (pi => GraphPath(pi, path_lookup_col.findOne(MongoDBObject("_id" -> pi)).map(_.getAs[Long]("road")).flatten.getOrElse(-1)))
  }

  private def getPaths (maxLength: Int) : List[List[GraphNode]] = {

    val visited = MMap[Long, Boolean]().withDefaultValue(false)
    val paths = ListBuffer[ListBuffer[GraphNode]]()
    for ((key, node) <- this.nodes) {
      if (node.attr.nodeType != Matcher.ROAD) {
        paths ++= getPathsHelper(maxLength, node, 0, visited)
      }
    }
    val output = ListBuffer[List[GraphNode]]()
    for (path <- paths) {
      output += path.toList
    }
    output.toList
  }

  private def getPathsHelper (maxLength: Int, current: GraphNode, depth: Int, visited: MMap[Long, Boolean])
    : ListBuffer[ListBuffer[GraphNode]] = {
    var paths = ListBuffer[ListBuffer[GraphNode]]()
    if (depth < maxLength && edges.contains(current.key.toString) ) {
      visited(current.key) = true
      for (edge <- edges(current.key.toString)) {
        if (!visited(edge) && this.nodes(edge).attr.nodeType != Matcher.ROAD) {

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

  private def getJoinCandidates(coveringPaths: List[List[GraphNode]]): MMap[Int, List[(Int, Int)]] =  {
    /* This function returns a map detailing which paths join with which other paths
     * The returned map's key corresponds to an index INTO coveringPaths, a key only exists if a
     * path joins with another path. The value corresponding to the key is a List of two-pules
     * The first value in the two-pule is the index INTO the path in which a new path is joined,
     * the second value in the two-pule is the index into coveringPaths, that denotes WHAT path is
     * being joined. I know its a little hair but its the most concise way I could think of representing this
     */
    val FORK_SIZE = 2
    // First we create a map from each Feature to the set of paths it belongs in.
    val pathMap = this.nodes.values.map((node => (node.key, coveringPaths.zipWithIndex
      .filter(path => path._1.map(_.key).toSet.contains(node.key))
      .map(path => path._2))))
      .toMap
      /*
      coveringPaths.zipWithIndex.map(path => path._1
        .map((node => (node.key, node.edges.getOrElse(Nil)))
        .filter(node => node._2.size > 2)))
        .filter(path => !path._1.isEmpty)
        .map()
        */

    MMap[Int, List[(Int, Int)]]()
  }

  private def degree (path: List[GraphNode]) : Int = {
    var total = 0
    for (node <- path) {
      total += edges(node.key.toString).length
    }
    total - 2*(path.length - 1)
  }

  private def density (path: List[GraphNode]) : Double = {
    var internalEdges = 0
    // TODO: If the graph is undirected we'll have to divide this by 2.
    for (node <- path) {
      for (edge <- edges(node.key.toString)){
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
  private def cardinality(node: Int, label: GraphNode) : Int = {
    DEFAULTCARDINALITY
  }

  private def ppu(node: Int, label: GraphNode) : Double = {
    // Only the edge probabilities.
    1.0
  }

  private def fpu(node: Int, label: GraphNode) : Double = {
    // Uses the probability of the feature as well.
    1.0
  }

  /***************************************
   * Node and Path Level Pruning Methods *
   ***************************************/

  private def nodesByPath(coveringPaths: List[List[GraphNode]]) : MMap[GraphNode, ListBuffer[Long]] = {
    // Returns the list of node IDs in the database that are in some
    // path that corresponds to a set cover path.
    val prelimNodes = MMap[GraphNode, ListBuffer[Long]]()
    for (path <- coveringPaths) {
      val fromDB = pIndex(path, this.minProb)
      for (prelimPath <- fromDB) {
        for ((p,i) <- prelimPath.zipWithIndex) {
          if (prelimNodes.contains(path(i))) {
            prelimNodes(path(i)) += prelimPath(i)
          } else {
            prelimNodes(path(i)) = ListBuffer[Long](prelimPath(i))
          }
        }
      }
    }
    prelimNodes
  }

  private def getCandidateNodes(prelim: MMap[GraphNode, ListBuffer[Long]]) : List[Long] = {
    // Returns the list of node IDs in the database that are the
    // remaining nodes from the node level pruning in the paper.
    val output = ListBuffer[Long]()
    val ROADCLASS = 3
    for ((queryNode, candidates) <- prelim) {
      val queryNeighborStats = MMap[Attributes, Int]().withDefaultValue(0)
      for (queryNeighbor <- edges(queryNode.key.toString)) {
        val qNN = this.nodes(queryNeighbor) // query neighbor node
        val key = qNN.attr
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

  private def getNodeNeighborInfo(nodeId: Long) : HashMap[Attributes,Long] = {
    val neighborMap = new HashMap[Attributes,Long]
    val nodeIndex = getNodeIndex("keyIndex").get
    val searchKey = 100
    val obj = nodeIndex.get("key", nodeId)
    if (obj.size() != 0) {
      val node = obj.getSingle()
      val neighbors =
        node.traverse(Traverser.Order.BREADTH_FIRST,
                     {tp: TraversalPosition =>
                       tp.depth >= 1
                     },
                     ReturnableEvaluator.ALL_BUT_START_NODE,
                     DynamicRelationshipType.withName("NEXT_TO"),
                     Direction.OUTGOING,
                     DynamicRelationshipType.withName("CONNECTS"),
                     Direction.OUTGOING,
                     DynamicRelationshipType.withName("ON"),
                     Direction.OUTGOING
                     ).toList
      for (n <- neighbors) {
        neighborMap(n.attr) = neighborMap.getOrElse(n.attr,0.toLong) + 1
      }
    } else {
      assert(false, "Queried nodeId that does not exist!")
    }
    neighborMap
  }

  private def getCandidatePaths(candidateNodes: List[Long], coveringPaths: List[List[GraphNode]])
    : Map[List[GraphNode], List[GraphPath]] = {
    // Returns a map from the paths in the set cover to the list of paths (by node ID)
    // in the database that correspond to the set cover paths, after path level pruning.
    val out = MMap[List[GraphNode], List[GraphPath]]()
    for (setPath <- coveringPaths) {
      val prelim = pIndex(setPath, this.minProb)
      val passed = ListBuffer[GraphPath]()
      for (path <- prelim) {
        val rightDirection = angleBetween(nodeIndex.get("key",path.road.toString).getSingle().attr.angle, angleL, angleH) || (roadMap(setPath) != mainRoad)
        if (rightDirection && checkPath(path.toList, candidateNodes)) {
          passed += path
        }
      }
      out(setPath) = passed.toList
    }
    out.toMap
  }

  private def checkPath(path: List[Long], candidateNodes: List[Long]) : Boolean = {
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
  /*
  * Creates kpartite graph with no edges
  * Returns tuple of maps, first maps query paths to neo4j nodes, other maps
  * database paths to neo4j nodes
  */
  def createKPartite(candidatePaths: Map[List[GraphNode], List[GraphPath]]) =
  {
    println("Building KPartite")
    val queryPaths = new HashMap[List[GraphNode],Node]()
    val realPaths =  new HashMap[(Node,GraphPath),Node]()
    withTx {
        implicit neo =>
        for ((qp,gps) <- candidatePaths) {
          val road = -1*roadMap(qp);
          queryPaths(qp) = createNode
          roadIndex += (queryPaths(qp), "road", road.toString)
          for (gp <- gps) {
            realPaths((queryPaths(qp),gp)) = createNode
            roadIndex += (realPaths(queryPaths(qp),gp), "road", gp.road.toString)
          }
        }
      }
    (queryPaths, realPaths)
  }
  def addRoadEdges(kpg:Matcher.KPartiteGraph)
  {
    println("Adding Road Edges")
    val queryPaths = kpg._1
    val realPaths = kpg._2
    val road:Stack[(Node,Node)] = new Stack()
    for ((qp, qpn) <- queryPaths) {
      val sameRoad = roadIndex.get("road",(-1*roadMap(qp)).toString).toSet
      for (qpn2 <- sameRoad) {
        if (qpn != qpn2) {
          road.push((qpn,qpn2))
        }
      }
    }

    for ((rp, rpn) <- realPaths) {
      val sameRoad = roadIndex.get("road",rp._2.road.toString).toSet
      for (rpn2 <- sameRoad) {
        if (rpn2 != rpn) {
          road.push((rpn,rpn2))
        }
      }
    }

  withTx {
      implicit neo =>
        for ((rpn,rpn2) <- road) {
          rpn --> "SAME_ROAD"  --> rpn2
       }
    }
  }

  /*
  * Adds "close" edges to kpartite graph
  */
  def addCloseEdges(kpg:Matcher.KPartiteGraph)
  {
    println("Adding close edges")
    val close:Stack[(Node,Node)] = new Stack()
    val queryPaths = kpg._1
    val realPaths = kpg._2
    for ((qp,qpn) <- queryPaths) {
      for ((qp2, qpn2) <- queryPaths) {
        close.push((qpn, qpn2))
      }
    }
    // Maps List of Neo4j nodes to a "SuperNode" representing that particular path
    val pathNodes:HashMap[(Node,List[Node]), Node] = realPaths.map(kv => ((kv._1._1,kv._1._2.map(nodeId => nodeIndex.get("key",nodeId.toString).getSingle())),kv._2))
    val mm = new HashMap[(Double,Double), Set[(List[Node],Node)]] with MultiMap[(Double,Double), (List[Node],Node)]
    var bindings = 0;
    for (((qpn,rp),rpn) <- pathNodes) {
      mm.addBinding((rp.head.x,rp.head.y),(rp,rpn))
      bindings +=1;
    }
    val tree = KDTree.fromSeq(mm.keySet.toList)
    for (((qpn,rp),rpn) <- pathNodes) {
      val point = (rp.head.x,rp.head.y)
      val points = tree.findNearest(point,DENSITY)
      for (otherPoint <- points; rpn2 <- mm(otherPoint)) {
        if (rp != rpn2._1) {
          close.push((rpn,rpn2._2))
        } else{
          println("NO SELF EDGES")
        }

      }
    }
    withTx {
      implicit neo =>
        for ((rpn,rpn2) <- close) {
          rpn --> "CLOSE"  --> rpn2
       }
    }
  }

  /*
   * Prunes kpartite graph by removing nodes that do not have edges to
   * the partition they are supposed to connect to. Prunes until graph doesn't change.
   */
  def prune(kpg:Matcher.KPartiteGraph, candidatePaths: Map[List[GraphNode], List[GraphPath]])
  {
    println("PRUNING")
    val queryPaths:HashMap[List[GraphNode],Node] = kpg._1
    val realPaths:HashMap[(Node,GraphPath),Node] = kpg._2
    val allPaths:List[GraphPath] = candidatePaths.values.flatten.toList
    val pathCount = allPaths.foldLeft(Map[GraphPath,Int]() withDefaultValue 0){
      (m,x) => m + (x -> (1 + m(x)))}
    val queryPathsR = queryPaths.map(_.swap)
    val realPathsR = realPaths.map(_.swap)

    var pruned = false;
    val pruneStat = new HashMap[String,Int]()
    var i = 0;
    do {
      pruned = false
      i += 1
      for ((qp,qpn) <- queryPaths){
        val rps = candidatePaths(qp);
        for (r <- qpn.getRelationships()) {
          val qpn2 = r.getOtherNode(qpn);
          val rType = r.getType();
          val joinable:List[(Node,GraphPath)] = candidatePaths(queryPathsR(qpn2)) filter (rp => realPaths.contains((qpn2,rp))) map ( rp => (qpn2, rp));
            for (rp <- rps) {
              if (realPaths.contains((qpn,rp))) {
                val rpn = realPaths((qpn,rp))
                val joins:List[(Node,GraphPath)] = rpn.getRelationships(rType).map(r => realPathsR(r.getOtherNode(rpn))).toList
                var empty = (joins intersect joinable).isEmpty && !(joins.isEmpty && joinable.isEmpty)
                if (empty) {
                  pruneStat(rType.name())  = pruneStat.getOrElse(rType.name(),0) + 1
                pruned = true;
                 withTx {
                    implicit neo =>
                    rpn.getRelationships().map(r => r.delete());
                    rpn.delete();
                  }
                  realPaths.remove((qpn,rp));
                  realPathsR.remove(rpn);
                }
              }
          }
        }
      }
    } while (pruned);
    println(pruneStat)
  }
  /*
   * Converts a list of candidate paths to a set of candidate nodes
   */
  def graphPaths2NodeSet(candidatePaths:List[GraphPath]):Set[GraphNode] = {
    val nodeSet:Set[GraphNode] = Set.empty;
    for (path <- candidatePaths) {
      for (nodeKey <- path){
       val node:Node = nodeIndex.get("key",nodeKey.toString).getSingle()
       nodeSet.add(Node2GraphNode(node))
      }
    }
    nodeSet
  }
  /*
   * Converts kpartite to a candidatePaths map
   */
  def kPartite2CandidatePaths(kpg:Matcher.KPartiteGraph,  candidatePaths:Map[List[GraphNode], List[GraphPath]]) =
  {
    val queryPaths:HashMap[List[GraphNode],Node] = kpg._1
    val queryPathsR = queryPaths.map(_.swap)
    val realPaths:HashMap[(Node,GraphPath),Node] = kpg._2
    val newCandidatePaths = new HashMap[List[GraphNode], List[GraphPath]]()
    for ((k,v) <- realPaths.keySet) {
      newCandidatePaths(queryPathsR(k)) = v::newCandidatePaths.getOrElse(queryPathsR(k),Nil)
    }
    newCandidatePaths.toMap
  }
  def angleBetween(n:Double, a:Double, b:Double) = {
    val nn = (360 + (n % 360)) % 360;
    val an = (360 + (a % 360)) % 360;
    val bn = (360 + (b % 360)) % 360;
    if (an < bn) {
      an <= nn && nn <= bn;
    } else {
      an <= nn || nn <= bn;
    }
  }
  private def generateShapefile(candidatePaths: Map[List[GraphNode], List[GraphPath]]) = {
    val finalPaths:List[GraphPath] = candidatePaths.values.toList.flatten
    val finalNodes = graphPaths2NodeSet(finalPaths)
    val Place = "id".of[String] ~ "the_geom".of[Point]
    forceXYMode()
    val lonlat = lookupEPSG("EPSG:4326").get
    val utm = lookupEPSG(utmZone).get
    val transform = utm.to(lonlat)
    val (schema, mkFeature) = Place.schemaAndFactory("output",lonlat)
    val ws = Directory("output/")
    val layer = (ws.create(schema)).writable.get
    println(finalNodes.size)
    for (n <- finalNodes){
      layer += mkFeature(n.key.toString ~ transform(Point(n.x,n.y)))
    }
  }

  private def pathPU(path: List[Int]) : Double = {
    1.0
  }

}


