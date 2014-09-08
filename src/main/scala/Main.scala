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
import java.io.File
import scopt._
import sys.process._
import com.mongodb.casbah.Imports._

import org.neo4j.graphdb.Node

import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization

import scala.language.implicitConversions

case class Config(gen: Boolean = false,
                  reset: Boolean = false,
                  queryNodes: String = "",
                  queryEdges: String = "",
                  dbpath: String = "/tmp/test.db",
                  nodejsonpath: String = "bg.nodes.json",
                  edgejsonpath: String = "bg.edges.json",
                  weightjsonpath: String = "bg.weights.json"
                  )
object Main extends App {

val parser = new scopt.OptionParser[Config]("graphmatch") {
  head("graphmatch","0.1")
  opt[Unit]('g', "gen")
  .action { (_, c) =>  c.copy(gen = true)}
  .text("Generates a neo4j and mongodb database using bg.json specified by db_json, WARNING: This will error if a db already exists")

  opt[Unit]('r', "reset")
  .action { (_, c) =>  c.copy(reset = true)}
  .text("Deletes existing neo4j and mongodb database")

  opt[String]('q', "query")
  .action { (x, c) => c.copy(queryNodes = x + ".nodes.json").copy(queryEdges = x +  ".edges.json")}
  .text("Query location")

  opt[String]('n', "neo4jpath")
  .action { (x, c) =>  c.copy(dbpath = x)}
  .text("Location of neo4j database ")

  opt[String]("db")
  .action { (x, c) => c.copy(nodejsonpath = x + ".nodes.json").copy(edgejsonpath = x +  ".edges.json").copy(weightjsonpath = x + ".weights.json")}
  .text("location of db json")

}


parser.parse(args, Config()) map {
  config => if (config.reset) {
                               ("rm -rf " + config.dbpath).!!
                               MongoClient()("graphmatch").dropDatabase()
                              }
            if (config.gen) { new GenDb(config.dbpath, config.nodejsonpath, config.edgejsonpath, config.weightjsonpath)
                              println("Data base successfully generated")
                            }
            if (config.queryNodes != "") Matcher.query(config.queryNodes,config.queryEdges, config.dbpath)
  }
}

object Implicits {

val db = MongoClient()("graphmatch")

implicit def Node2GraphNode(n:Node):GraphNode = {
    val attrs = Attributes(nodeType=n.getProperty("nodeType").asInstanceOf[Int],
                height=n.getProperty("height").asInstanceOf[Int],
                length=n.getProperty("length").asInstanceOf[Int],
                roadClass=n.getProperty("roadClass").asInstanceOf[Int],
                degree=n.getProperty("degree").asInstanceOf[Int],
                angle=n.getProperty("angle").asInstanceOf[Double])
    val node = GraphNode(key=n.getProperty("key").asInstanceOf[Long],
                         x=n.getProperty("x").asInstanceOf[Double],
                         y=n.getProperty("y").asInstanceOf[Double],
                         attr=attrs)
    node
}


implicit def Attribute2DefiniteAttribute(a:Attributes):DefiniteAttributes = {
  DefiniteAttributes(a.nodeType, a.degree)
}

// Converts a GraphPath to List of ints
implicit def GraphPath2NodeKeyList(p:GraphPath):List[Long] = {
  val pathLookup = db("pathLookup")
  val po = MongoDBObject("_id" -> p.index)
  val path = pathLookup.findOne(po).map(_.getAs[List[Long]]("path")).flatten.getOrElse(Nil)
  path
}

implicit val formats = Serialization.formats(NoTypeHints)

}
