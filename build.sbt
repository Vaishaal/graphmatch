import org.scalastyle.sbt.PluginKeys._

import com.typesafe.sbt.SbtStartScript

seq(SbtStartScript.startScriptForClassesSettings: _*)

org.scalastyle.sbt.ScalastylePlugin.Settings

lazy val testScalaStyle = taskKey[Unit]("testScalaStyle")

testScalaStyle := {
  org.scalastyle.sbt.PluginKeys.scalastyle.toTask("").value
}

(stage in Compile) <<= (stage in Compile) dependsOn testScalaStyle

//scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xfatal-warnings")

name := "hello"

version := "1.0"

fork := true

scalaVersion := "2.10.3"

resolvers += "neo4j-public-repository" at "http://m2.neo4j.org/content/groups/everything/"


resolvers += Resolver.sonatypeRepo("public")

resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies ++= Seq(
  "org.neo4j" % "neo4j" % "1.9.4",
  "org.neo4j" % "neo4j-shell" % "1.9.4",
  "org.neo4j" % "neo4j-kernel" % "1.9.4",
  "org.neo4j" % "neo4j-rest-graphdb" % "1.9.3-SNAPSHOT",
  "eu.fakod" % "neo4j-scala_2.10" % "0.3.1-SNAPSHOT",
  "com.typesafe.slick" %% "slick" % "2.0.2",
  "org.json4s" %% "json4s-native" % "3.2.9",
  "com.github.scopt" %% "scopt" % "3.2.0",
  "com.thesamet" %% "kdtree" % "1.0.1"
)

libraryDependencies += "org.mongodb" %% "casbah" % "2.7.2"

libraryDependencies += "com.fasterxml.jackson.module" % "jackson-module-scala" % "2.0.2"


