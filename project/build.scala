import sbt._

object build extends sbt.Build {
  lazy val root = Project("root", file(".")) dependsOn(geoscript)
  lazy val geoscript = ProjectRef(uri("git://github.com/dwins/geoscript.scala"), "library")
}

