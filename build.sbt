organization := "me.lyh"
name := "scio-playground"
version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.11"
val scioVersion = "0.3.0"
scalacOptions ++= Seq("-target:jvm-1.8", "-deprecation", "-feature", "-unchecked")
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

libraryDependencies ++= Seq(
  "com.spotify" %% "scio-core" % scioVersion,
  "com.spotify" %% "scio-extra" % scioVersion,
  "com.spotify" %% "scio-test" % scioVersion % "test",
  "org.http4s" %% "http4s-blaze-client" % "0.15.11",
  "org.slf4j" % "slf4j-simple" % "1.7.21"
)

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
