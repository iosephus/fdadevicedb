name := "FDADevices-SNAPSHOT"
version := "0.1"
scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.1" % "provided"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"
resolvers += Resolver.sonatypeRepo("public")


