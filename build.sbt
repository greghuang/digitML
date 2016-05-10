name := "digitML"

version := "1.0"

scalaVersion := "2.10.5"

resolvers ++= Seq(
  Resolver.defaultLocal,
  Resolver.mavenLocal,
  // make sure default maven local repository is added... Resolver.mavenLocal has bugs.
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
)

val sparkVersion = "1.6.1"

val sparkDependencyScope = "provided"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % sparkDependencyScope,
  "org.apache.spark" %% "spark-sql" % sparkVersion % sparkDependencyScope,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % sparkDependencyScope,
  "com.github.scopt" %% "scopt" % "3.4.0",
  "com.databricks" %% "spark-csv" % "1.4.0" % sparkDependencyScope,
  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
)