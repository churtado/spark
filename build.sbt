name := "HelloWorld"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq("org.apache.spark" % "spark-core_2.11" % "2.3.0",
   "org.apache.avro" % "avro-tools" % "1.8.2"
)