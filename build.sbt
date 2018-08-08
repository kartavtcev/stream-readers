name := "stream-readers"

version := "1.0"

scalaVersion := "2.12.6"

lazy val akkaVersion = "2.5.13"
lazy val akkaTypedVersion = "2.5.7"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream"          % akkaVersion,

  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream-testkit"  % akkaVersion     % Test,
  "org.scalatest" %% "scalatest" % "3.0.5" % Test
)
