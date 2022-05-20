import sbt._

object Dependencies {
  // Versions
  lazy val sparkVersion = "3.2.1"
  lazy val log4jVersion = "2.4.1"
  lazy val postgresVersion = "42.2.2"

  // Libraries
  // spark
  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion
  
  // logging
  val log4jApi = "org.apache.logging.log4j" % "log4j-api" % log4jVersion
  val log4jCore =  "org.apache.logging.log4j" % "log4j-core" % log4jVersion
  
  // postgres for DB connectivity
  val postgresql = "org.postgresql" % "postgresql" % postgresVersion

  // Projects
  val backendDeps = Seq(sparkCore, sparkSql, postgresql, log4jApi, log4jCore)
}
