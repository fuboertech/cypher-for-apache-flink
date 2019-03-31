/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
// tag::full-example[]
package org.opencypher.spark.examples

import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.util.App

/**
  * Demonstrates usage patterns where Cypher and SQL can be interleaved in the
  * same processing chain, by using the tabular output of a Cypher query as a
  * SQL table, and using the output of a SQL query as an input driving table
  * for a Cypher query.
  */
object CypherSQLRoundtripExample extends App {
  // 1) Create CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  // 2) Register a file based data source at the session
  //    It contains a purchase network graph called 'products'
  val graphDir = getClass.getResource("/fs-graphsource/csv").getFile
  session.registerSource(Namespace("myDataSource"), GraphSources.fs(rootPath = graphDir).csv)

  // 3) Load social network data via case class instances
  val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)

  // 4) Query for a view of the people in the social network
  val result = socialNetwork.cypher(
    """|MATCH (p:Person)
       |RETURN p.age AS age, p.name AS name
    """.stripMargin
  )

  // 5) Register the result as a table called people
  result.records.asCaps.df.toDF("age", "name").createOrReplaceTempView("people")

  // 6) Query the registered table using SQL
  val sqlResults = session.sql("SELECT age, name FROM people")

  // 7) Use the results from the SQL query as driving table for a Cypher query on a graph contained in the data source
  val result2 = session.catalog.graph("myDataSource.products").cypher(
    s"""
       |MATCH (c:Customer {name: name})-->(p:Product)
       |RETURN c.name, age, p.title
     """.stripMargin, drivingTable = Some(sqlResults))

  // 8) Print the results
  result2.show
}
// end::full-example[]
