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
package org.opencypher.spark.api.io.neo4j.sync

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.opencypher.graphddl.GraphDdl
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherString}
import org.opencypher.okapi.impl.exception.SchemaException
import org.opencypher.okapi.neo4j.io.MetaLabelSupport._
import org.opencypher.okapi.neo4j.io.Neo4jHelpers.Neo4jDefaults._
import org.opencypher.okapi.neo4j.io.Neo4jHelpers._
import org.opencypher.okapi.neo4j.io.testing.Neo4jServerFixture
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.testing.Bag
import org.opencypher.spark.api.io.HiveFormat
import org.opencypher.spark.api.io.neo4j.Neo4jPropertyGraphDataSource
import org.opencypher.spark.api.io.sql.{SqlDataSourceConfig, SqlPropertyGraphDataSource}
import org.opencypher.spark.impl.acceptance.ScanGraphInit
import org.opencypher.spark.impl.table.SparkTable
import org.opencypher.spark.testing.CAPSTestSuite

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class Neo4JGraphMergeTest extends CAPSTestSuite with Neo4jServerFixture with ScanGraphInit {

  override def dataFixture: String = ""

  val nodeKeys = Map("Person" -> Set("id"))
  val relKeys = Map("R" -> Set("id"))

  val initialGraph: RelationalCypherGraph[SparkTable.DataFrameTable] = initGraph(
    """
      |CREATE (s:Person {id: 1, name: "bar"})
      |CREATE (e:Person:Employee {id: 2})
      |CREATE (s)-[r:R {id: 1}]->(e)
    """.stripMargin)

  override def afterEach(): Unit = {
    neo4jContext.clear()
    super.afterEach()
  }

  describe("merging into the entire graph") {
    it("can do basic Neo4j merge") {
      Neo4jGraphMerge.createIndexes(entireGraphName, neo4jConfig, nodeKeys)
      Neo4jGraphMerge.merge(entireGraphName, initialGraph, neo4jConfig, Some(nodeKeys), Some(relKeys))

      val readGraph = Neo4jPropertyGraphDataSource(neo4jConfig).graph(entireGraphName)

      readGraph.cypher("MATCH (n) RETURN n.id as id, n.name as name, labels(n) as labels").records.toMaps should equal(Bag(
        CypherMap("id" -> 1, "name" -> "bar", "labels" -> Seq("Person")),
        CypherMap("id" -> 2, "name" -> null, "labels" -> Seq("Employee", "Person"))
      ))

      readGraph.cypher("MATCH (n)-[r]->(m) RETURN n.id as nid, r.id as id, m.id as mid").records.toMaps should equal(Bag(
        CypherMap("nid" -> 1, "id" -> 1, "mid" -> 2)
      ))

      // Do not change a graph when the same graph is merged as a delta
      Neo4jGraphMerge.merge(entireGraphName, initialGraph, neo4jConfig, Some(nodeKeys), Some(relKeys))
      val graphAfterSameMerge = Neo4jPropertyGraphDataSource(neo4jConfig)
        .graph(entireGraphName)

      graphAfterSameMerge.cypher("MATCH (n) RETURN n.id as id, n.name as name, labels(n) as labels").records.toMaps should equal(Bag(
        CypherMap("id" -> 1, "name" -> "bar", "labels" -> Seq("Person")),
        CypherMap("id" -> 2, "name" -> null, "labels" -> Seq("Employee", "Person"))
      ))

      graphAfterSameMerge.cypher("MATCH (n)-[r]->(m) RETURN n.id as nid, r.id as id, m.id as mid").records.toMaps should equal(Bag(
        CypherMap("nid" -> 1, "id" -> 1, "mid" -> 2)
      ))

      // merge a delta
      val delta = initGraph(
        """
          |CREATE (s:Person {id: 1, name: "baz", bar: 1})
          |CREATE (e:Person {id: 2})
          |CREATE (s)-[r:R {id: 1, name: 1}]->(e)
          |CREATE (s)-[r:R {id: 2}]->(e)
        """.stripMargin)

      Neo4jGraphMerge.merge(entireGraphName, delta, neo4jConfig, Some(nodeKeys), Some(relKeys))
      val graphAfterDeltaSync = Neo4jPropertyGraphDataSource(neo4jConfig)
        .graph(entireGraphName)

      graphAfterDeltaSync.cypher("MATCH (n) RETURN n.id as id, n.name as name, n.bar as bar, labels(n) as labels").records.toMaps should equal(Bag(
        CypherMap("id" -> 1, "name" -> "baz", "bar" -> 1, "labels" -> Seq("Person")),
        CypherMap("id" -> 2, "name" -> null, "bar" -> null, "labels" -> Seq("Employee", "Person"))
      ))

      graphAfterDeltaSync.cypher("MATCH (n)-[r]->(m) RETURN n.id as nid, r.id as id, r.name as name, m.id as mid").records.toMaps should equal(Bag(
        CypherMap("nid" -> 1, "id" -> 1, "name" -> 1, "mid" -> 2),
        CypherMap("nid" -> 1, "id" -> 2, "name" -> null, "mid" -> 2)
      ))
    }

    it("merges when using the same entity key for all labels") {
      val nodeKeys = Map("Person" -> Set("id"), "Employee" -> Set("id"))
      val relKeys = Map("R" -> Set("id"))

      Neo4jGraphMerge.createIndexes(entireGraphName, neo4jConfig, nodeKeys)
      val graphName = GraphName("graph")

      val graph = initGraph(
        """
          |CREATE (s:Person {id: 1, name: "bar"})
          |CREATE (e:Person:Employee {id: 2 })
          |CREATE (f:Employee {id: 3})
          |CREATE (s)-[r:R {id: 1}]->(e)
        """.stripMargin)

      Neo4jGraphMerge.merge(entireGraphName, graph, neo4jConfig, Some(nodeKeys), Some(relKeys))

      val readGraph = Neo4jPropertyGraphDataSource(neo4jConfig).graph(graphName)

      readGraph.cypher("MATCH (n) RETURN n.id as id, n.name as name, labels(n) as labels").records.toMaps should equal(Bag(
        CypherMap("id" -> 1, "name" -> "bar", "labels" -> Seq("Person")),
        CypherMap("id" -> 2, "name" -> null, "labels" -> Seq("Employee", "Person")),
        CypherMap("id" -> 3, "name" -> null, "labels" -> Seq("Employee"))
      ))
    }

    it("merges when using multiple entity keys with different names") {
      val nodeKeys = Map("Person" -> Set("nId"), "Employee" -> Set("mId"))
      val relKeys = Map("R" -> Set("id"))

      Neo4jGraphMerge.createIndexes(entireGraphName, neo4jConfig, nodeKeys)
      val graphName = GraphName("graph")

      val graph = initGraph(
        """
          |CREATE (s:Person {nId: 1, name: "bar"})
          |CREATE (e:Person:Employee {nId: 2, mId: 3 })
          |CREATE (f:Employee {mId: 2})
          |CREATE (s)-[r:R {id: 1}]->(e)
        """.stripMargin)

      Neo4jGraphMerge.merge(entireGraphName, graph, neo4jConfig, Some(nodeKeys), Some(relKeys))

      val readGraph = Neo4jPropertyGraphDataSource(neo4jConfig).graph(graphName)

      readGraph.cypher("MATCH (n) RETURN n.nId as nId, n.mId as mId, n.name as name, labels(n) as labels").records.toMaps should equal(Bag(
        CypherMap("nId" -> 1, "mId" -> null, "name" -> "bar", "labels" -> Seq("Person")),
        CypherMap("nId" -> 2, "mId" -> 3, "name" -> null, "labels" -> Seq("Employee", "Person")),
        CypherMap("nId" -> null, "mId" -> 2, "name" -> null, "labels" -> Seq("Employee"))
      ))
    }

    it("merges with entity keys set in the schema") {
      val data = List(
        Row(1L, "baz")
      ).asJava

      val df = sparkSession.createDataFrame(data, StructType(Seq(StructField("id", LongType), StructField("name", StringType))))
      caps.sql("CREATE DATABASE IF NOT EXISTS db")
      df.write.saveAsTable("db.persons")

      val ddlString =
        """
          |CREATE GRAPH TYPE personType (
          |  Person (id INTEGER, name STRING) KEY pk (id),
          |
          |  (Person)
          |)
          |
          |CREATE GRAPH personGraph OF personType (
          |  (Person) FROM ds1.db.persons
          |)
        """.stripMargin
      val hiveDataSourceConfig = SqlDataSourceConfig.Hive
      val pgds = SqlPropertyGraphDataSource(GraphDdl(ddlString), Map("ds1" -> hiveDataSourceConfig))
      val graph = pgds.graph(GraphName("personGraph"))

      Neo4jGraphMerge.createIndexes(entireGraphName, neo4jConfig, graph.schema.nodeKeys)

      Neo4jGraphMerge.merge(entireGraphName, graph, neo4jConfig)

      val readGraph = Neo4jPropertyGraphDataSource(neo4jConfig).graph(entireGraphName)

      readGraph.cypher("MATCH (n:Person) RETURN n.id as id, n.name as name, labels(n) as labels").records.toMaps should equal(Bag(
        CypherMap("id" -> 1, "name" -> "baz", "labels" -> Seq("Person"))
      ))
    }

    it("checks if entity keys are present in the merge graph") {
      val nodeKeys = Map("Person" -> Set("name"), "Employee" -> Set("mId"))
      val relKeys = Map("R" -> Set("id"))
      Neo4jGraphMerge.createIndexes(entireGraphName, neo4jConfig, nodeKeys)

      val graph = initGraph(
        """
          |CREATE (s:Person {nId: 1, name: "bar"})
          |CREATE (e:Person:Employee {nId: 2, mId: 3 })
          |CREATE (f:Employee {mId: 2})
          |CREATE (s)-[r:R {id: 1}]->(e)
        """.stripMargin)

      Try(Neo4jGraphMerge.merge(entireGraphName, graph, neo4jConfig, Some(nodeKeys), Some(relKeys))) match {
        case Success(_) => fail
        case Failure(exception) =>
          exception.getMessage should include("Properties [name] have nullable types")
      }
    }

    it("creates indexes correctly") {
      val nodeKeys = Map("Person" -> Set("name", "bar"), "Employee" -> Set("baz"))
      val relKeys = Map("REL" -> Set("a"))

      Neo4jGraphMerge.createIndexes(entireGraphName, neo4jConfig, nodeKeys)

      neo4jConfig.cypherWithNewSession("CALL db.constraints YIELD description").toSet should equal(Set(
        Map("description" -> new CypherString("CONSTRAINT ON ( person:Person ) ASSERT (person.name, person.bar) IS NODE KEY")),
        Map("description" -> new CypherString("CONSTRAINT ON ( employee:Employee ) ASSERT employee.baz IS NODE KEY"))
      ))

      neo4jConfig.cypherWithNewSession("CALL db.indexes YIELD description").toSet should equal(Set(
        Map("description" -> new CypherString(s"INDEX ON :Person($metaPropertyKey)")),
        Map("description" -> new CypherString(s"INDEX ON :Person(name, bar)")),
        Map("description" -> new CypherString(s"INDEX ON :Employee($metaPropertyKey)")),
        Map("description" -> new CypherString(s"INDEX ON :Employee(baz)"))
      ))
    }
  }

  describe("merging into subgraphs") {
    it("merges subgraphs") {
      val subGraphName = GraphName("name")

      Neo4jGraphMerge.createIndexes(subGraphName, neo4jConfig, nodeKeys)
      Neo4jGraphMerge.merge(subGraphName, initialGraph, neo4jConfig, Some(nodeKeys), Some(relKeys))

      val readGraph = Neo4jPropertyGraphDataSource(neo4jConfig).graph(subGraphName)

      readGraph.cypher("MATCH (n) RETURN n.id as id, n.name as name, labels(n) as labels").records.toMaps should equal(Bag(
        CypherMap("id" -> 1, "name" -> "bar", "labels" -> Seq("Person")),
        CypherMap("id" -> 2, "name" -> null, "labels" -> Seq("Employee", "Person"))
      ))

      readGraph.cypher("MATCH (n)-[r]->(m) RETURN n.id as nid, r.id as id, m.id as mid").records.toMaps should equal(Bag(
        CypherMap("nid" -> 1, "id" -> 1, "mid" -> 2)
      ))

      // Do not change a graph when the same graph is synced as a delta
      Neo4jGraphMerge.merge(entireGraphName, initialGraph, neo4jConfig, Some(nodeKeys), Some(relKeys))
      val graphAfterSameSync = Neo4jPropertyGraphDataSource(neo4jConfig).graph(subGraphName)

      graphAfterSameSync.cypher("MATCH (n) RETURN n.id as id, n.name as name, labels(n) as labels").records.toMaps should equal(Bag(
        CypherMap("id" -> 1, "name" -> "bar", "labels" -> Seq("Person")),
        CypherMap("id" -> 2, "name" -> null, "labels" -> Seq("Employee", "Person"))
      ))

      graphAfterSameSync.cypher("MATCH (n)-[r]->(m) RETURN n.id as nid, r.id as id, m.id as mid").records.toMaps should equal(Bag(
        CypherMap("nid" -> 1, "id" -> 1, "mid" -> 2)
      ))

      // Sync a delta
      val delta = initGraph(
        """
          |CREATE (s:Person {id: 1, name: "baz", bar: 1})
          |CREATE (e:Person {id: 2})
          |CREATE (s)-[r:R {id: 1, name: 1}]->(e)
          |CREATE (s)-[r:R {id: 2}]->(e)
        """.stripMargin)
      Neo4jGraphMerge.merge(subGraphName, delta, neo4jConfig, Some(nodeKeys), Some(relKeys))
      val graphAfterDeltaSync = Neo4jPropertyGraphDataSource(neo4jConfig).graph(subGraphName)

      graphAfterDeltaSync.cypher("MATCH (n) RETURN n.id as id, n.name as name, n.bar as bar, labels(n) as labels").records.toMaps should equal(Bag(
        CypherMap("id" -> 1, "name" -> "baz", "bar" -> 1, "labels" -> Seq("Person")),
        CypherMap("id" -> 2, "name" -> null, "bar" -> null, "labels" -> Seq("Employee", "Person"))
      ))

      graphAfterDeltaSync.cypher("MATCH (n)-[r]->(m) RETURN n.id as nid, r.id as id, r.name as name, m.id as mid").records.toMaps should equal(Bag(
        CypherMap("nid" -> 1, "id" -> 1, "name" -> 1, "mid" -> 2),
        CypherMap("nid" -> 1, "id" -> 2, "name" -> null, "mid" -> 2)
      ))
    }

    it("creates indexes correctly") {
      val nodeKeys = Map("Person" -> Set("name", "bar"), "Employee" -> Set("baz"))
      val relKeys = Map("REL" -> Set("a"))

      val subGraphName = GraphName("myGraph")
      Neo4jGraphMerge.createIndexes(subGraphName, neo4jConfig, nodeKeys)

      neo4jConfig.cypherWithNewSession("CALL db.constraints YIELD description").toSet shouldBe empty

      neo4jConfig.cypherWithNewSession("CALL db.indexes YIELD description").toSet should equal(Set(
        Map("description" -> new CypherString(s"INDEX ON :${subGraphName.metaLabelForSubgraph}($metaPropertyKey)")),
        Map("description" -> new CypherString(s"INDEX ON :Person(name, bar)")),
        Map("description" -> new CypherString(s"INDEX ON :Employee(baz)"))
      ))
    }
  }

  describe("error handling") {

    it("should throw when a node key is missing") {
      a[SchemaException] should be thrownBy Neo4jGraphMerge.merge(entireGraphName, initialGraph, neo4jConfig)
    }

    it("should throw when a missing entity key is not only appearing with an implied label that has an entity key") {
      val nodeKeys = Map("Person" -> Set("id"))
      Neo4jGraphMerge.createIndexes(entireGraphName, neo4jConfig, nodeKeys)
      val graph = initGraph(
        """
          |CREATE (s:Person {id: 1, name: "bar"})
          |CREATE (e:Person:Employee {id: 2})
          |CREATE (f:Employee {id: 3})
          |CREATE (s)-[r:R {id: 1}]->(e)
        """.stripMargin)

      a[SchemaException] should be thrownBy Neo4jGraphMerge.merge(entireGraphName, graph, neo4jConfig, Some(nodeKeys))
    }

  }

}
