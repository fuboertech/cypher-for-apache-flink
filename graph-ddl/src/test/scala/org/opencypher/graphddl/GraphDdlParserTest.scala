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
package org.opencypher.graphddl

import fastparse._
import fastparse.Parsed.{Failure, Success}
import org.opencypher.graphddl.GraphDdlParser._
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.util.ParserUtils._
import org.opencypher.okapi.testing.{BaseTestSuite, TestNameFixture}
import org.scalatest.mockito.MockitoSugar

class GraphDdlParserTest extends BaseTestSuite with MockitoSugar with TestNameFixture {

  override val separator = "parses"

  private def success[T](
    parser: P[_] => P[T],
    expectation: T,
    input: String = testName
  ): Unit = {

    val parsed = parse(input, parser)

    parsed match {
      case Failure(_, _, _) =>
        debug(parser, input)
      case _ =>
    }

    parsed should matchPattern {
      case Success(`expectation`, _) =>
    }
  }

  private def failure[T, Elem](parser: P[_] => P[T], input: String = testName): Unit = {
    parse(input, parser) should matchPattern {
      case Failure(_, _, _) =>
    }
  }

  def debug[T](parser: P[_] => P[T], input: String): T = {
    parse(input, parser) match {
      case Success(v, _) =>
        v
      case Failure(expected, index, extra) =>
        val before = index - math.max(index - 20, 0)
        val after = math.min(index + 20, extra.input.length) - index
        val locationPointer =
          s"""|\t${extra.input.slice(index - before, index + after).replace('\n', ' ')}
              |\t${"~" * before + "^" + "~" * after}
           """.stripMargin
        throw DdlParsingException(index, locationPointer, expected, extra.trace())
    }
  }

  val emptyMap = Map.empty[String, CypherType]
  val emptyList: List[Nothing] = List.empty[Nothing]

  describe("escapedIdentifiers") {
    it("parses `foo.json`") {
      success(escapedIdentifier(_), "foo.json")
    }
  }

  describe("set schema") {
    it("parses SET SCHEMA foo.bar") {
      success(setSchemaDefinition(_), SetSchemaDefinition("foo", "bar"))
    }

    it("parses SET SCHEMA foo.bar;") {
      success(setSchemaDefinition(_), SetSchemaDefinition("foo", "bar"))
    }
  }

  describe("element type definitions") {
    it("parses A") {
      success(elementTypeDefinition(_), ElementTypeDefinition("A"))
    }

    it("parses  A ( foo  string? )") {
      success(elementTypeDefinition(_), ElementTypeDefinition("A", properties = Map("foo" -> CTString.nullable)))
    }

    it("parses  A ( key FLOAT )") {
      success(elementTypeDefinition(_), ElementTypeDefinition("A", properties = Map("key" -> CTFloat)))
    }

    it("parses  A ( key FLOAT? )") {
      success(elementTypeDefinition(_), ElementTypeDefinition("A", properties = Map("key" -> CTFloat.nullable)))
    }

    it("!parses  A ( key _ STRING )") {
      failure(elementTypeDefinition(_))
    }

    it("parses  A ( key1 FLOAT, key2 STRING)") {
      success(elementTypeDefinition(_), ElementTypeDefinition("A", properties = Map("key1" -> CTFloat, "key2" -> CTString)))
    }

    it("parses A ( key DATE )") {
      success(elementTypeDefinition(_), ElementTypeDefinition("A", properties = Map("key" -> CTDate)))
    }

    it("parses A ( key DATE? )") {
      success(elementTypeDefinition(_), ElementTypeDefinition("A", properties = Map("key" -> CTDateOrNull)))
    }

    it("parses A ( key LOCALDATETIME )") {
      success(elementTypeDefinition(_), ElementTypeDefinition("A", properties = Map("key" -> CTLocalDateTime)))
    }

    it("parses A ( key LOCALDATETIME? )") {
      success(elementTypeDefinition(_), ElementTypeDefinition("A", properties = Map("key" -> CTLocalDateTimeOrNull)))
    }

    it("parses A ()") {
      success(elementTypeDefinition(_), ElementTypeDefinition("A"))
    }

    it("parses A EXTENDS B ()") {
      success(elementTypeDefinition(_), ElementTypeDefinition("A", parents = Set("B")))
    }

    it("parses A <: B ()") {
      success(elementTypeDefinition(_), ElementTypeDefinition("A", parents = Set("B")))
    }

    it("parses A EXTENDS B, C ()") {
      success(elementTypeDefinition(_), ElementTypeDefinition("A", parents = Set("B", "C")))
    }

    it("parses A <: B, C ()") {
      success(elementTypeDefinition(_), ElementTypeDefinition("A", parents = Set("B", "C")))
    }

    it("parses A EXTENDS B, C ( key STRING )") {
      success(elementTypeDefinition(_), ElementTypeDefinition("A", parents = Set("B", "C"), properties = Map("key" -> CTString)))
    }

    it("parses A <: B, C ( key STRING )") {
      success(elementTypeDefinition(_), ElementTypeDefinition("A", parents = Set("B", "C"), properties = Map("key" -> CTString)))
    }
  }

  describe("catalog element type definition") {
    it("parses CREATE ELEMENT TYPE A") {
      success(globalElementTypeDefinition(_), ElementTypeDefinition("A"))
    }

    it("parses CREATE ELEMENT TYPE A ( foo STRING ) ") {
      success(globalElementTypeDefinition(_), ElementTypeDefinition("A", properties = Map("foo" -> CTString)))
    }

    it("parses CREATE ELEMENT TYPE A KEY A_NK   (foo,   bar)") {
      success(globalElementTypeDefinition(_), ElementTypeDefinition("A", properties = Map.empty, maybeKey = Some("A_NK" -> Set("foo", "bar"))))
    }

    it("parses CREATE ELEMENT TYPE A ( foo STRING ) KEY A_NK (foo,   bar)") {
      success(globalElementTypeDefinition(_), ElementTypeDefinition("A", properties = Map("foo" -> CTString), maybeKey = Some("A_NK" -> Set("foo", "bar"))))
    }

    it("!parses CREATE ELEMENT TYPE A ( foo STRING ) KEY A ()") {
      failure(globalElementTypeDefinition(_))
    }
  }

  describe("node and relationship type definitions") {
    it("parses (A)") {
      success(nodeTypeDefinition(_), NodeTypeDefinition("A"))
    }

    it("parses (A,B)") {
      success(nodeTypeDefinition(_), NodeTypeDefinition("A", "B"))
    }

    it("parses (A)-[R]->(B)") {
      success(relTypeDefinition(_), RelationshipTypeDefinition("A", "R", "B"))
    }

    it("parses (A)-[R,S]->(B)") {
      success(relTypeDefinition(_), RelationshipTypeDefinition("A")("R", "S")("B"))
    }
  }

  describe("schema definitions") {

    it("parses multiple label definitions") {
      parseDdl(
        """|SET SCHEMA foo.bar
           |
           |CREATE ELEMENT TYPE A ( name STRING )

           |
           |CREATE ELEMENT TYPE B ( sequence INTEGER, nationality STRING?, age INTEGER? )
           |
           |CREATE ELEMENT TYPE TYPE_1
           |
           |CREATE ELEMENT TYPE TYPE_2 ( prop BOOLEAN? ) """.stripMargin) shouldEqual
        DdlDefinition(List(
          SetSchemaDefinition("foo", "bar"),
          ElementTypeDefinition("A", properties = Map("name" -> CTString)),
          ElementTypeDefinition("B", properties = Map("sequence" -> CTInteger, "nationality" -> CTString.nullable, "age" -> CTInteger.nullable)),
          ElementTypeDefinition("TYPE_1"),
          ElementTypeDefinition("TYPE_2", properties = Map("prop" -> CTBoolean.nullable))
        ))
    }

    it("parses a schema with node type, and rel type definitions") {

      val input =
        """|CREATE GRAPH TYPE mySchema (
           |
           |  --NODES
           |  (A),
           |  (B),
           |  (A, B),
           |
           |  --EDGES
           |  (A)-[TYPE_1]->(B),
           |  (A, B)-[TYPE_2]->(A)
           |)
        """.stripMargin
      success(graphTypeDefinition(_), GraphTypeDefinition(
        name = "mySchema",
        statements = List(
          NodeTypeDefinition("A"),
          NodeTypeDefinition("B"),
          NodeTypeDefinition("A", "B"),
          RelationshipTypeDefinition("A", "TYPE_1", "B"),
          RelationshipTypeDefinition("A", "B")("TYPE_2")("A")
        )), input)
    }

    it("parses CREATE GRAPH TYPE mySchema ( (A)-[TYPE]->(B) )") {
      success(graphTypeDefinition(_),
        GraphTypeDefinition(
          name = "mySchema",
          statements = List(
            RelationshipTypeDefinition("A", "TYPE", "B")
          )))
    }

    it("parses a schema with node and rel definitions in any order") {

      val input =
        """|CREATE GRAPH TYPE mySchema (
           |  (A, B)-[TYPE_1]->(B),
           |  (A),
           |  (A, B),
           |  (A)-[TYPE_1]->(A),
           |  (B),
           |  (B)-[TYPE_2]->(A, B)
           |)
        """.stripMargin
      success(graphTypeDefinition(_), GraphTypeDefinition(
        name = "mySchema",
        statements = List(
          RelationshipTypeDefinition("A", "B")("TYPE_1")("B"),
          NodeTypeDefinition("A"),
          NodeTypeDefinition("A", "B"),
          RelationshipTypeDefinition("A", "TYPE_1", "A"),
          NodeTypeDefinition("B"),
          RelationshipTypeDefinition("B")("TYPE_2")("A", "B")
        )), input)
    }
  }

  describe("graph definitions") {
    it("parses CREATE GRAPH myGraph OF foo ()") {
      success(graphDefinition(_), GraphDefinition("myGraph", Some("foo")))
    }

    it("parses CREATE GRAPH myGraph OF mySchema ()") {
      success(graphDefinition(_), GraphDefinition("myGraph", Some("mySchema")))
    }

    it("parses a graph definition with inlined graph type elements") {
      val expectedGraphStatements = List(
        ElementTypeDefinition("A", properties = Map("foo" -> CTString)),
        ElementTypeDefinition("B"),
        NodeTypeDefinition("A", "B"),
        RelationshipTypeDefinition("A", "B")("B")("C"),
        NodeMappingDefinition(NodeTypeDefinition("A", "B"), List(NodeToViewDefinition(List("view_a_b")))),
        RelationshipMappingDefinition(
          relType = RelationshipTypeDefinition("A", "B")("B")("C"),
          relTypeToView = List(RelationshipTypeToViewDefinition(
            viewDef = ViewDefinition(List("baz"), "alias_baz"),
            startNodeTypeToView = NodeTypeToViewDefinition(
              NodeTypeDefinition("A", "B"),
              ViewDefinition(List("foo"), "alias_foo"),
              JoinOnDefinition(List(
                (List("alias_foo", "COLUMN_A"), List("edge", "COLUMN_A")),
                (List("alias_foo", "COLUMN_C"), List("edge", "COLUMN_D"))))),
            endNodeTypeToView = NodeTypeToViewDefinition(
              NodeTypeDefinition("C"),
              ViewDefinition(List("bar"), "alias_bar"),
              JoinOnDefinition(List(
                (List("alias_bar", "COLUMN_A"), List("edge", "COLUMN_A")))))
          )))
      )
      parse(
        """|CREATE GRAPH myGraph (
           | A ( foo STRING ) ,
           | B,
           | (A, B),
           | (A, B)-[B]->(C),
           | (A,B) FROM view_a_b,
           | (A, B)-[B]->(C) FROM baz alias_baz
           |  START NODES (A, B) FROM foo alias_foo
           |      JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
           |          AND alias_foo.COLUMN_C = edge.COLUMN_D
           |  END NODES (C) FROM bar alias_bar
           |      JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
           |)
        """.stripMargin, graphDefinition(_)) should matchPattern {
        case Success(GraphDefinition("myGraph", None, `expectedGraphStatements`), _) =>
      }
    }

    it("fails if node / edge type definitions are present") {
      failure(graphDefinition(_),
        """|CREATE GRAPH myGraph (
           | A ( foo STRING ) ,
           | B,
           |
           | (A,B),
           | [B]
           |
           |)
        """.stripMargin)
    }
  }

  describe("Node mappings and relationship mappings") {

    it("parses (A) FROM view") {
      success(nodeMappingDefinition(_), NodeMappingDefinition(NodeTypeDefinition("A"), List(NodeToViewDefinition(List("view")))))
    }

    it("parses (A) FROM view (column1 AS propertyKey1, column2 AS propertyKey2)") {
      success(nodeMappingDefinition(_), NodeMappingDefinition(NodeTypeDefinition("A"), List(NodeToViewDefinition(List("view"), Some(Map("propertyKey1" -> "column1", "propertyKey2" -> "column2"))))))
    }
    it("parses (A) FROM viewA FROM viewB") {
      success(nodeMappingDefinition(_), NodeMappingDefinition(NodeTypeDefinition("A"), List(NodeToViewDefinition(List("viewA")), NodeToViewDefinition(List("viewB")))))
    }

    it("parses (A) FROM viewA, (B) FROM viewB") {
      success(nodeMappings(_), List(NodeMappingDefinition(NodeTypeDefinition("A"), List(NodeToViewDefinition(List("viewA")))), NodeMappingDefinition(NodeTypeDefinition("B"), List(NodeToViewDefinition(List("viewB"))))))
    }

    it("parses (A) FROM viewA (column1 AS propertyKey1, column2 AS propertyKey2) FROM viewB (column1 AS propertyKey1, column2 AS propertyKey2)") {
      success(nodeMappings(_), List(
        NodeMappingDefinition(NodeTypeDefinition("A"), List(
          NodeToViewDefinition(List("viewA"), Some(Map("propertyKey1" -> "column1", "propertyKey2" -> "column2"))),
          NodeToViewDefinition(List("viewB"), Some(Map("propertyKey1" -> "column1", "propertyKey2" -> "column2")))))
      ))
    }

    it("parses (A) FROM `foo.json`") {
      success(nodeMappingDefinition(_), NodeMappingDefinition(NodeTypeDefinition("A"), List(NodeToViewDefinition(List("foo.json")))))
    }

    it("parses (A) FROM viewA (column1 AS propertyKey1, column2 AS propertyKey2), (B) FROM viewB (column1 AS propertyKey1, column2 AS propertyKey2)") {
      success(nodeMappings(_), List(
        NodeMappingDefinition(NodeTypeDefinition("A"), List(NodeToViewDefinition(List("viewA"), Some(Map("propertyKey1" -> "column1", "propertyKey2" -> "column2"))))),
        NodeMappingDefinition(NodeTypeDefinition("B"), List(NodeToViewDefinition(List("viewB"), Some(Map("propertyKey1" -> "column1", "propertyKey2" -> "column2")))))
      ))
    }

    it("parses a relationship mapping definition") {
      val input =
        """|(X)-[Y]->(Z) FROM baz alias_baz
           |  START NODES (A, B) FROM foo alias_foo
           |      JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
           |          AND alias_foo.COLUMN_C = edge.COLUMN_D
           |  END NODES (C) FROM bar alias_bar
           |      JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
        """.stripMargin

      success(relationshipMappingDefinition(_), RelationshipMappingDefinition(
        relType = RelationshipTypeDefinition("X", "Y", "Z"),
        relTypeToView = List(RelationshipTypeToViewDefinition(
          viewDef = ViewDefinition(List("baz"), "alias_baz"),
          startNodeTypeToView = NodeTypeToViewDefinition(
            NodeTypeDefinition("A", "B"),
            ViewDefinition(List("foo"), "alias_foo"),
            JoinOnDefinition(List(
              (List("alias_foo", "COLUMN_A"), List("edge", "COLUMN_A")),
              (List("alias_foo", "COLUMN_C"), List("edge", "COLUMN_D"))))),
          endNodeTypeToView = NodeTypeToViewDefinition(
            NodeTypeDefinition("C"),
            ViewDefinition(List("bar"), "alias_bar"),
            JoinOnDefinition(List(
              (List("alias_bar", "COLUMN_A"), List("edge", "COLUMN_A")))))
        ))), input)
    }

    it("parses a relationship mapping definition with custom property to column mapping") {
      val input =
        """|(a)-[a]->(a) FROM baz alias_baz ( colA AS foo, colB AS bar )
           |  START NODES (A, B) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
           |  END NODES   (C)    FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
        """.stripMargin

      success(relationshipMappingDefinition(_), RelationshipMappingDefinition(
        relType = RelationshipTypeDefinition("a", "a", "a"),
        relTypeToView = List(RelationshipTypeToViewDefinition(
          viewDef = ViewDefinition(List("baz"), "alias_baz"),
          maybePropertyMapping = Some(Map("foo" -> "colA", "bar" -> "colB")),
          startNodeTypeToView = NodeTypeToViewDefinition(
            NodeTypeDefinition("A", "B"),
            ViewDefinition(List("foo"), "alias_foo"),
            JoinOnDefinition(List((List("alias_foo", "COLUMN_A"), List("edge", "COLUMN_A"))))),
          endNodeTypeToView = NodeTypeToViewDefinition(
            NodeTypeDefinition("C"),
            ViewDefinition(List("bar"), "alias_bar"),
            JoinOnDefinition(List((List("alias_bar", "COLUMN_A"), List("edge", "COLUMN_A")))))
        ))), input)
    }

    it("parses a relationship label set definition") {
      val input =
        """|(A)-[TYPE_1]->(B)
           |  FROM baz edge
           |    START NODES (A) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
           |    END NODES   (B) FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
           |  FROM baz edge
           |    START NODES (A) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
           |    END NODES   (B) FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
        """.stripMargin

      val relMappingDef = RelationshipTypeToViewDefinition(
        viewDef = ViewDefinition(List("baz"), "edge"),
        startNodeTypeToView = NodeTypeToViewDefinition(
          NodeTypeDefinition("A"),
          ViewDefinition(List("foo"), "alias_foo"),
          JoinOnDefinition(List((List("alias_foo", "COLUMN_A"), List("edge", "COLUMN_A"))))),
        endNodeTypeToView = NodeTypeToViewDefinition(
          NodeTypeDefinition("B"),
          ViewDefinition(List("bar"), "alias_bar"),
          JoinOnDefinition(List((List("alias_bar", "COLUMN_A"), List("edge", "COLUMN_A")))))
      )

      success(
        relationshipMappingDefinition(_),
        RelationshipMappingDefinition(
          RelationshipTypeDefinition("A", "TYPE_1", "B"),
          List(relMappingDef, relMappingDef)),
        input)
    }

    it("parses relationship label sets") {
      val input =
        """|(A)-[TYPE_1]->(B)
           |  FROM baz alias_baz
           |    START NODES (A) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
           |    END NODES   (B) FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
           |  FROM baz alias_baz
           |    START NODES (A) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
           |    END NODES   (B) FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A,
           |
           |(A)-[TYPE_2]->(B)
           |  FROM baz alias_baz
           |    START NODES (A) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
           |    END NODES   (B) FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
           |  FROM baz alias_baz
           |    START NODES (A) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
           |    END NODES   (B) FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
        """.stripMargin

      val relMappingDef = RelationshipTypeToViewDefinition(
        viewDef = ViewDefinition(List("baz"), "alias_baz"),
        startNodeTypeToView = NodeTypeToViewDefinition(
          NodeTypeDefinition("A"),
          ViewDefinition(List("foo"), "alias_foo"),
          JoinOnDefinition(List((List("alias_foo", "COLUMN_A"), List("edge", "COLUMN_A"))))),
        endNodeTypeToView = NodeTypeToViewDefinition(
          NodeTypeDefinition("B"),
          ViewDefinition(List("bar"), "alias_bar"),
          JoinOnDefinition(List((List("alias_bar", "COLUMN_A"), List("edge", "COLUMN_A")))))
      )

      success(relationshipMappings(_),
        List(
          RelationshipMappingDefinition(
            RelationshipTypeDefinition("A", "TYPE_1", "B"),
            List(relMappingDef, relMappingDef)),
          RelationshipMappingDefinition(
            RelationshipTypeDefinition("A", "TYPE_2", "B"),
            List(relMappingDef, relMappingDef))
        ), input)
    }
  }

  describe("parser error handling") {

    it("does not accept unknown types") {
      val ddlString =
        """|
           |CREATE ELEMENT TYPE (A {prop: char, prop2: int})
           |
           |CREATE GRAPH TYPE mySchema
           |
           |  (A);
           |
           |CREATE GRAPH myGraph WITH SCHEMA mySchema""".stripMargin

      an[DdlParsingException] shouldBe thrownBy {
        parseDdl(ddlString)
      }
    }

    it("gives a nice error message for old syntax") {
      val ddlString =
        """CREATE GRAPH TYPE mySchema (
          |
          |  (A),
          |  [FOO]
          |)
          |""".stripMargin
      val ex = the[DdlParsingException] thrownBy parseDdl(ddlString)
      ex.expected shouldEqual "(elementTypeDefinition | relTypeDefinition | nodeTypeDefinition)"
    }
  }

}
