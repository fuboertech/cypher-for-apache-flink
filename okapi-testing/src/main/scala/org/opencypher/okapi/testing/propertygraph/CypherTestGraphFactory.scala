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
package org.opencypher.okapi.testing.propertygraph

import org.opencypher.okapi.api.graph.{CypherSession, Pattern, PropertyGraph}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CypherType._

trait CypherTestGraphFactory[C <: CypherSession] {

  def apply(propertyGraph: InMemoryTestGraph, additionalPattern: Seq[Pattern] = Seq.empty)(implicit caps: C): PropertyGraph

  def name: String

  override def toString: String = name

  def computeSchema(propertyGraph: InMemoryTestGraph): Schema = {
    def extractFromNode(n: InMemoryTestNode) =
      n.labels -> n.properties.value.map {
        case (name, prop) => name -> prop.cypherType
      }

    def extractFromRel(r: InMemoryTestRelationship) =
      r.relType -> r.properties.value.map {
        case (name, prop) => name -> prop.cypherType
      }

    val labelsAndProps = propertyGraph.nodes.map(extractFromNode)
    val typesAndProps = propertyGraph.relationships.map(extractFromRel)

    val schemaWithLabels = labelsAndProps.foldLeft(Schema.empty) {
      case (acc, (labels, props)) => acc.withNodePropertyKeys(labels, props)
    }

    typesAndProps.foldLeft(schemaWithLabels) {
      case (acc, (t, props)) => acc.withRelationshipPropertyKeys(t)(props.toSeq: _*)
    }
  }
}
