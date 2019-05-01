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
package org.opencypher.flink.impl

import org.opencypher.flink.api.CAPFSession
import org.opencypher.flink.impl.table.FlinkCypherTable.FlinkTable
import org.opencypher.okapi.api.graph.{CypherResult, CypherSession, PropertyGraph}
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.api.planning.RelationalCypherResult

import scala.util.{Failure, Success, Try}

object CAPFConverters {

  private def unsupported(expected: String, got: Any): Nothing =
    throw UnsupportedOperationException(s"Can only handle $expected, got $got")

  implicit class RichPropertyGraph(val graph: PropertyGraph) extends AnyVal {
    def asCapf: RelationalCypherGraph[FlinkTable] = graph.asInstanceOf[RelationalCypherGraph[_]] match {
      case capf: RelationalCypherGraph[_] =>
        Try {
          capf.asInstanceOf[RelationalCypherGraph[FlinkTable]]
        } match {
          case Success(value) => value
          case Failure(_) => unsupported("CAPF graphs", capf)
        }
      case other => unsupported("CAPF graphs", other)
    }
  }

  implicit class RichSession(session: CypherSession) {
    def asCapf: CAPFSession = session match {
      case capf: CAPFSession  => capf
      case other              => unsupported("CAPF session", other)
    }
  }

  implicit class RichCypherRecords(val records: CypherRecords) extends AnyVal {
    def asCapf: CAPFRecords = records match {
      case capf: CAPFRecords => capf
      case other => unsupported("CAPF records", other)
    }
  }

  implicit class RichCypherResult(val records: CypherResult) extends AnyVal {
    def asCapf(implicit capf: CAPFSession): RelationalCypherResult[FlinkTable] = records match {
      case relational: RelationalCypherResult[_] =>
        Try {
          relational.asInstanceOf[RelationalCypherResult[FlinkTable]]
        } match {
          case Success(value) => value
          case Failure(_) => unsupported("CAPF results", relational)
        }
      case other => unsupported("CAPF results", other)
    }
  }

}
