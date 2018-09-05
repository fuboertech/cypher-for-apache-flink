/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.flink.impl.physical.operators

import org.opencypher.flink.impl.physical.{CAPFPhysicalResult, CAPFRuntimeContext}
import org.opencypher.flink.impl.{CAPFRecords, CAPFSession}
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.relational.impl.table.RecordHeader

private[flink] abstract class LeafPhysicalOperator extends CAPFPhysicalOperator {

  override def execute(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = executeLeaf()

  def executeLeaf()(implicit context: CAPFRuntimeContext): CAPFPhysicalResult
}

object Start {

  def apply(qgn: QualifiedGraphName, records: CAPFRecords)(implicit capf: CAPFSession): Start = {
    Start(qgn, Some(records))
  }
}

final case class Start(qgn: QualifiedGraphName, recordsOpt: Option[CAPFRecords])
  (implicit capf: CAPFSession) extends LeafPhysicalOperator {

  override val header = recordsOpt.map(_.header).getOrElse(RecordHeader.empty)

  override def executeLeaf()(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    val records = recordsOpt.getOrElse(CAPFRecords.unit())
    CAPFPhysicalResult(records, resolve(qgn), qgn)
  }

  override def toString = {
    val graphArg = qgn.toString
    val recordsArg = recordsOpt.map(_.toString)
    val allArgs = List(recordsArg, graphArg).mkString(",  ")
    s"Start($allArgs)"
  }

}