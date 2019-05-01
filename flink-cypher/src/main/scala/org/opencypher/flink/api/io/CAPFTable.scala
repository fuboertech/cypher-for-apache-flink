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
package org.opencypher.flink.api.io

import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.expressions
import org.apache.flink.table.expressions.UnresolvedFieldReference
import org.opencypher.flink.api.CAPFSession
import org.opencypher.flink.impl.TableOps._
import org.opencypher.flink.impl.table.FlinkCypherTable.FlinkTable
import org.opencypher.flink.impl.{CAPFRecords, RecordBehaviour}
import org.opencypher.okapi.api.io.conversion.{EntityMapping, NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.okapi.api.types.{CTInteger, CTString}
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.okapi.relational.api.io.EntityTable
import org.opencypher.okapi.relational.api.table.RelationalEntityTableFactory

case object CAPFEntityTableFactory extends RelationalEntityTableFactory[FlinkTable] {
  override def entityTable(
    nodeMapping: EntityMapping,
    table: FlinkTable
  ): EntityTable[FlinkTable] = {
    CAPFEntityTable.create(nodeMapping, table)
  }
}

case class CAPFEntityTable private[flink](
  override val mapping: EntityMapping,
  override val table: FlinkTable
) extends EntityTable[FlinkTable] with RecordBehaviour {

  override type Records = CAPFEntityTable

  private[flink] def records(implicit capf: CAPFSession): CAPFRecords = capf.records.fromEntityTable(entityTable = this)

  override def cache(): CAPFEntityTable = {
    this.cache()
    this
  }

  override protected def verify(): Unit = {
    mapping.idKeys.values.toSeq.flatten.foreach {
      case (_, column) => table.verifyColumnType(column, CTInteger, "id key")
    }
  }
}

object CAPFEntityTable {
  def create(mapping: EntityMapping, table: FlinkTable): CAPFEntityTable = {
    val sourceIdColumns = mapping.allSourceIdKeys
    val idCols = sourceIdColumns.map(UnresolvedFieldReference)
    val remainingCols = mapping.allSourcePropertyKeys.map(UnresolvedFieldReference)
    val colsToSelect = idCols ++ remainingCols

    CAPFEntityTable(mapping, table.table.select(colsToSelect: _*))
  }
}

object CAPFNodeTable {

  /**
    * Creates a node table from the given [[Table]]. By convention, there needs to be one column storing node
    * identifiers and named after [[GraphEntity.sourceIdKey]]. All remaining columns are interpreted as node property columns, the column name is used as property
    * key.
    *
    * @param impliedLabels  implied node labels
    * @param nodeDF         node data
    * @return a node table with inferred node mapping
    */
  def apply(impliedLabels: Set[String], nodeTable: Table): CAPFEntityTable = {
    val propertyColumnNames = nodeTable.columns.filter(_ != GraphEntity.sourceIdKey).toSet
    val propertyKeyMapping = propertyColumnNames.map(p => p.toProperty -> p)

    val mapping = NodeMappingBuilder
      .on(GraphEntity.sourceIdKey)
      .withImpliedLabels(impliedLabels.toSeq: _*)
      .withPropertyKeyMappings(propertyKeyMapping.toSeq: _*)
      .build

    CAPFEntityTable.create(mapping, nodeTable)
  }
}

object CAPFRelationshipTable {

  /**
    * Creates a relationship table from the given [[Table]]. By convention, there needs to be one column storing
    * relationship identifiers and named after [[GraphEntity.sourceIdKey]], one column storing source node identifiers
    * and named after [[Relationship.sourceStartNodeKey]] and one column storing target node identifiers and named after
    * [[Relationship.sourceEndNodeKey]]. All remaining columns are interpreted as relationship property columns, the
    * column name is used as property key.
    *
    * Column names prefixed with `property#` are decoded by [[org.opencypher.okapi.impl.util.StringEncodingUtilities]] to
    * recover the original property name.
    *
    * @param relationshipType relationship type
    * @param relationshipDF   relationship data
    * @return a relationship table with inferred relationship mapping
    */
  def apply(relationshipType: String, relationshipTable: Table): CAPFEntityTable = {
    val propertyColumnNames = relationshipTable.columns.filter(!Relationship.nonPropertyAttributes.contains(_)).toSet
    val propertyKeyMapping = propertyColumnNames.map(p => p.toProperty -> p)

    val mapping = RelationshipMappingBuilder
      .on(GraphEntity.sourceIdKey)
      .from(Relationship.sourceStartNodeKey)
      .to(Relationship.sourceEndNodeKey)
      .withRelType(relationshipType)
      .withPropertyKeyMappings(propertyKeyMapping.toSeq: _*)
      .build

    CAPFEntityTable.create(mapping, relationshipTable)
  }

}