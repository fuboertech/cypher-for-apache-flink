package org.opencypher.flink.physical

import org.opencypher.flink.physical.operators.CAPFPhysicalOperator
import org.opencypher.flink.{CAPFGraph, CAPFRecords}
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.relational.api.physical.RuntimeContext

import scala.collection.mutable

object CAPFRuntimeContext {
  val empty = CAPFRuntimeContext(CypherMap.empty,  _ => None, mutable.Map.empty)
}

case class CAPFRuntimeContext(
  parameters: CypherMap,
  resolve: QualifiedGraphName => Option[CAPFGraph],
  cache: mutable.Map[CAPFPhysicalOperator, CAPFPhysicalResult])
extends RuntimeContext[CAPFRecords , CAPFGraph]