package org.opencypher.flink.test.support

import org.apache.flink.table.api.Table
import org.opencypher.flink.api.io.CAPFEntityTable
import org.opencypher.okapi.api.graph.{IdKey, Pattern, SourceEndNodeKey, SourceIdKey, SourceStartNodeKey}
import org.opencypher.okapi.api.io.conversion.EntityMapping
import org.opencypher.okapi.impl.util.StringEncodingUtilities._

trait EntityTableCreationSupport {

  def constructEntityTable(pattern: Pattern, table: Table): CAPFEntityTable = {
    val mapping = pattern.entities.foldLeft(EntityMapping.empty(pattern)) {
      case (acc, entity) =>

        val entityColumns = table.getSchema.getFieldNames.filter(_.startsWith(s"${entity.name}_"))

        val idMapping: Map[IdKey, String] = entityColumns.collect {
          case id if id.endsWith("_id") => SourceIdKey -> id
          case src if src.endsWith("_source") => SourceStartNodeKey -> src
          case tgt if tgt.endsWith("_target") => SourceEndNodeKey -> tgt
        }.toMap

        val propertyMapping: Map[String, String] = entityColumns.collect {
          case prop if prop.endsWith("_property") =>
            val encodedKey = prop.replaceFirst(s"${entity.name}_", "").replaceFirst("_property", "")
            encodedKey.decodeSpecialCharacters -> prop
        }.toMap

        acc.copy(
          properties = acc.properties.updated(entity, propertyMapping),
          idKeys = acc.idKeys.updated(entity, idMapping)
        )
    }

    CAPFEntityTable.create(mapping, table)
  }

}
