/**
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
package org.opencypher.spark.impl.temporal

import java.sql.{Date, Timestamp}
import java.time.temporal.{ChronoField, IsoFields, TemporalField}

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.unsafe.types.CalendarInterval
import org.opencypher.okapi.impl.exception.UnsupportedOperationException

import scala.reflect.runtime.universe._

object TemporalUdfs extends Logging {

  /**
    * Adds a duration to a date.
    * Duration components on a sub-day level are ignored
    */
  val dateAdd: UserDefinedFunction =
    udf[Date, Date, CalendarInterval]((date: Date, interval: CalendarInterval) => {
      if (date == null || interval == null) {
        null
      } else {
        val days = interval.microseconds / CalendarInterval.MICROS_PER_DAY

        if (interval.microseconds % CalendarInterval.MICROS_PER_DAY != 0) {
          logger.warn("Arithmetic with Date and Duration can lead to incorrect results when sub-day values are present.")
        }

        val reducedLocalDate = date
          .toLocalDate
          .plusMonths(interval.months)
          .plusDays(days)

        Date.valueOf(reducedLocalDate)
      }
    })

  /**
    * Subtracts a duration from a date.
    * Duration components on a sub-day level are ignored
    */
  val dateSubtract: UserDefinedFunction =
    udf[Date, Date, CalendarInterval]((date: Date, interval: CalendarInterval) => {
      if (date == null || interval == null) {
        null
      } else {
        val days = interval.microseconds / CalendarInterval.MICROS_PER_DAY

        if (interval.microseconds % CalendarInterval.MICROS_PER_DAY != 0) {
          logger.warn("Arithmetic with Date and Duration can lead to incorrect results when sub-day values are present.")
        }

        val reducedLocalDate = date
          .toLocalDate
          .minusMonths(interval.months)
          .minusDays(days)

        Date.valueOf(reducedLocalDate)
      }
    })

  /**
    * Returns the week based year of a given temporal type.
    */
  def weekYear[I: TypeTag]: UserDefinedFunction = dateAccessor[I](IsoFields.WEEK_BASED_YEAR)

  /**
    * Returns the day of the quarter of a given temporal type.
    */
  def dayOfQuarter[I: TypeTag]: UserDefinedFunction = dateAccessor[I](IsoFields.DAY_OF_QUARTER)

  /**
    * Returns the day of the week of a given temporal type.
    */
  def dayOfWeek[I: TypeTag]: UserDefinedFunction = dateAccessor[I](ChronoField.DAY_OF_WEEK)

  /**
    * Returns the milliseconds.
    */
  def milliseconds[I: TypeTag]: UserDefinedFunction = timeAccessor[I](ChronoField.MILLI_OF_SECOND)

  /**
    * Returns the microseconds.
    */
  def microseconds[I: TypeTag]: UserDefinedFunction = timeAccessor[I](ChronoField.MICRO_OF_SECOND)

  def durationAccessor(accessor: String): UserDefinedFunction = udf[java.lang.Long, CalendarInterval](
    (duration: CalendarInterval) => {
      if (duration == null) {
        null
      } else {
        val days = duration.microseconds / CalendarInterval.MICROS_PER_DAY
        // Note: in cypher days (and weeks) make up their own group, thus we have to exclude them for all values < day
        val daysInMicros =  days * CalendarInterval.MICROS_PER_DAY

        val l: Long = accessor match {
          case "years" => duration.months / 12
          case "quarters" => duration.months / 3
          case "months" => duration.months
          case "weeks" => duration.microseconds / CalendarInterval.MICROS_PER_DAY / 7
          case "days" => duration.microseconds / CalendarInterval.MICROS_PER_DAY
          case "hours" => (duration.microseconds - daysInMicros ) / CalendarInterval.MICROS_PER_HOUR
          case "minutes" => (duration.microseconds - daysInMicros ) / CalendarInterval.MICROS_PER_MINUTE
          case "seconds" => (duration.microseconds - daysInMicros ) / CalendarInterval.MICROS_PER_SECOND
          case "milliseconds" => (duration.microseconds - daysInMicros ) / CalendarInterval.MICROS_PER_MILLI
          case "microseconds" => duration.microseconds - daysInMicros

          case "quartersofyear" => (duration.months / 3) % 4
          case "monthsofquarter" => duration.months % 3
          case "monthsofyear" => duration.months % 12
          case "daysofweek" => (duration.microseconds / CalendarInterval.MICROS_PER_DAY) % 7
          case "minutesofhour" => ((duration.microseconds  - daysInMicros )/ CalendarInterval.MICROS_PER_MINUTE) % 60
          case "secondsofminute" => ((duration.microseconds - daysInMicros ) / CalendarInterval.MICROS_PER_SECOND) % 60
          case "millisecondsofsecond" => ((duration.microseconds - daysInMicros ) / CalendarInterval.MICROS_PER_MILLI) % 1000
          case "microsecondsofsecond" => (duration.microseconds - daysInMicros ) % 1000000

          case other => throw UnsupportedOperationException(s"Unknown Duration accessor: $other")
        }
        new java.lang.Long(l)
      }
    }
  )

  private def dateAccessor[I: TypeTag](accessor: TemporalField): UserDefinedFunction = udf[Long, I] {
    case d: Date => d.toLocalDate.get(accessor)
    case l: Timestamp => l.toLocalDateTime.get(accessor)
    case other => throw UnsupportedOperationException(s"Date Accessor '$accessor' is not supported for '$other'.")
  }

  private def timeAccessor[I: TypeTag](accessor: TemporalField): UserDefinedFunction = udf[Long, I] {
    case l: Timestamp => l.toLocalDateTime.get(accessor)
    case other => throw UnsupportedOperationException(s"Time Accessor '$accessor' is not supported for '$other'.")
  }
}
