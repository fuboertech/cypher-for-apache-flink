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
package org.opencypher.okapi.impl.temporal

import java.time.temporal.ChronoUnit.DAYS

object TemporalConstants {
  val NANOS_PER_SECOND: Long = 1000000000L
  val SECONDS_PER_DAY: Long = DAYS.getDuration.getSeconds
  val SECONDS_PER_HOUR = 3600
  val SECONDS_PER_MINUTE = 60

  val MONTHS_PER_YEAR = 12
  val DAYS_PER_WEEK = 7

  /** 30.4375 days = 30 days, 10 hours, 30 minutes */
  val AVG_DAYS_PER_MONTH: Double = 365.2425 / 12
  val AVG_SECONDS_PER_MONTH: Long = 2629746
}
