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
package org.opencypher.okapi.impl.util

import java.io.PrintStream

object PrintOptions {
  private val DEFAULT_MAX_COLUMN_WIDTH: Int = Int.MaxValue

  implicit lazy val out: PrintOptions =
    PrintOptions(stream = Console.out, maxColumnWidth = DEFAULT_MAX_COLUMN_WIDTH)

  lazy val err: PrintOptions =
    PrintOptions(stream = Console.err, maxColumnWidth = DEFAULT_MAX_COLUMN_WIDTH)

  def current(implicit options: PrintOptions): PrintOptions =
    options
}

final case class PrintOptions(stream: PrintStream, maxColumnWidth: Int = PrintOptions.DEFAULT_MAX_COLUMN_WIDTH) {
  def stream(newStream: PrintStream): PrintOptions = copy(stream = newStream)
  def maxColumnWidth(maxColumnWidth: Int): PrintOptions = copy(maxColumnWidth = maxColumnWidth)
}
