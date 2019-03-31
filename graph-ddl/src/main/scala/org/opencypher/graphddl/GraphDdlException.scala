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

import org.opencypher.okapi.api.types.CypherType

abstract class GraphDdlException(msg: String, cause: Option[Exception] = None) extends RuntimeException(msg, cause.orNull) with Serializable {
  import GraphDdlException._
  def getFullMessage: String = causeChain(this).map(_.getMessage).mkString("\n")
}

private[graphddl] object GraphDdlException {

  def unresolved(desc: String, reference: Any): Nothing = throw UnresolvedReferenceException(
    s"$desc: $reference"
  )

  def unresolved(desc: String, reference: Any, available: Traversable[Any]): Nothing = throw UnresolvedReferenceException(
    s"""$desc: $reference
       |Expected one of: ${available.mkString(", ")}""".stripMargin
  )

  def duplicate(desc: String, definition: Any): Nothing = throw DuplicateDefinitionException(
    s"$desc: $definition"
  )

  def illegalInheritance(desc: String, reference: Any): Nothing = throw IllegalInheritanceException(
    s"$desc: $reference"
  )

  def illegalConstraint(desc: String, reference: Any): Nothing = throw IllegalConstraintException(
    s"$desc: $reference"
  )

  def incompatibleTypes(msg: String): Nothing =
    throw TypeException(msg)

  def incompatibleTypes(key: String, t1: CypherType, t2: CypherType): Nothing = throw TypeException(
    s"""|Incompatible property types for property key: $key
        |Conflicting types: $t1 and $t2""".stripMargin
  )

  def malformed(desc: String, identifier: String): Nothing =
    throw MalformedIdentifier(s"$desc: $identifier")

  def tryWithContext[T](msg: String)(block: => T): T =
    try { block } catch { case e: Exception => throw ContextualizedException(msg, Some(e)) }

  def causeChain(e: Throwable): List[Throwable] = causeChain(e, Set.empty)
  def causeChain(e: Throwable, seen: Set[Throwable]): List[Throwable] = {
    val newSeen = seen + e
    e :: Option(e.getCause).filterNot(newSeen).toList.flatMap(causeChain(_, newSeen))
  }
}

case class UnresolvedReferenceException(msg: String, cause: Option[Exception] = None) extends GraphDdlException(msg, cause)

case class DuplicateDefinitionException(msg: String, cause: Option[Exception] = None) extends GraphDdlException(msg, cause)

case class IllegalInheritanceException(msg: String, cause: Option[Exception] = None) extends GraphDdlException(msg, cause)

case class IllegalConstraintException(msg: String, cause: Option[Exception] = None) extends GraphDdlException(msg, cause)

case class TypeException(msg: String, cause: Option[Exception] = None) extends GraphDdlException(msg, cause)

case class MalformedIdentifier(msg: String, cause: Option[Exception] = None) extends GraphDdlException(msg, cause)

case class ContextualizedException(msg: String, cause: Option[Exception] = None) extends GraphDdlException(msg, cause)