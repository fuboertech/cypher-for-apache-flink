package org.opencypher.spark.prototype.impl.convert

import org.neo4j.cypher.internal.frontend.v3_2.ast
import org.opencypher.spark.api.CypherType
import org.opencypher.spark.impl.types.{SchemaTyper, TyperContext}
import org.opencypher.spark.prototype.api.expr.Expr
import org.opencypher.spark.prototype.api.ir.global.GlobalsRegistry
import org.opencypher.spark.prototype.api.ir.pattern.Pattern
import org.opencypher.spark.prototype.api.schema.Schema

final case class IRBuilderContext(
  queryString: String,
  globals: GlobalsRegistry,
  schema: Schema,
  blocks: BlockRegistry[Expr] = BlockRegistry.empty[Expr],
  knownTypes: Map[ast.Expression, CypherType] = Map.empty)
{
  private lazy val typer = SchemaTyper(schema)
  private lazy val exprConverter = new ExpressionConverter(globals)
  private lazy val patternConverter = new PatternConverter(globals)

  // TODO: Fuse monads
  def infer(expr: ast.Expression): Map[ast.Expression, CypherType] = {
    typer.infer(expr, TyperContext(knownTypes)) match {
      case Right(result) =>
        result.context.typings

      case Left(errors) =>
        throw new IllegalArgumentException(s"Some error in type inference: ${errors.toList.mkString(", ")}")
    }
  }

  def convertPattern(p: ast.Pattern): Pattern[Expr] =
    patternConverter.convert(p)

  def convertExpression(e: ast.Expression): Expr =
    exprConverter.convert(e)(infer(e))

}
