/*
 * Copyright (2026) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.catalog

import org.apache.spark.sql.delta.DeltaErrors

import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, If}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, LogicalPlan, Project, ReplaceData}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, RowLevelOperation, RowLevelOperationBuilder, RowLevelOperationInfo, RowLevelOperationTable, WriteBuilder}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * The [[RowLevelOperationBuilder]] of
 * [[DeltaTableV2.DeltaTableV2WithRowLevelOperations]].
 *
 * Delta executes MERGE, UPDATE and DELETE through its own commands, not through Spark's DSv2
 * row-level rewrite. Spark may still build a [[RowLevelOperation]] while analyzing such a command,
 * before Delta's analysis rules have replaced it; the resulting plan is then discarded by
 * [[org.apache.spark.sql.delta.DeltaAnalysis]]. The operation therefore never scans or writes,
 * and fails loudly if Spark ever tries to.
 */
private[delta] class DeltaCommandRowLevelOperationBuilder(info: RowLevelOperationInfo)
  extends RowLevelOperationBuilder {

  override def build(): RowLevelOperation = new DeltaCommandRowLevelOperation(info.command)
}

/** See [[DeltaCommandRowLevelOperationBuilder]]. */
private[delta] class DeltaCommandRowLevelOperation(cmd: RowLevelOperation.Command)
  extends RowLevelOperation {

  override def command(): RowLevelOperation.Command = cmd

  override def description(): String = s"DeltaCommand($cmd)"

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    throw DeltaErrors.operationNotSupportedException(s"DSv2 row-level $cmd scan")

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    throw DeltaErrors.operationNotSupportedException(s"DSv2 row-level $cmd write")
}

/**
 * Matches a [[ReplaceData]] plan that Spark's DSv2 row-level rewrite built for the given command
 * on a [[DeltaTableV2]] whose operation is a [[DeltaCommandRowLevelOperation]].
 */
private object DeltaCommandReplaceData {
  def unapply(plan: LogicalPlan): Option[(ReplaceData, RowLevelOperation.Command)] = plan match {
    case rd: ReplaceData if rd.resolved => rd.table match {
      case r: DataSourceV2Relation => r.table match {
        case RowLevelOperationTable(_: DeltaTableV2, op: DeltaCommandRowLevelOperation) =>
          Some((rd, op.command()))
        case _ => None
      }
      case _ => None
    }
    case _ => None
  }
}

/**
 * Matches the [[ReplaceData]] plan that Spark's `RewriteDeleteFromTable` produces for a DELETE on
 * a Delta table that advertises DSv2 row-level operations, and extracts the original target
 * relation and DELETE condition, so the DELETE can be executed with Delta's own command.
 */
private[delta] object DeltaCommandRowLevelDelete {
  def unapply(plan: LogicalPlan): Option[(NamedRelation, Expression)] = plan match {
    case DeltaCommandReplaceData(rd, RowLevelOperation.Command.DELETE) =>
      Some((rd.originalTable, rd.condition))
    case _ => None
  }
}

/**
 * Matches the [[ReplaceData]] plan that Spark's `RewriteUpdateTable` produces for an UPDATE on a
 * Delta table that advertises DSv2 row-level operations, and extracts the original target
 * relation, assignments and UPDATE condition, so the UPDATE can be executed with Delta's own
 * command.
 *
 * Spark only rewrites an UPDATE on a Delta table if it assigns every column in table order, and
 * then projects each column `c` with value `v` as `If(condition, v, c)`, next to internal columns
 * such as the row operation marker. Assignments are recovered by matching each target column to
 * its `If` projection, regardless of position. Only this shape is matched; any other plan (e.g. an
 * UPDATE whose condition has a subquery) is not, and fails in [[DeltaCommandRowLevelOperation]]
 * instead.
 */
private[delta] object DeltaCommandRowLevelUpdate {
  def unapply(plan: LogicalPlan): Option[(NamedRelation, Seq[Assignment], Expression)] = {
    plan match {
      case DeltaCommandReplaceData(rd, RowLevelOperation.Command.UPDATE) => rd.query match {
        case Project(projectList, _: DataSourceV2Relation) =>
          val assignments = rd.originalTable.output.flatMap { column =>
            projectList.collectFirst {
              case Alias(If(cond, value, updated: Attribute), _)
                  if updated.semanticEquals(column) && cond.semanticEquals(rd.condition) =>
                Assignment(column, value)
            }
          }
          if (assignments.size == rd.originalTable.output.size) {
            Some((rd.originalTable, assignments, rd.condition))
          } else {
            None
          }
        case _ => None
      }
      case _ => None
    }
  }
}
