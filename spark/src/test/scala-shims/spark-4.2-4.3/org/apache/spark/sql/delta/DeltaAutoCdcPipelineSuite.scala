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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{AnalysisException, Column, Row}
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.pipelines.autocdc.{ChangeArgs, ColumnSelection, ScdType, UnqualifiedColumnName}
import org.apache.spark.sql.pipelines.common.RunState
import org.apache.spark.sql.pipelines.graph.{AutoCdcFlow, QueryContext, QueryOrigin}
import org.apache.spark.sql.pipelines.logging.RunProgress
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}

/**
 * End-to-end tests for Spark Declarative Pipelines AUTO CDC (SCD type 1) flows that target Delta
 * tables, see [[DeltaSQLConf.DSV2_ROW_LEVEL_OPERATIONS_ENABLED]].
 */
class DeltaAutoCdcPipelineSuite extends ExecutionTest with DeltaSQLCommandTest {

  private val catalog = TestGraphRegistrationContext.DEFAULT_CATALOG
  private val database = TestGraphRegistrationContext.DEFAULT_DATABASE
  private val target = "target"
  private val qualifiedTarget = s"$catalog.$database.$target"
  private val cdcMetadataDdl =
    "__spark_autocdc_metadata STRUCT<deleteSequence: BIGINT, upsertSequence: BIGINT> NOT NULL"

  private def cdcMeta(deleteSeq: Option[Long], upsertSeq: Option[Long]): Row =
    Row(deleteSeq.orNull, upsertSeq.orNull)

  private def autoCdcPipeline(
      source: DataFrame,
      deleteCondition: Option[Column]): TestGraphRegistrationContext = {
    new TestGraphRegistrationContext(spark) {
      // The AUTO CDC auxiliary table inherits the format of the target.
      registerTable(
        target, format = Some("delta"), catalog = Some(catalog), database = Some(database))
      registerFlow(AutoCdcFlow(
        identifier = fullyQualifiedIdentifier("auto_cdc_flow", Some(catalog), Some(database)),
        destinationIdentifier = fullyQualifiedIdentifier(target, Some(catalog), Some(database)),
        func = dfFlowFunc(source),
        queryContext = QueryContext(Some(catalog), Some(database)),
        origin = QueryOrigin.empty,
        changeArgs = ChangeArgs(
          keys = Seq(UnqualifiedColumnName("id")),
          sequencing = col("version"),
          columnSelection = deleteCondition.map(_ =>
            ColumnSelection.ExcludeColumns(Seq(UnqualifiedColumnName("is_delete")))),
          deleteCondition = deleteCondition,
          storedAsScdType = ScdType.Type1)))
    }
  }

  private def withAutoCdcConfs(rowLevelOperationsEnabled: Boolean)(f: => Unit): Unit = {
    withSQLConf(
        DeltaSQLConf.DSV2_ROW_LEVEL_OPERATIONS_ENABLED.key -> rowLevelOperationsEnabled.toString) {
      f
    }
  }

  /** Runs the pipeline and returns the errors of a failed run, or an empty Seq on success. */
  private def runPipeline(ctx: TestGraphRegistrationContext): Seq[Throwable] = {
    val updateCtx = TestPipelineUpdateContext(spark, ctx.toDataflowGraph, storageRoot)
    updateCtx.pipelineExecution.runPipeline()
    updateCtx.pipelineExecution.awaitCompletion()
    val events = updateCtx.eventBuffer.getEvents
    if (events.exists(_.details == RunProgress(RunState.FAILED))) {
      events.flatMap(_.error)
    } else {
      Seq.empty
    }
  }

  private def createTarget(): Unit = {
    sql(
      s"CREATE TABLE $qualifiedTarget " +
        s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $cdcMetadataDdl) USING delta")
  }

  test("AUTO CDC flow rejects a Delta target when DSv2 row-level operations are disabled") {
    withAutoCdcConfs(rowLevelOperationsEnabled = false) {
      val session = spark
      import session.implicits._
      createTarget()
      val stream = MemoryStream[(Int, String, Long)]
      stream.addData((1, "alice", 1L))

      val errors = runPipeline(
        autoCdcPipeline(stream.toDF().toDF("id", "name", "version"), deleteCondition = None))
      val conditions = errors.flatMap { e =>
        Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).collect {
          case a: AnalysisException => a.getCondition
        }
      }
      assert(conditions.contains("AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE"), errors)
    }
  }

  test("AUTO CDC flow applies upserts and deletes to a Delta target") {
    withAutoCdcConfs(rowLevelOperationsEnabled = true) {
      val session = spark
      import session.implicits._
      createTarget()
      val stream = MemoryStream[(Int, String, Long, Boolean)]
      stream.addData(
        (1, "alice", 1L, false),
        (2, "bob", 1L, false),
        (3, "carol", 1L, false),
        (1, "alice", 2L, true), // delete
        (1, "alice2", 3L, false), // re-insert after the delete
        (2, "bob2", 2L, false), // update
        (3, "carol", 2L, true)) // delete
      val source = stream.toDF().toDF("id", "name", "version", "is_delete")
      val ctx = autoCdcPipeline(source, deleteCondition = Some(col("is_delete") === true))

      assert(runPipeline(ctx).isEmpty)
      checkAnswer(
        spark.table(qualifiedTarget),
        Seq(
          Row(1, "alice2", 3L, cdcMeta(None, Some(3L))),
          Row(2, "bob2", 2L, cdcMeta(None, Some(2L)))))

      // Out-of-order events in a second run: the stale update of key 2 is ignored, and the
      // tombstone recorded for key 3 keeps the stale upsert of key 3 from resurrecting it.
      stream.addData(
        (2, "stale", 1L, false),
        (3, "stale", 1L, false),
        (4, "dave", 1L, false))
      assert(runPipeline(ctx).isEmpty)
      checkAnswer(
        spark.table(qualifiedTarget),
        Seq(
          Row(1, "alice2", 3L, cdcMeta(None, Some(3L))),
          Row(2, "bob2", 2L, cdcMeta(None, Some(2L))),
          Row(4, "dave", 1L, cdcMeta(None, Some(1L)))))

      // Both the target and the auxiliary state table are Delta tables written by Delta MERGE.
      val operations = sql(s"DESCRIBE HISTORY $qualifiedTarget")
        .select("operation").as[String].collect().toSet
      assert(operations.contains("MERGE"), operations)
      val auxTable = s"$catalog.$database.__spark_autocdc_aux_state_$target"
      assert(sql(s"DESCRIBE DETAIL $auxTable").select("format").as[String].head() === "delta")
    }
  }
}
