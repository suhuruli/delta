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

import org.apache.spark.sql.delta.DeltaAnalysisException
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsRowLevelOperations, Table, TableCatalog}
import org.apache.spark.sql.functions.col

/**
 * Tests for [[DeltaSQLConf.DSV2_ROW_LEVEL_OPERATIONS_ENABLED]]: Delta tables advertise the DSv2
 * [[SupportsRowLevelOperations]] interface while MERGE, UPDATE and DELETE keep running through
 * Delta's own commands.
 */
class DeltaTableV2RowLevelOperationsSuite
  extends QueryTest
  with DeltaSQLCommandTest {

  private val targetTable = "row_level_ops_target"

  private val initialRows = Seq(Row(1, "a", 1L), Row(2, "b", 1L), Row(3, "c", 1L))

  private def withRowLevelOperations(enabled: Boolean)(f: => Unit): Unit = {
    withSQLConf(DeltaSQLConf.DSV2_ROW_LEVEL_OPERATIONS_ENABLED.key -> enabled.toString) {
      withTable(targetTable) {
        sql(s"CREATE TABLE $targetTable (id INT, name STRING, version BIGINT) USING delta")
        sql(s"INSERT INTO $targetTable VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', 1)")
        f
      }
    }
  }

  private def loadTable(): Table = {
    val catalog = spark.sessionState.catalogManager.v2SessionCatalog.asInstanceOf[TableCatalog]
    catalog.loadTable(Identifier.of(Array("default"), targetTable))
  }

  test("loaded table implements SupportsRowLevelOperations only when the conf is enabled") {
    withRowLevelOperations(enabled = false) {
      assert(loadTable().isInstanceOf[DeltaTableV2])
      assert(!loadTable().isInstanceOf[SupportsRowLevelOperations])
    }
    withRowLevelOperations(enabled = true) {
      val table = loadTable()
      assert(table.isInstanceOf[DeltaTableV2])
      assert(table.isInstanceOf[SupportsRowLevelOperations])
      // Copies keep the interface, e.g. when time travel or CDC options are applied.
      assert(table.asInstanceOf[DeltaTableV2].copy().isInstanceOf[SupportsRowLevelOperations])
    }
  }

  for (enabled <- Seq(false, true)) {
    test(s"SQL MERGE runs through Delta - rowLevelOperations=$enabled") {
      withRowLevelOperations(enabled) {
        sql(
          s"""MERGE INTO $targetTable t
             |USING (SELECT * FROM VALUES (1, 'a2', 2L), (3, 'x', 0L), (4, 'd', 1L)
             |       AS s(id, name, version)) s
             |ON t.id = s.id
             |WHEN MATCHED AND s.version > t.version THEN UPDATE SET *
             |WHEN MATCHED AND s.version = 0 THEN DELETE
             |WHEN NOT MATCHED THEN INSERT *""".stripMargin)
        checkAnswer(
          spark.table(targetTable),
          Seq(Row(1, "a2", 2L), Row(2, "b", 1L), Row(4, "d", 1L)))
        assertLastOperation("MERGE")
      }
    }

    test(s"SQL UPDATE runs through Delta - rowLevelOperations=$enabled") {
      withRowLevelOperations(enabled) {
        sql(s"UPDATE $targetTable SET name = 'z' WHERE id = 2")
        checkAnswer(
          spark.table(targetTable),
          Seq(Row(1, "a", 1L), Row(2, "z", 1L), Row(3, "c", 1L)))
        assertLastOperation("UPDATE")
      }
    }

    test(s"SQL UPDATE of all columns in table order runs through Delta - " +
        s"rowLevelOperations=$enabled") {
      // Spark rewrites an UPDATE that assigns every column in table order before Delta's
      // analysis rules run.
      withRowLevelOperations(enabled) {
        sql(s"UPDATE $targetTable t SET id = t.id + 10, name = 'z', version = 2L WHERE t.id = 2")
        sql(s"UPDATE $targetTable SET id = id, name = upper(name), version = version")
        checkAnswer(
          spark.table(targetTable),
          Seq(Row(1, "A", 1L), Row(12, "Z", 2L), Row(3, "C", 1L)))
        assertLastOperation("UPDATE")
      }
    }

    test(s"SQL UPDATE with a subquery condition - rowLevelOperations=$enabled") {
      // Delta does not support subqueries in UPDATE conditions. Spark does not rewrite this
      // UPDATE (its assignments are not aligned), so Delta's own check applies with the conf on.
      withRowLevelOperations(enabled) {
        val e = intercept[DeltaAnalysisException] {
          sql(s"UPDATE $targetTable SET name = 'z' WHERE id IN (SELECT max(id) FROM $targetTable)")
        }
        assert(e.getErrorClass === "DELTA_UNSUPPORTED_SUBQUERY")
        checkAnswer(spark.table(targetTable), initialRows)
      }
    }

    test(s"SQL UPDATE of all columns with a subquery condition - rowLevelOperations=$enabled") {
      withRowLevelOperations(enabled) {
        val update = s"UPDATE $targetTable SET id = id, name = 'z', version = version " +
          s"WHERE id IN (SELECT max(id) FROM $targetTable)"
        val e = intercept[DeltaAnalysisException](sql(update))
        if (enabled) {
          // Known limitation: Spark rewrites this aligned UPDATE into a union-shaped plan that
          // Delta does not recover, so it fails before Delta's own subquery check runs.
          checkError(
            e,
            "DELTA_OPERATION_NOT_ALLOWED",
            parameters = Map("operation" -> "DSv2 row-level UPDATE scan"))
        } else {
          assert(e.getErrorClass === "DELTA_UNSUPPORTED_SUBQUERY")
        }
        checkAnswer(spark.table(targetTable), initialRows)
      }
    }

    test(s"SQL DELETE with alias and subquery condition - rowLevelOperations=$enabled") {
      withRowLevelOperations(enabled) {
        sql(s"DELETE FROM $targetTable t WHERE t.id = 3")
        checkAnswer(spark.table(targetTable), Seq(Row(1, "a", 1L), Row(2, "b", 1L)))
        assertLastOperation("DELETE")
        // Delta rejects non-EXISTS subqueries in DELETE conditions. With the conf on, the error
        // comes from Delta's own command, which shows that the rewritten plan was recovered.
        val e = intercept[DeltaAnalysisException] {
          sql(s"DELETE FROM $targetTable t WHERE t.id IN (SELECT max(id) FROM $targetTable)")
        }
        assert(e.getErrorClass === "DELTA_UNSUPPORTED_SUBQUERY")
        checkAnswer(spark.table(targetTable), Seq(Row(1, "a", 1L), Row(2, "b", 1L)))
      }
    }

    test(s"SQL DELETE runs through Delta - rowLevelOperations=$enabled") {
      withRowLevelOperations(enabled) {
        sql(s"DELETE FROM $targetTable WHERE id = 2")
        checkAnswer(spark.table(targetTable), Seq(Row(1, "a", 1L), Row(3, "c", 1L)))
        assertLastOperation("DELETE")
        sql(s"DELETE FROM $targetTable")
        checkAnswer(spark.table(targetTable), Seq.empty)
      }
    }

    test(s"Dataset.mergeInto with fully qualified target columns - rowLevelOperations=$enabled") {
      // The shape of the MERGE that Spark Declarative Pipelines AUTO CDC flows issue: the target
      // is referenced by its fully qualified, quoted name in conditions and assignments.
      withRowLevelOperations(enabled) {
        val target = s"`spark_catalog`.`default`.`$targetTable`"
        val source = spark.createDataFrame(Seq((1, "a2", 2L), (2, "b", 0L), (5, "e", 1L)))
          .toDF("id", "name", "version")
          .as("microbatch")
        source
          .mergeInto(target, col("microbatch.id") === col(s"$target.`id`"))
          .whenMatched(col("microbatch.version") === 0L)
          .delete()
          .whenMatched(col("microbatch.version") >= col(s"$target.`version`"))
          .update(Map(
            s"$target.`name`" -> col("microbatch.name"),
            s"$target.`version`" -> col("microbatch.version")))
          .whenNotMatched(col("microbatch.version") > 0L)
          .insert(Map(
            s"$target.`id`" -> col("microbatch.id"),
            s"$target.`name`" -> col("microbatch.name"),
            s"$target.`version`" -> col("microbatch.version")))
          .merge()
        checkAnswer(
          spark.table(targetTable),
          Seq(Row(1, "a2", 2L), Row(3, "c", 1L), Row(5, "e", 1L)))
        assertLastOperation("MERGE")
      }
    }
  }

  private def assertLastOperation(operation: String): Unit = {
    val lastOperation = sql(s"DESCRIBE HISTORY $targetTable LIMIT 1")
      .select("operation").head().getString(0)
    assert(lastOperation === operation)
  }
}
