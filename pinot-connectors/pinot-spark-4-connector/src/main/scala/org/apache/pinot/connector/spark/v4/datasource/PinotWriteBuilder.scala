/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.connector.spark.v4.datasource

import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, SupportsOverwriteV2, Write, WriteBuilder}

/**
 * Spark 4 write builder. Implements {@link SupportsOverwriteV2} (Predicate-based) — the V1
 * {@code SupportsOverwrite} (Filter-based) is deprecated in Spark 4 and slated for removal in
 * a future release.
 *
 * The Pinot write path only ever appends new segments: it cannot drop or replace segments
 * matching an arbitrary predicate as part of the same write job. Rather than silently dropping
 * the overwrite intent (which would leave existing rows in place while new rows are appended,
 * producing duplicate or stale query results), we fail fast with a clear message on every
 * overwrite entry point:
 *
 *   1. {@link SupportsOverwriteV2#overwrite(Array)} — invoked by Spark for
 *      {@code df.writeTo(...).overwrite(Predicate)} and by the V2Writes analyzer rule when an
 *      overwrite carries a non-trivial predicate.
 *   2. {@link org.apache.spark.sql.connector.write.SupportsTruncate#truncate()} — invoked by
 *      the V2Writes analyzer rule for the common {@code df.write.mode("overwrite")} path,
 *      which lowers to an overwrite-by-TRUE and is dispatched to {@code truncate()} because
 *      {@code SupportsOverwriteV2} extends {@code SupportsTruncate}. In Spark 4.0.0 the default
 *      {@code truncate()} happens to delegate to {@code overwrite([AlwaysTrue])} so the
 *      override above would already throw, but we override {@code truncate()} explicitly to
 *      (a) emit an error message tailored to the {@code df.write.mode("overwrite")} entry
 *      point and (b) defend against future Spark default-implementation changes.
 *
 * Users who need replacement semantics should drop the target table first or use
 * pinot-batch-ingestion-spark-4's segment push runners with REFRESH / consistent-push enabled.
 */
class PinotWriteBuilder(logicalWriteInfo: LogicalWriteInfo)
  extends WriteBuilder with SupportsOverwriteV2 {

  override def build(): Write = new PinotWrite(logicalWriteInfo)

  override def overwrite(predicates: Array[Predicate]): WriteBuilder = {
    throw new UnsupportedOperationException(
      "The Pinot Spark 4 connector does not support overwrite semantics: df.write always " +
        "appends new segments. Received " + predicates.length + " overwrite predicate(s). To " +
        "replace existing data, drop the Pinot table via the controller REST API first, or use " +
        "pinot-batch-ingestion-spark-4's SparkSegment*PushJobRunner with REFRESH / " +
        "consistent-push enabled.")
  }

  override def truncate(): WriteBuilder = {
    throw new UnsupportedOperationException(
      "The Pinot Spark 4 connector does not support truncate / overwrite semantics: df.write " +
        "always appends new segments. This error is typically triggered by " +
        "df.write.mode(\"overwrite\").format(\"pinot\")... or an INSERT OVERWRITE. Use " +
        "df.write.mode(\"append\") instead, or drop the Pinot table via the controller REST " +
        "API first, or use pinot-batch-ingestion-spark-4's SparkSegment*PushJobRunner with " +
        "REFRESH / consistent-push enabled.")
  }
}
