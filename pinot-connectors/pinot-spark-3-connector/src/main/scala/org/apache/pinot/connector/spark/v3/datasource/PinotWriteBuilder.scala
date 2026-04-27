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
package org.apache.pinot.connector.spark.v3.datasource

import org.apache.spark.sql.connector.write.{LogicalWriteInfo, SupportsOverwrite, Write, WriteBuilder}
import org.apache.spark.sql.sources.Filter

/**
 * Spark 3 write builder. Implements {@link SupportsOverwrite} (Filter-based).
 *
 * The Pinot write path only ever appends new segments: it cannot drop or replace segments
 * matching an arbitrary filter as part of the same write job. Rather than silently dropping
 * the overwrite intent (which would leave existing rows in place while new rows are appended,
 * producing duplicate or stale query results), we fail fast with a clear message on every
 * overwrite entry point:
 *
 *   1. {@link SupportsOverwrite#overwrite(Array)} — filter-based overwrite.
 *   2. {@link org.apache.spark.sql.connector.write.SupportsTruncate#truncate()} — in Spark 3.5.x
 *      {@code SupportsOverwrite extends SupportsOverwriteV2}, which in turn extends
 *      {@code SupportsTruncate}. The V2Writes analyzer rule dispatches
 *      {@code df.write.mode("overwrite")} (overwrite-by-TRUE) to {@code truncate()} rather
 *      than {@code overwrite([AlwaysTrue])}. In Spark 3.5.x the default {@code truncate()}
 *      happens to delegate to {@code overwrite([AlwaysTrue])} so the override above would
 *      already throw, but we override {@code truncate()} explicitly to (a) emit an error
 *      message tailored to the {@code df.write.mode("overwrite")} entry point and
 *      (b) defend against future Spark default-implementation changes.
 *
 * Users who need replacement semantics should drop the target table first or use
 * pinot-batch-ingestion-spark-3's segment push runners with REFRESH / consistent-push enabled.
 */
class PinotWriteBuilder(logicalWriteInfo: LogicalWriteInfo)
  extends WriteBuilder with SupportsOverwrite {

  // Backward-compatible auxiliary constructor: prior releases of this connector exposed
  // PinotWriteBuilder(filters: Array[Filter], logicalWriteInfo: LogicalWriteInfo). The
  // `filters` parameter was unused (build() ignored it). Removing it would be a binary
  // break for any external embedder that called this constructor directly. The connector
  // itself only ever instantiates the 1-arg form (see PinotTable#newWriteBuilder), so the
  // 2-arg form is purely for source/binary compatibility during the spark-3 deprecation
  // window — `filters` are still ignored at build time, and overwrite()/truncate() throw.
  @deprecated("filters parameter is ignored; use the 1-arg constructor", "1.6.0")
  def this(filters: Array[Filter], logicalWriteInfo: LogicalWriteInfo) = this(logicalWriteInfo)

  override def build(): Write = new PinotWrite(logicalWriteInfo)

  // The default SupportsOverwrite#canOverwrite returns true, which advertises support that
  // overwrite()/truncate() then immediately reject. Returning false instead lets Spark's
  // V2Writes analyzer surface its own rejection earlier, before reaching our throw — and
  // defends against future Spark analyzer changes that might trust this method to gate the
  // dispatch to overwrite()/truncate().
  override def canOverwrite(filters: Array[Filter]): Boolean = false

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    throw new UnsupportedOperationException(
      "The Pinot Spark 3 connector does not support overwrite semantics: df.write always " +
        "appends new segments. Received " + filters.length + " overwrite filter(s). To " +
        "replace existing data, drop the Pinot table via the controller REST API first, or use " +
        "pinot-batch-ingestion-spark-3's SparkSegment*PushJobRunner with REFRESH / " +
        "consistent-push enabled.")
  }

  override def truncate(): WriteBuilder = {
    throw new UnsupportedOperationException(
      "The Pinot Spark 3 connector does not support truncate / overwrite semantics: df.write " +
        "always appends new segments. This error is typically triggered by " +
        "df.write.mode(\"overwrite\").format(\"pinot\")... or an INSERT OVERWRITE. Use " +
        "df.write.mode(\"append\") instead, or drop the Pinot table via the controller REST " +
        "API first, or use pinot-batch-ingestion-spark-3's SparkSegment*PushJobRunner with " +
        "REFRESH / consistent-push enabled.")
  }
}
