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
 * the filters the caller supplies to {@code overwrite(...)} (which would leave existing rows
 * in place while new rows are appended, producing duplicate or stale query results), we fail
 * fast with a clear message. Users who need replacement semantics should drop the target
 * table first or use pinot-batch-ingestion-spark-3's segment push runners with REFRESH /
 * consistent-push enabled.
 */
class PinotWriteBuilder(logicalWriteInfo: LogicalWriteInfo)
  extends WriteBuilder with SupportsOverwrite {

  override def build(): Write = new PinotWrite(logicalWriteInfo)

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    throw new UnsupportedOperationException(
      "The Pinot Spark 3 connector does not support overwrite semantics: df.write always " +
        "appends new segments. Received " + filters.length + " overwrite filter(s). To " +
        "replace existing data, drop the Pinot table via the controller REST API first, or use " +
        "pinot-batch-ingestion-spark-3's SparkSegment*PushJobRunner with REFRESH / " +
        "consistent-push enabled.")
  }
}
