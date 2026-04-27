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

import org.apache.pinot.connector.spark.common.{PinotClusterClient, PinotDataSourceReadOptions}
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.{Logger, LoggerFactory}

import java.util

/**
 * PinotDataSource implements TableProvider interface of Spark DataSourceV2 API
 * to provide read access to Pinot tables.
 *
 * Both pinot-spark-3-connector and pinot-spark-4-connector register the same `"pinot"`
 * short name via `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`. If both
 * jars end up on the same classpath (e.g., a fat-jar that mistakenly bundles both, or a
 * `--packages` invocation that includes both), Spark's `DataSource.lookupDataSource` resolves
 * the format non-deterministically — and the wrong builder being picked silently flips
 * write semantics. We fail fast at connector construction with a clear message instructing
 * the user to remove one jar. The symmetric guard in pinot-spark-4-connector probes for v3;
 * this one probes for v4 — together they guarantee the conflict is caught regardless of
 * which connector Spark happens to instantiate first.
 */
class PinotDataSource extends TableProvider with DataSourceRegister {
  PinotDataSource.guardAgainstSpark4ConnectorOnClasspath()

  override def shortName(): String = "pinot"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val readParameters = PinotDataSourceReadOptions.from(options)
    val tableName = readParameters.tableName
    val controller = readParameters.controller

    val pinotTableSchema =
      PinotClusterClient.getTableSchema(controller, tableName, readParameters.useHttps, readParameters.authHeader, readParameters.authToken, readParameters.proxyEnabled)
    DataExtractor.pinotSchemaToSparkSchema(pinotTableSchema)
  }

  override def getTable(schema: StructType,
                        partitioning: Array[Transform],
                        properties: util.Map[String, String]): Table = {
    val tableName = properties.get(PinotDataSourceReadOptions.CONFIG_TABLE_NAME)
    new PinotTable(tableName, schema)
  }

  override def supportsExternalMetadata(): Boolean = true
}

private[datasource] object PinotDataSource {
  private val LOGGER: Logger = LoggerFactory.getLogger(classOf[PinotDataSource])
  private val SPARK_4_DATASOURCE_FQN = "org.apache.pinot.connector.spark.v4.datasource.PinotDataSource"
  // Escape hatch: when set to "true", the classpath-collision detection logs a WARN and
  // continues instead of throwing. Same property name as the v4-side guard so a single -D
  // flag disables both directions of the bidirectional check.
  private[datasource] val SKIP_CONFLICT_GUARD_PROP = "pinot.spark.connector.skip-conflict-guard"

  // Probe for the Spark 4 connector's PinotDataSource by class name on every constructor
  // call. Class.forName is JVM-cached after first lookup so the per-call cost is negligible —
  // and probing per call detects v4 jars added to the classpath after the first
  // PinotDataSource was constructed (e.g. via spark-shell `:require` or a custom plugin
  // loader). We probe both `getClass.getClassLoader` and the thread context classloader
  // to cover Spark deployments where the v4 jar is visible only via one or the other.
  private[datasource] def spark4Conflict: Boolean = {
    val ownLoader = getClass.getClassLoader
    val ctxLoader = Thread.currentThread.getContextClassLoader
    isSpark4ConnectorOnClasspath(ownLoader) ||
      (ctxLoader != null && (ctxLoader ne ownLoader) && isSpark4ConnectorOnClasspath(ctxLoader))
  }

  def guardAgainstSpark4ConnectorOnClasspath(): Unit = {
    if (spark4Conflict) {
      if (java.lang.Boolean.getBoolean(SKIP_CONFLICT_GUARD_PROP)) {
        LOGGER.warn(
          "{} (escape hatch -D{}=true was set, continuing anyway)",
          spark4ConflictMessage, SKIP_CONFLICT_GUARD_PROP)
      } else {
        throw new IllegalStateException(spark4ConflictMessage)
      }
    }
  }

  private[datasource] def isSpark4ConnectorOnClasspath(loader: ClassLoader): Boolean = {
    try {
      Class.forName(SPARK_4_DATASOURCE_FQN, /* initialize */ false, loader)
      true
    } catch {
      case _: ClassNotFoundException => false
      // Treat partial linkage as "present" (fail-closed) — the v4 class IS on the classpath,
      // it just cannot run. Falling back to "absent" would let the exact non-deterministic
      // resolution this guard exists to prevent slip through.
      case e: LinkageError =>
        LOGGER.warn(
          "Spark 4 PinotDataSource is on the classpath but failed to link ({}); " +
            "treating as a conflict to preserve the fail-fast guarantee. " +
            "Set -D{}=true to bypass.",
          e.getMessage, SKIP_CONFLICT_GUARD_PROP)
        true
    }
  }

  private[datasource] val spark4ConflictMessage: String =
    "pinot-spark-3-connector and pinot-spark-4-connector are both on the classpath; both " +
      "register the data-source short name 'pinot'. Spark would resolve the format " +
      "non-deterministically, which can silently route writes through the wrong overwrite " +
      "contract. Remove pinot-spark-4-connector-*.jar from the Spark application's --jars / " +
      "--packages / fat-jar, or remove pinot-spark-3-connector-*.jar if you intended to use " +
      "the Spark 4 connector."
}
