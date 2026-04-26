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
 * write semantics (the V1 `SupportsOverwrite` vs V2 `SupportsOverwriteV2` overwrite/truncate
 * rejection contracts diverge subtly even though both reject overwrites). We fail fast at
 * connector construction with a clear message instructing the user to remove one jar.
 */
class PinotDataSource extends TableProvider with DataSourceRegister {
  PinotDataSource.guardAgainstSpark3ConnectorOnClasspath()

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
  private val SPARK_3_DATASOURCE_FQN = "org.apache.pinot.connector.spark.v3.datasource.PinotDataSource"
  // Escape hatch: when set to "true", the classpath-collision detection logs a WARN and
  // continues instead of throwing. Useful for advanced users who genuinely need both
  // connector jars (e.g., reading from a Spark 3 Pinot deployment and writing to a Spark 4
  // deployment in the same long-running Spark application). The default (unset / false)
  // preserves the safer fail-fast behavior.
  private[datasource] val SKIP_CONFLICT_GUARD_PROP = "pinot.spark.connector.skip-conflict-guard"

  // Probe for the Spark 3 connector's PinotDataSource by class name. We use Class.forName
  // rather than a static reference so this module does not develop a compile-time dependency
  // on the Spark 3 connector. The probe runs once per JVM via Scala `lazy val`, which the
  // compiler implements with a synchronized initialization barrier — under concurrent
  // construction every caller observes the fully-computed result rather than the default
  // (`false`) value of a half-initialized var. The earlier two-flag @volatile pattern had
  // a race window where a thread could read `spark3Probed=true` but `spark3Conflict=false`
  // (default) before the probing thread wrote the real value.
  //
  // We probe both `getClass.getClassLoader` (the loader that loaded this Spark 4 connector)
  // and `Thread.currentThread.getContextClassLoader` (what Spark's
  // `DataSource.lookupDataSource` uses to discover DataSourceRegister candidates). In a
  // typical Spark deployment with `--packages` and isolated executor classloaders, the v3
  // jar may be visible only via the context classloader — probing only our own would let
  // the conflict slip through.
  //
  // Limitation: the lazy val is computed exactly once at first construction, so a v3 jar
  // added later in the same JVM (e.g., via spark-shell `:require`, a custom plugin loader,
  // or any post-startup classpath mutation) will NOT be detected. Users who dynamically
  // load both connectors in the same session should set
  // `-Dpinot.spark.connector.skip-conflict-guard=true` and accept the silent-overwrite-
  // contract risk explicitly, or restart the JVM with both jars present so the probe runs
  // against the final classpath.
  private[datasource] lazy val spark3Conflict: Boolean = {
    val ownLoader = getClass.getClassLoader
    val ctxLoader = Thread.currentThread.getContextClassLoader
    isSpark3ConnectorOnClasspath(ownLoader) ||
      (ctxLoader != null && (ctxLoader ne ownLoader) && isSpark3ConnectorOnClasspath(ctxLoader))
  }

  def guardAgainstSpark3ConnectorOnClasspath(): Unit = {
    if (spark3Conflict) {
      if (java.lang.Boolean.getBoolean(SKIP_CONFLICT_GUARD_PROP)) {
        LOGGER.warn(
          "{} (escape hatch -D{}=true was set, continuing anyway)",
          spark3ConflictMessage, SKIP_CONFLICT_GUARD_PROP)
      } else {
        throw new IllegalStateException(spark3ConflictMessage)
      }
    }
  }

  private[datasource] def isSpark3ConnectorOnClasspath(loader: ClassLoader): Boolean = {
    try {
      Class.forName(SPARK_3_DATASOURCE_FQN, /* initialize */ false, loader)
      true
    } catch {
      case _: ClassNotFoundException => false
      // Class.forName can also throw LinkageError / NoClassDefFoundError if the v3 jar is on
      // the classpath but partially shadowed (e.g., a transitive dependency is missing). We
      // conservatively treat that as "not present" so the constructor does not leak an
      // unrelated bytecode-resolution error to the user; the worst case is the conflict guard
      // is bypassed and the user falls back to Spark's own multi-source error message.
      case _: LinkageError => false
    }
  }

  private[datasource] val spark3ConflictMessage: String =
    "pinot-spark-4-connector and pinot-spark-3-connector are both on the classpath; both " +
      "register the data-source short name 'pinot'. Spark would resolve the format " +
      "non-deterministically, which can silently route writes through the wrong overwrite " +
      "contract. Remove pinot-spark-3-connector-*.jar from the Spark application's --jars / " +
      "--packages / fat-jar, or remove pinot-spark-4-connector-*.jar if you intended to use " +
      "the Spark 3 connector."
}
