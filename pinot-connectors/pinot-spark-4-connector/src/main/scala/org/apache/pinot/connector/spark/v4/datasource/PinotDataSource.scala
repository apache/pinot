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
  private val SPARK_3_DATASOURCE_FQN = "org.apache.pinot.connector.spark.v3.datasource.PinotDataSource"

  // Probe for the Spark 3 connector's PinotDataSource by class name. We use Class.forName
  // rather than a static reference so this module does not develop a compile-time dependency
  // on the Spark 3 connector. The probe runs once per JVM at first connector instantiation.
  @volatile private var spark3Probed = false
  @volatile private var spark3Conflict = false

  def guardAgainstSpark3ConnectorOnClasspath(): Unit = {
    if (!spark3Probed) {
      spark3Probed = true
      spark3Conflict = isSpark3ConnectorOnClasspath(getClass.getClassLoader)
    }
    if (spark3Conflict) {
      throw new IllegalStateException(spark3ConflictMessage)
    }
  }

  private[datasource] def isSpark3ConnectorOnClasspath(loader: ClassLoader): Boolean = {
    try {
      Class.forName(SPARK_3_DATASOURCE_FQN, /* initialize */ false, loader)
      true
    } catch {
      case _: ClassNotFoundException => false
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
