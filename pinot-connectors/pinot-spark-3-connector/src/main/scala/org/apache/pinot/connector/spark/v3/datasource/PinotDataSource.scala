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

import java.util

/**
 * PinotDataSource implements TableProvider interface of Spark DataSourceV2 API
 * to provide read access to Pinot tables.
 */
class PinotDataSource extends TableProvider with DataSourceRegister {
  override def shortName(): String = "pinot"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val readParameters = PinotDataSourceReadOptions.from(options)
    val tableName = readParameters.tableName
    val controller = readParameters.controller

    val pinotTableSchema =
      PinotClusterClient.getTableSchema(controller, tableName)
    TypeConverter.pinotSchemaToSparkSchema(pinotTableSchema)
  }

  override def getTable(schema: StructType,
                        partitioning: Array[Transform],
                        properties: util.Map[String, String]): Table = {
    val tableName = properties.get(PinotDataSourceReadOptions.CONFIG_TABLE_NAME)
    new PinotTable(tableName, schema)
  }

  override def supportsExternalMetadata(): Boolean = true
}
