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

import org.apache.pinot.connector.spark.common.PinotDataSourceReadOptions
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

/**
 * PinotTable implements Spark's Table interface to expose a logical representation of a
 * Pinot table. This is the interface where Spark discovers table capabilities such as
 * 'SupportsRead'. For now Pinot tables only support batch reads.
 *
 * @param name    Pinot table name
 * @param schema  Schema provided by Spark. This can be different than table schema
 */
class PinotTable(name: String, schema: StructType) extends Table with SupportsRead {
  override def name(): String = name

  override def schema(): StructType = schema

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(TableCapability.BATCH_READ)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val readParameters = PinotDataSourceReadOptions.from(options)
    new PinotScanBuilder(readParameters)
  }
}
