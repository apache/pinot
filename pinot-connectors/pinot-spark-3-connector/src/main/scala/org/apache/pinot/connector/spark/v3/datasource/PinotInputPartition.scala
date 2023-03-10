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
import org.apache.pinot.connector.spark.common.partition.PinotSplit
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types.StructType

/**
 * PinotInputPartition: Implements Spark's InputPartition which convey partition related information
 * from Spark master to executors. This class is serialized and sent across the network so it should
 * be kept lean for good performance.
 *
 * @param schema        Schema for the scan/read operation. This can be a subset of the tables schema
 * @param partitionId   Integer which is used as requestId when sending query to Pinot servers
 * @param pinotSplit    An instance of PinotSplit which encapsulates segment and query information
 * @param dataSourceOptions PinotDataSourceReadOptions instance created for the read
 */
case class PinotInputPartition(
    schema: StructType,
    partitionId: Int,
    pinotSplit: PinotSplit,
    dataSourceOptions: PinotDataSourceReadOptions)
  extends InputPartition {
}
