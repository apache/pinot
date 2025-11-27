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
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("read-pinot-airlineStats").master("local[*]").getOrCreate()

val df = spark.read.
  format("org.apache.pinot.connector.spark.v3.datasource.PinotDataSource").
  option("table", "myTable").
  option("tableType", "offline").
  option("controller", "pinot-proxy:8080").
  option("secureMode", "true").
  option("authToken", "st-xxxxxxx").
  option("proxy.enabled", "true").
  option("grpc.proxy-uri", "pinot-proxy:8094").
  option("useGrpcServer", "true").
  load()

println("Schema:")
df.printSchema()

println("Sample rows:")
df.show(10, truncate = false)

println(s"Total rows: ${df.count()}")

spark.stop()
