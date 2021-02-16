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
package org.apache.pinot.compat.tests;


public class ClusterDescriptor {

  public static final String DEFAULT_HOST = "localhost";
  public static final String ZOOKEEPER_PORT = "2181";
  public static final String KAFKA_PORT = "19092";
  public static final String CONTROLLER_PORT = "9000";
  public static final String BROKER_PORT = "8099";

  public static final String ZOOKEEPER_URL = String.format("http://%s:%s", DEFAULT_HOST, ZOOKEEPER_PORT);
  public static final String KAFKA_URL = String.format("http://%s:%s", DEFAULT_HOST, KAFKA_PORT);
  public static final String CONTROLLER_URL = String.format("http://%s:%s", DEFAULT_HOST, CONTROLLER_PORT);
  public static final String BROKER_URL = String.format("http://%s:%s", DEFAULT_HOST, BROKER_PORT);
}
