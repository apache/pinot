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
package org.apache.pinot.graph.spi;


/**
 * Configuration key constants for the Pinot graph query feature.
 *
 * <p>Thread-safe: this class holds only constants.</p>
 */
public final class PinotGraphConfiguration {

  /**
   * Feature flag key to enable or disable graph query support.
   * When set to {@code true}, the broker will accept openCypher queries.
   */
  public static final String GRAPH_ENABLED_KEY = "pinot.graph.enabled";

  /**
   * Configuration key prefix for graph schema definitions.
   */
  public static final String GRAPH_SCHEMA_PREFIX = "pinot.graph.schema";

  private PinotGraphConfiguration() {
    // Utility class; do not instantiate.
  }
}
