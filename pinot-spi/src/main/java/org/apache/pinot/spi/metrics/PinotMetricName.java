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
package org.apache.pinot.spi.metrics;

/**
 * Metric Name in Pinot.
 */
public interface PinotMetricName {

  /**
   * Returns the actual metric name.
   */
  Object getMetricName();

  /**
   * Overrides the equals method. This is needed as {@link PinotMetricName} is used as the key of the key-value pair
   * inside the hashmap in MetricsRegistry. Without overriding equals() and hashCode() methods, all the existing k-v pairs
   * stored in hashmap cannot be retrieved by initializing a new key.
   */
  boolean equals(Object obj);

  /**
   * Overrides the hashCode method. This method's contract is the same as equals() method.
   */
  int hashCode();
}
