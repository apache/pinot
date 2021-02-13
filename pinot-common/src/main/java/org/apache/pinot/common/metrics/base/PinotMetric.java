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
package org.apache.pinot.common.metrics.base;


public interface PinotMetric {

  /**
   * Allow the given {@link PinotMetricProcessor} to process {@code this} as a metric.
   *
   * @param processor    a {@link PinotMetricProcessor}
   * @param name         the name of the current metric
   * @param context      a given context which should be passed on to {@code processor}
   * @param <T>          the type of the context object
   * @throws Exception if something goes wrong
   */
  <T> void processWith(PinotMetricProcessor<T> processor, PinotMetricName name, T context) throws Exception;
}
