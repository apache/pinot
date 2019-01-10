/*
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

package org.apache.pinot.thirdeye.detection.alert;

import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.detection.DataProvider;


/**
 * The Detection alert filter.
 */
public abstract class DetectionAlertFilter {
  /**
   * The Provider.
   */
  protected final DataProvider provider;
  /**
   * The Config.
   */
  protected final DetectionAlertConfigDTO config;
  /**
   * The End time.
   */
  protected final long endTime;

  /**
   * Instantiates a new Detection alert filter.
   *
   * @param provider the provider
   * @param config the config
   * @param endTime the end time
   */
  public DetectionAlertFilter(DataProvider provider, DetectionAlertConfigDTO config, long endTime) {
    this.provider = provider;
    this.config = config;
    this.endTime = endTime;
  }

  /**
   * Returns a detection alert filter result for the time range between {@code startTime} and {@code endTime}.
   *
   * @return alert filter result
   * @throws Exception the exception
   */
  public abstract DetectionAlertFilterResult run() throws Exception;
}
