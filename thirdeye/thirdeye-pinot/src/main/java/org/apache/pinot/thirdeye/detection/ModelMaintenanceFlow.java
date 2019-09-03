/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 *
 */

package org.apache.pinot.thirdeye.detection;

import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.joda.time.Instant;


/**
 * The model maintenance flow. This flow re-tunes the detection model automatically if the the model's performance is bad.
 */
public interface ModelMaintenanceFlow {

  /**
   * Maintain the detection model
   * @param detectionConfig the detection config to maintain
   * @param timestamp the time stamp of this maintenance
   * @return the maintained detection config
   */
  DetectionConfigDTO maintain(DetectionConfigDTO detectionConfig, Instant timestamp);
}
