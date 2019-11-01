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

package org.apache.pinot.thirdeye.anomaly.detection.trigger;

public interface DataAvailabilityEvent {
  enum Status {
    FINISHED,  // data load finished
    FAILED,  // data load failed
    NONE  // default value
  }

  /**
   * @return the status of event
   */
  Status getStatus();

  /**
   * @return the name of the dataset
   */
  String getDatasetName();

  /**
   * @return the name of data source
   */
  String getDataSource();

  /**
   * @return the namespace of data
   */
  String getNamespace();

  /**
   * @return the smallest timestamp for the data event in epoch time (milliseconds)
   */
  long getLowWatermark();

  /**
   * @return the largest timestamp for the data event in epoch time (milliseconds)
   */
  long getHighWatermark();

  /**
   * @return the timestamp of event generated in epoch time (milliseconds)
   */
  long getEventTime();
}
