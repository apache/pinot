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

package org.apache.pinot.thirdeye.detection.spi.components;

import java.util.Map;
import org.apache.pinot.thirdeye.detection.spec.AbstractSpec;
import org.joda.time.Interval;

/**
 * The tunable. For tuning specs of each component. Will be initialize with user's input yaml for this
 * component.
 */
public interface Tunable<T extends AbstractSpec> extends BaseComponent<T> {
  /**
   * Returns the new spec for the component it's tuning for a given metric urn.
   * @param currentSpec current spec for the component. empty if not exist.
   * @param tuningWindow the tuning window
   * @param metricUrn the metric urn to tune. When detection runs,the tuned spec will be used for detection
   *                  to run on this metric urn.
   * @return the specs for the component it's tuning. Will be used to initialize the tuned component when detection runs.
   */
  Map<String, Object> tune(Map<String, Object> currentSpec, Interval tuningWindow, String metricUrn);
}
