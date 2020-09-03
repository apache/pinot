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

package org.apache.pinot.thirdeye.anomaly;

/**
 * The severity of anomaly.
 */
public enum AnomalySeverity {
  // the order of definition follows the severity from highest to lowest
  CRITICAL ("critical"),
  HIGH ("high"),
  MEDIUM ("medium"),
  LOW ("low"),
  DEBUG ("debug");

  private String severity;

  AnomalySeverity(String severity) {
    this.severity = severity;
  }

  public String getLabel() {
    return severity;
  }
}
