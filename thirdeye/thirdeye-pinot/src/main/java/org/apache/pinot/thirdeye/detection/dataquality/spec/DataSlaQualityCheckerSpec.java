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

package org.apache.pinot.thirdeye.detection.dataquality.spec;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.detection.spec.AbstractSpec;


@JsonIgnoreProperties(ignoreUnknown = true)
public class DataSlaQualityCheckerSpec extends AbstractSpec {
  public static final String DEFAULT_DATA_SLA = "3_DAYS";
  private String sla;

  public String getSla() {
    return StringUtils.isNotEmpty(this.sla) ? this.sla : DEFAULT_DATA_SLA;
  }

  public void setSla(String sla) {
    this.sla = sla;
  }
}
