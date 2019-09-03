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
package org.apache.pinot.common.config;

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * DSL for durations, which turns "5 days" into a duration.
 */
public class DurationDsl implements SingleKeyDsl<Duration> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DurationDsl.class);

  @Override
  public Duration parse(String text) {
    if (text == null || text.isEmpty()) {
      return null;
    }

    try {
      String[] parts = text.split(" ");
      final String unit = parts[1].toUpperCase();
      final String unitCount = parts[0];
      return new Duration(TimeUnit.valueOf(unit), Integer.parseInt(unitCount));
    } catch (Exception e) {
      LOGGER.warn("Caught exception while parsing duration {}, discarding the invalid duration.", e, text);
      return null;
    }
  }

  @Override
  public String unparse(Duration value) {
    if (value != null) {
      return value.getUnitCount() + " " + value.getUnit();
    } else {
      return null;
    }
  }
}
