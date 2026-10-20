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
package org.apache.pinot.common.coordination;

import java.util.Locale;
import org.apache.pinot.spi.env.PinotConfiguration;


/// Bootstrap-only coordination selection. Absence preserves Helix; misspellings never silently choose a backend.
public enum CoordinationBackend {
  HELIX, RATIS;

  public static final String CONFIG_KEY = "pinot.coordination.backend";

  public static CoordinationBackend from(PinotConfiguration config) {
    String value = config.getProperty(CONFIG_KEY, HELIX.name());
    try {
      return valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException | NullPointerException e) {
      throw new IllegalArgumentException(CONFIG_KEY + " must be HELIX or RATIS", e);
    }
  }
}
