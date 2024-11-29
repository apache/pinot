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
package org.apache.pinot.controller.helix.core.realtime;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;


public class SegmentCompletionConfig {
  public static final String FSM_SCHEME = "pinot.controller.segment.completion.fsm.scheme.";
  public static final String DEFAULT_FSM_SCHEME_KEY = "pinot.controller.segment.completion.fsm.scheme.default.";
  public static final String DEFAULT_FSM_SCHEME = "BlockingSegmentCompletionFSM";
  private final Map<String, String> _fsmSchemes = new HashMap<>();
  private final String _defaultFsmScheme;

  public SegmentCompletionConfig(PinotConfiguration configuration) {
    // Parse properties to extract FSM schemes
    // Assuming properties keys are in the format scheme=className
    for (String key : configuration.getKeys()) {
      if (key.startsWith(FSM_SCHEME)) {
        String scheme = key.substring(FSM_SCHEME.length());
        String className = configuration.getProperty(key);
        _fsmSchemes.put(scheme, className);
      }
    }

    // Get the default FSM scheme
    _defaultFsmScheme = configuration.getProperty(DEFAULT_FSM_SCHEME_KEY, DEFAULT_FSM_SCHEME);
  }

  public Map<String, String> getFsmSchemes() {
    return _fsmSchemes;
  }

  public String getDefaultFsmScheme() {
    return _defaultFsmScheme;
  }
}
