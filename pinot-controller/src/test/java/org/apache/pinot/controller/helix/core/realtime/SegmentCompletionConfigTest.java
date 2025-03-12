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

import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.helix.core.realtime.SegmentCompletionConfig.*;


public class SegmentCompletionConfigTest {

  private static final String DEFAULT_FSM_CLASS =
      "org.apache.pinot.controller.helix.core.realtime.BlockingSegmentCompletionFSM";
  private static final String PAUSELESS_FSM_CLASS =
      "org.apache.pinot.controller.helix.core.realtime.PauselessSegmentCompletionFSM";
  private static final String CUSTOM_FSM_CLASS =
      "org.apache.pinot.controller.helix.core.realtime.CustomSegmentCompletionFSM";

  @Test
  public void testGetFsmSchemes() {
    String customSchemeKey = FSM_SCHEME + "custom";
    PinotConfiguration pinotConfiguration = new PinotConfiguration(
        Map.of(DEFAULT_PAUSELESS_FSM_SCHEME_KEY, PAUSELESS_FSM_CLASS, DEFAULT_FSM_SCHEME_KEY, DEFAULT_FSM_CLASS,
            customSchemeKey, CUSTOM_FSM_CLASS));
    SegmentCompletionConfig segmentCompletionConfig = new SegmentCompletionConfig(pinotConfiguration);
    Map<String, String> expectedFsmSchemes =
        Map.of(DEFAULT_FSM_SCHEME, DEFAULT_FSM_CLASS, DEFAULT_PAUSELESS_FSM_SCHEME, PAUSELESS_FSM_CLASS, "custom",
            CUSTOM_FSM_CLASS);
    Assert.assertEquals(expectedFsmSchemes, segmentCompletionConfig.getFsmSchemes());
    Assert.assertEquals(segmentCompletionConfig.getDefaultFsmScheme(), DEFAULT_FSM_SCHEME);
    Assert.assertEquals(segmentCompletionConfig.getDefaultPauselessFsmScheme(), DEFAULT_PAUSELESS_FSM_SCHEME);
  }
}
