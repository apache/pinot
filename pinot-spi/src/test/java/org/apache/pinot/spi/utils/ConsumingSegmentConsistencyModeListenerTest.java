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
package org.apache.pinot.spi.utils;

import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.utils.ConsumingSegmentConsistencyModeListener.Mode;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class ConsumingSegmentConsistencyModeListenerTest {
  private final ConsumingSegmentConsistencyModeListener _listener =
      ConsumingSegmentConsistencyModeListener.getInstance();

  @AfterMethod
  public void tearDown() {
    _listener.reset();
  }

  @Test
  public void testDefaultModeIsProtected() {
    assertEquals(Mode.DEFAULT_CONSUMING_SEGMENT_CONSISTENCY_MODE, Mode.PROTECTED);
    assertEquals(_listener.getConsistencyMode(), Mode.PROTECTED);
    assertTrue(_listener.isForceCommitAllowed());
  }

  @Test
  public void testExplicitClusterConfigOverridesDefault() {
    _listener.onChange(Set.of(_listener.getConfigKey()),
        Map.of(_listener.getConfigKey(), Mode.RESTRICTED.name()));
    assertEquals(_listener.getConsistencyMode(), Mode.RESTRICTED);
    assertFalse(_listener.isForceCommitAllowed());

    _listener.onChange(Set.of(_listener.getConfigKey()),
        Map.of(_listener.getConfigKey(), Mode.UNSAFE.name()));
    assertEquals(_listener.getConsistencyMode(), Mode.UNSAFE);
    assertTrue(_listener.isForceCommitAllowed());
  }

  @Test
  public void testUnchangedConfigKeyIsIgnored() {
    _listener.onChange(Set.of(_listener.getConfigKey()),
        Map.of(_listener.getConfigKey(), Mode.RESTRICTED.name()));
    assertEquals(_listener.getConsistencyMode(), Mode.RESTRICTED);

    // A change notification that doesn't include our key must not touch the mode, even if some
    // unrelated cluster config value is present.
    _listener.onChange(Set.of("some.other.config"), Map.of("some.other.config", "value"));
    assertEquals(_listener.getConsistencyMode(), Mode.RESTRICTED);
  }

  @Test
  public void testMissingOrBlankValueFallsBackToDefault() {
    assertEquals(Mode.fromString(null), Mode.DEFAULT_CONSUMING_SEGMENT_CONSISTENCY_MODE);
    assertEquals(Mode.fromString(""), Mode.DEFAULT_CONSUMING_SEGMENT_CONSISTENCY_MODE);
    assertEquals(Mode.fromString("   "), Mode.DEFAULT_CONSUMING_SEGMENT_CONSISTENCY_MODE);
  }

  @Test
  public void testInvalidValueFallsBackToDefault() {
    assertEquals(Mode.fromString("not-a-real-mode"), Mode.DEFAULT_CONSUMING_SEGMENT_CONSISTENCY_MODE);
  }

  @Test
  public void testResetRestoresDefault() {
    _listener.setMode(Mode.RESTRICTED);
    assertEquals(_listener.getConsistencyMode(), Mode.RESTRICTED);

    _listener.reset();
    assertEquals(_listener.getConsistencyMode(), Mode.PROTECTED);
  }
}
