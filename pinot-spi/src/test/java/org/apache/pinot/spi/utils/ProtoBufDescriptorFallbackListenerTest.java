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
import org.apache.pinot.spi.utils.CommonConstants.ConfigChangeListenerConstants;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class ProtoBufDescriptorFallbackListenerTest {
  private static final String KEY = ConfigChangeListenerConstants.PROTOBUF_DESCRIPTOR_FALLBACK_ENABLED;

  private final ProtoBufDescriptorFallbackListener _listener = ProtoBufDescriptorFallbackListener.getInstance();

  @AfterMethod
  public void reset() {
    _listener.reset();
  }

  @Test
  public void testEnabledByDefault() {
    assertTrue(_listener.isEnabled());
  }

  @Test
  public void testDisableAndReEnable() {
    _listener.onChange(Set.of(KEY), Map.of(KEY, "false"));
    assertFalse(_listener.isEnabled());
    _listener.onChange(Set.of(KEY), Map.of(KEY, "true"));
    assertTrue(_listener.isEnabled());
  }

  @Test
  public void testRemovedOrBlankValueRestoresDefault() {
    _listener.onChange(Set.of(KEY), Map.of(KEY, "false"));
    assertFalse(_listener.isEnabled());
    // Key removed from the cluster config
    _listener.onChange(Set.of(KEY), Map.of());
    assertTrue(_listener.isEnabled());

    _listener.onChange(Set.of(KEY), Map.of(KEY, "false"));
    assertFalse(_listener.isEnabled());
    _listener.onChange(Set.of(KEY), Map.of(KEY, " "));
    assertTrue(_listener.isEnabled());
  }

  @Test
  public void testUnrelatedChangeIgnored() {
    _listener.onChange(Set.of(KEY), Map.of(KEY, "false"));
    assertFalse(_listener.isEnabled());
    _listener.onChange(Set.of("some.other.config"), Map.of("some.other.config", "value", KEY, "true"));
    assertFalse(_listener.isEnabled());
  }
}
