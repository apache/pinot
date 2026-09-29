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
package org.apache.pinot.core.query.aggregation.function.funnel;

import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FunnelStepEventWithExtraFieldsTest {

  @Test
  public void testSerializeDeserializeRoundTrip() {
    long timestamp = 123456789L;
    int step = 2;
    FunnelStepEvent event = new FunnelStepEvent(timestamp, step);
    List<Object> extra = Arrays.asList("foo", "bar", 42, "baz");

    FunnelStepEventWithExtraFields original = new FunnelStepEventWithExtraFields(event, extra);
    byte[] bytes = original.getBytes();

    FunnelStepEventWithExtraFields restored = new FunnelStepEventWithExtraFields(bytes);

    Assert.assertEquals(restored.getFunnelStepEvent().getTimestamp(), timestamp);
    Assert.assertEquals(restored.getFunnelStepEvent().getStep(), step);
    Assert.assertEquals(restored.getExtraFields(), extra);
  }
}
