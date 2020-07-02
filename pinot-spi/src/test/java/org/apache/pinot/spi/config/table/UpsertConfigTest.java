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
package org.apache.pinot.spi.config.table;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


public class UpsertConfigTest {

  @Test
  public void testUpsertConfig() {
    UpsertConfig upsertConfig =
        new UpsertConfig(Collections.singletonList("primaryKey"), "offset", "validFrom", "validUntil");
    assertEquals(upsertConfig.getPrimaryKeyColumns(), Collections.singletonList("primaryKey"));
    assertEquals(upsertConfig.getOffsetColumn(), "offset");
    assertEquals(upsertConfig.getValidFromColumn(), "validFrom");
    assertEquals(upsertConfig.getValidUntilColumn(), "validUntil");

    // Test illegal arguments
    try {
      new UpsertConfig(null, "offset", "validFrom", "validUntil");
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
    try {
      new UpsertConfig(ImmutableList.of("pk1", "pk2"), "offset", "validFrom", "validUntil");
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
    try {
      new UpsertConfig(ImmutableList.of("pk1"), null, "validFrom", "validUntil");
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
    try {
      new UpsertConfig(ImmutableList.of("pk1"), "offset", null, "validUntil");
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
    try {
      new UpsertConfig(ImmutableList.of("pk1"), "offset", "validFrom", null);
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }
}
