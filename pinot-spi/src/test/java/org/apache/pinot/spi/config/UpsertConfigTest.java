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
package org.apache.pinot.spi.config;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class UpsertConfigTest {

  @Test
  public void testIngestionMode() {
    UpsertConfig upsertConfig;

    // test regular upsert table
    upsertConfig = new UpsertConfig(ImmutableList.of("primaryKey"), "offset", "validFrom",
        "validUntil");
    assertEquals(1, upsertConfig.getPrimaryKeyColumns().size());
    assertEquals("primaryKey", upsertConfig.getPrimaryKeyColumns().get(0));
    assertEquals("offset", upsertConfig.getOffsetColumn());
    assertEquals("validFrom", upsertConfig.getValidFromColumn());
    assertEquals("validUntil", upsertConfig.getValidUntilColumn());

    // test sanity check
    try {
      upsertConfig = new UpsertConfig(null,
          "offset", "validFrom", "validUntil");
      fail();
    } catch (RuntimeException ex) {}

    try {
      upsertConfig = new UpsertConfig(ImmutableList.of("pk1", "pk2"),
          "offset", "validFrom", "validUntil");
      fail();
    } catch (RuntimeException ex) {}

    try {
      upsertConfig = new UpsertConfig(ImmutableList.of("pk1"),
          null, "validFrom", "validUntil");
      fail();
    } catch (RuntimeException ex) {}

    try {
      upsertConfig = new UpsertConfig(ImmutableList.of("pk1"),
          "offset", null, "validUntil");
      fail();
    } catch (RuntimeException ex) {}

    try {
      upsertConfig = new UpsertConfig(ImmutableList.of("pk1"),
          "offset", "validFrom", null);
      fail();
    } catch (RuntimeException ex) {}
  }
}