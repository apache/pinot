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

package org.apache.pinot.spi.utils.builder;

import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class TableNameBuilderTest {

  @Test
  public void testGetTableNameVariations() {

    assertEquals(TableNameBuilder.getTableNameVariations("tableAbc"),
        ImmutableSet.of("tableAbc", "tableAbc_REALTIME", "tableAbc_OFFLINE"));

    assertEquals(TableNameBuilder.getTableNameVariations("tableAbc_REALTIME"),
        ImmutableSet.of("tableAbc", "tableAbc_REALTIME", "tableAbc_OFFLINE"));

    assertEquals(TableNameBuilder.getTableNameVariations("tableAbc_OFFLINE"),
        ImmutableSet.of("tableAbc", "tableAbc_REALTIME", "tableAbc_OFFLINE"));
  }

  @Test
  public void testQuoteTableNameWithType() {
    assertEquals(TableNameBuilder.quoteTableNameWithType("events_OFFLINE"), "\"events_OFFLINE\"");
    assertEquals(TableNameBuilder.quoteTableNameWithType("events_REALTIME"), "\"events_REALTIME\"");
    assertEquals(TableNameBuilder.quoteTableNameWithType("analytics.events_OFFLINE"),
        "\"analytics\".\"events_OFFLINE\"");
    assertEquals(TableNameBuilder.quoteTableNameWithType("analytics.my-table$1_REALTIME"),
        "\"analytics\".\"my-table$1_REALTIME\"");
    assertEquals(TableNameBuilder.quoteTableNameWithType("\u6570\u636E\u5E93.\u4E8B\u4EF6_REALTIME"),
        "\"\u6570\u636E\u5E93\".\"\u4E8B\u4EF6_REALTIME\"");
    assertEquals(TableNameBuilder.quoteTableNameWithType("_OFFLINE"), "\"_OFFLINE\"");
    assertEquals(TableNameBuilder.quoteTableNameWithType("analytics._REALTIME"),
        "\"analytics\".\"_REALTIME\"");
  }

  @Test
  public void testQuoteTableNameWithTypeEscapesEmbeddedQuotes() {
    assertEquals(TableNameBuilder.quoteTableNameWithType("db\"name.events\";DROP_TABLE--_OFFLINE"),
        "\"db\"\"name\".\"events\"\";DROP_TABLE--_OFFLINE\"");
  }

  @Test
  public void testQuoteTableNameWithTypeRejectsInvalidNames() {
    String[] invalidTableNames = {
        null,
        "",
        "events",
        "events_offline",
        ".events_OFFLINE",
        "analytics.",
        "analytics.events_OFFLINE.extra",
        "analytics..events_REALTIME",
        "events name_OFFLINE",
        "events_OFFLINE;SELECT"
    };
    for (String invalidTableName : invalidTableNames) {
      assertThrows(IllegalArgumentException.class, () -> TableNameBuilder.quoteTableNameWithType(invalidTableName));
    }
  }
}
