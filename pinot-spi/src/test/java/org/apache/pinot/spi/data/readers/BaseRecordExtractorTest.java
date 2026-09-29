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
package org.apache.pinot.spi.data.readers;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Set;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class BaseRecordExtractorTest {

  // === init / include-list resolution ===

  @Test
  public void testInitNullFieldsExtractsAll() {
    NoOpExtractor extractor = new NoOpExtractor();
    extractor.init(null, null);
    assertTrue(extractor._extractAll);
    assertEquals(extractor._fields, Set.of());
  }

  @Test
  public void testInitEmptyFieldsExtractsAll() {
    NoOpExtractor extractor = new NoOpExtractor();
    extractor.init(Set.of(), null);
    assertTrue(extractor._extractAll);
    assertEquals(extractor._fields, Set.of());
  }

  @Test
  public void testInitNonEmptyFieldsRestrictsToIncludeList() {
    NoOpExtractor extractor = new NoOpExtractor();
    extractor.init(Set.of("a", "b"), null);
    assertFalse(extractor._extractAll);
    assertEquals(extractor._fields, Set.of("a", "b"));
  }

  // === stringifyMapKey — shared helper for the Map<String, Object> contract ===

  @Test
  public void testStringifyMapKeyByteArrayBase64Encoded() {
    assertEquals(BaseRecordExtractor.stringifyMapKey(new byte[]{0, 1, 2, 3}), "AAECAw==");
  }

  @Test
  public void testStringifyMapKeyTimestampUsesIsoUtcWithNanos() {
    Timestamp ts = new Timestamp(1700000000123L);
    ts.setNanos(123_456_789);
    // ISO-8601 UTC, JVM-TZ-stable, full nanosecond precision preserved.
    assertEquals(BaseRecordExtractor.stringifyMapKey(ts), "2023-11-14T22:13:20.123456789Z");
  }

  @Test
  public void testStringifyMapKeyOtherTypesUseToString() {
    assertEquals(BaseRecordExtractor.stringifyMapKey("k"), "k");
    assertEquals(BaseRecordExtractor.stringifyMapKey(42), "42");
    assertEquals(BaseRecordExtractor.stringifyMapKey(123L), "123");
    assertEquals(BaseRecordExtractor.stringifyMapKey(true), "true");
    assertEquals(BaseRecordExtractor.stringifyMapKey(new BigDecimal("1.50")), "1.50");
    assertEquals(BaseRecordExtractor.stringifyMapKey(LocalDate.of(2024, 1, 1)), "2024-01-01");
    assertEquals(BaseRecordExtractor.stringifyMapKey(LocalTime.of(8, 51, 32)), "08:51:32");
  }

  // === Helper ===

  private static final class NoOpExtractor extends BaseRecordExtractor<Object> {
    @Override
    public GenericRow extract(Object from, GenericRow to) {
      throw new UnsupportedOperationException();
    }
  }
}
