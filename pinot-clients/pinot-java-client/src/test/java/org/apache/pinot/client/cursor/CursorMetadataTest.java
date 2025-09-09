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
package org.apache.pinot.client.cursor;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for CursorMetadata class.
 */
public class CursorMetadataTest {

  @Test
  public void testCursorMetadataCreation() {
    String cursorId = "test-cursor-123";
    int currentPage = 0;
    int pageSize = 100;
    long totalRows = 1000;
    int totalPages = 10;
    boolean hasNext = true;
    boolean hasPrevious = false;
    long expirationTime = System.currentTimeMillis() + 300000; // 5 minutes
    String brokerId = "localhost:8099";

    CursorMetadata metadata = new CursorMetadata(cursorId, currentPage, pageSize, totalRows, totalPages,
        hasNext, hasPrevious, expirationTime, brokerId);

    assertEquals(metadata.getCursorId(), cursorId);
    assertEquals(metadata.getCurrentPage(), currentPage);
    assertEquals(metadata.getPageSize(), pageSize);
    assertEquals(metadata.getTotalRows(), totalRows);
    assertEquals(metadata.getTotalPages(), totalPages);
    assertEquals(metadata.hasNext(), hasNext);
    assertEquals(metadata.hasPrevious(), hasPrevious);
    assertEquals(metadata.getExpirationTimeMs(), expirationTime);
    assertEquals(metadata.getBrokerId(), brokerId);
  }

  @Test
  public void testIsExpired() {
    long currentTime = System.currentTimeMillis();

    // Not expired - expiration time is in the future
    CursorMetadata notExpired = new CursorMetadata("test-id", 0, 100, 1000, 10,
        true, false, currentTime + 60000, "localhost:8099");
    assertFalse(notExpired.isExpired());

    // Expired - expiration time is in the past
    CursorMetadata expired = new CursorMetadata("test-id", 0, 100, 1000, 10,
        true, false, currentTime - 60000, "localhost:8099");
    assertTrue(expired.isExpired());

    // Edge case - expiration time is exactly now (should be expired)
    CursorMetadata edgeCase = new CursorMetadata("test-id", 0, 100, 1000, 10,
        true, false, currentTime - 1, "localhost:8099");
    assertTrue(edgeCase.isExpired());
  }

  @Test
  public void testNavigationFlags() {
    // First page - no previous, has next
    CursorMetadata firstPage = new CursorMetadata("test-id", 0, 100, 1000, 10,
        true, false, System.currentTimeMillis() + 60000, "localhost:8099");
    assertTrue(firstPage.hasNext());
    assertFalse(firstPage.hasPrevious());

    // Middle page - has both previous and next
    CursorMetadata middlePage = new CursorMetadata("test-id", 5, 100, 1000, 10,
        true, true, System.currentTimeMillis() + 60000, "localhost:8099");
    assertTrue(middlePage.hasNext());
    assertTrue(middlePage.hasPrevious());

    // Last page - has previous, no next
    CursorMetadata lastPage = new CursorMetadata("test-id", 9, 100, 1000, 10,
        false, true, System.currentTimeMillis() + 60000, "localhost:8099");
    assertFalse(lastPage.hasNext());
    assertTrue(lastPage.hasPrevious());
  }

  @Test
  public void testToString() {
    CursorMetadata metadata = new CursorMetadata("test-cursor-123", 2, 50, 500, 10,
        true, true, System.currentTimeMillis() + 60000, "localhost:8099");
    String toString = metadata.toString();

    assertTrue(toString.contains("test-cursor-123"));
    assertTrue(toString.contains("currentPage=2"));
    assertTrue(toString.contains("pageSize=50"));
    assertTrue(toString.contains("totalRows=500"));
    assertTrue(toString.contains("totalPages=10"));
    assertTrue(toString.contains("hasNext=true"));
    assertTrue(toString.contains("hasPrevious=true"));
    assertTrue(toString.contains("localhost:8099"));
  }

  @Test
  public void testWithZeroValues() {
    CursorMetadata metadata = new CursorMetadata("test-id", 0, 0, 0, 0,
        false, false, 0L, "host:0");

    assertEquals(metadata.getCursorId(), "test-id");
    assertEquals(metadata.getCurrentPage(), 0);
    assertEquals(metadata.getPageSize(), 0);
    assertEquals(metadata.getTotalRows(), 0);
    assertEquals(metadata.getTotalPages(), 0);
    assertFalse(metadata.hasNext());
    assertFalse(metadata.hasPrevious());
    assertEquals(metadata.getExpirationTimeMs(), 0L);
    assertEquals(metadata.getBrokerId(), "host:0");
  }

  @Test
  public void testWithLargeValues() {
    CursorMetadata metadata = new CursorMetadata("test-id", Integer.MAX_VALUE, Integer.MAX_VALUE,
        Long.MAX_VALUE, Integer.MAX_VALUE, true, true, Long.MAX_VALUE, "host:9999");

    assertEquals(metadata.getCurrentPage(), Integer.MAX_VALUE);
    assertEquals(metadata.getPageSize(), Integer.MAX_VALUE);
    assertEquals(metadata.getTotalRows(), Long.MAX_VALUE);
    assertEquals(metadata.getTotalPages(), Integer.MAX_VALUE);
    assertEquals(metadata.getExpirationTimeMs(), Long.MAX_VALUE);
  }
}
