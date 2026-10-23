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
package org.apache.pinot.segment.local.segment.index.creator;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueSortedForwardIndexCreator;
import org.apache.pinot.segment.spi.V1Constants;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class SingleValueSortedForwardIndexCreatorTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), SingleValueSortedForwardIndexCreatorTest.class.getSimpleName());
  private static final String COLUMN = "orgId";

  @BeforeMethod
  public void setUp()
      throws Exception {
    FileUtils.forceMkdir(TEMP_DIR);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @Test
  public void testWritesDocIdRangePerDictId()
      throws Exception {
    try (SingleValueSortedForwardIndexCreator creator = new SingleValueSortedForwardIndexCreator(TEMP_DIR, COLUMN, 2)) {
      creator.putDictId(0);
      creator.putDictId(0);
      creator.putDictId(1);
    }
    assertEquals(readIndexFile(), new int[]{0, 1, 2, 2});
  }

  // Dictionary id 1 never gets a row, as when the index pass drops the only row for a value. Its fill values
  // (Integer.MAX_VALUE, Integer.MIN_VALUE) must never reach disk.
  @Test
  public void testCloseFailsWhenDictIdHasNoRows()
      throws Exception {
    SingleValueSortedForwardIndexCreator creator = new SingleValueSortedForwardIndexCreator(TEMP_DIR, COLUMN, 3);
    creator.putDictId(0);
    creator.putDictId(0);
    creator.putDictId(2);

    IOException e = expectThrows(IOException.class, creator::close);
    assertTrue(e.getMessage().contains("Dictionary id: 1 of sorted column: " + COLUMN + " has no rows"),
        e.getMessage());

    for (int value : readIndexFile()) {
      assertNotEquals(value, Integer.MAX_VALUE);
      assertNotEquals(value, Integer.MIN_VALUE);
    }
  }

  private static int[] readIndexFile()
      throws IOException {
    File indexFile = new File(TEMP_DIR, COLUMN + V1Constants.Indexes.SORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
    ByteBuffer buffer = ByteBuffer.wrap(FileUtils.readFileToByteArray(indexFile));
    int[] values = new int[buffer.remaining() / Integer.BYTES];
    for (int i = 0; i < values.length; i++) {
      values[i] = buffer.getInt();
    }
    return values;
  }
}
