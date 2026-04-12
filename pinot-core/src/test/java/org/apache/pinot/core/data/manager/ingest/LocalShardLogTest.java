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
package org.apache.pinot.core.data.manager.ingest;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link LocalShardLog} append, read, prepare, commit, and abort operations.
 */
public class LocalShardLogTest {

  private File _tempDir;
  private LocalShardLog _shardLog;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _tempDir = new File(FileUtils.getTempDirectory(), "shard-log-test-" + System.currentTimeMillis());
    _shardLog = new LocalShardLog(_tempDir);
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testAppendAndRead()
      throws IOException {
    byte[] data1 = "hello".getBytes(StandardCharsets.UTF_8);
    byte[] data2 = "world".getBytes(StandardCharsets.UTF_8);

    long offset1 = _shardLog.append(data1);
    long offset2 = _shardLog.append(data2);

    assertEquals(offset1, 0);
    assertEquals(offset2, 4 + data1.length); // 4 bytes header + data length

    // Read from beginning
    Iterator<byte[]> iter = _shardLog.read(0);
    assertTrue(iter.hasNext());
    assertEquals(iter.next(), data1);
    assertTrue(iter.hasNext());
    assertEquals(iter.next(), data2);
    assertFalse(iter.hasNext());
  }

  @Test
  public void testReadFromOffset()
      throws IOException {
    byte[] data1 = "first".getBytes(StandardCharsets.UTF_8);
    byte[] data2 = "second".getBytes(StandardCharsets.UTF_8);

    _shardLog.append(data1);
    long offset2 = _shardLog.append(data2);

    // Read from second entry
    Iterator<byte[]> iter = _shardLog.read(offset2);
    assertTrue(iter.hasNext());
    assertEquals(iter.next(), data2);
    assertFalse(iter.hasNext());
  }

  @Test
  public void testPrepareAndCommit() {
    byte[] data = "test data".getBytes(StandardCharsets.UTF_8);
    long offset = _shardLog.append(data);

    _shardLog.prepare("stmt-1", offset, offset);
    LocalShardLog.StatementMeta meta = _shardLog.getStatementMeta("stmt-1");
    assertNotNull(meta);
    assertEquals(meta.getStatus(), LocalShardLog.StatementStatus.PREPARED);
    assertEquals(meta.getStartOffset(), offset);
    assertEquals(meta.getEndOffset(), offset);

    _shardLog.commit("stmt-1");
    meta = _shardLog.getStatementMeta("stmt-1");
    assertEquals(meta.getStatus(), LocalShardLog.StatementStatus.COMMITTED);
  }

  @Test
  public void testPrepareAndAbort() {
    byte[] data = "test data".getBytes(StandardCharsets.UTF_8);
    long offset = _shardLog.append(data);

    _shardLog.prepare("stmt-2", offset, offset);
    _shardLog.abort("stmt-2");

    LocalShardLog.StatementMeta meta = _shardLog.getStatementMeta("stmt-2");
    assertNotNull(meta);
    assertEquals(meta.getStatus(), LocalShardLog.StatementStatus.ABORTED);
  }

  @Test
  public void testRecoveryAfterRestart()
      throws IOException {
    byte[] data = "persistent data".getBytes(StandardCharsets.UTF_8);
    long offset = _shardLog.append(data);
    _shardLog.prepare("stmt-3", offset, offset);
    _shardLog.commit("stmt-3");

    // Simulate restart by creating a new instance with the same directory
    LocalShardLog recovered = new LocalShardLog(_tempDir);

    // Verify data is readable
    Iterator<byte[]> iter = recovered.read(0);
    assertTrue(iter.hasNext());
    assertEquals(iter.next(), data);

    // Verify metadata was recovered
    LocalShardLog.StatementMeta meta = recovered.getStatementMeta("stmt-3");
    assertNotNull(meta);
    assertEquals(meta.getStatus(), LocalShardLog.StatementStatus.COMMITTED);
    assertEquals(meta.getStartOffset(), offset);
  }

  @Test
  public void testRecoveryReadsUtf8Metadata()
      throws IOException {
    String statementId = "stmt-\u4f60\u597d";
    byte[] data = "persistent data".getBytes(StandardCharsets.UTF_8);
    long offset = _shardLog.append(data);
    _shardLog.prepare(statementId, offset, offset);

    LocalShardLog recovered = new LocalShardLog(_tempDir);

    LocalShardLog.StatementMeta meta = recovered.getStatementMeta(statementId);
    assertNotNull(meta);
    assertEquals(meta.getStatus(), LocalShardLog.StatementStatus.PREPARED);
    assertEquals(meta.getStartOffset(), offset);
  }

  @Test
  public void testMultipleAppends() {
    long prevOffset = -1;
    for (int i = 0; i < 100; i++) {
      byte[] data = ("entry-" + i).getBytes(StandardCharsets.UTF_8);
      long offset = _shardLog.append(data);
      assertTrue(offset > prevOffset || (i == 0 && offset == 0));
      prevOffset = offset;
    }

    // Read all back
    Iterator<byte[]> iter = _shardLog.read(0);
    int count = 0;
    while (iter.hasNext()) {
      byte[] data = iter.next();
      assertEquals(new String(data, StandardCharsets.UTF_8), "entry-" + count);
      count++;
    }
    assertEquals(count, 100);
  }

  @Test
  public void testGetWriteOffset() {
    assertEquals(_shardLog.getWriteOffset(), 0);

    byte[] data = "hello".getBytes(StandardCharsets.UTF_8);
    _shardLog.append(data);

    assertEquals(_shardLog.getWriteOffset(), 4 + data.length);
  }
}
