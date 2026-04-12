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
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.ingest.PreparedStore;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link InsertRowApplier} persistence and replay tracking.
 */
public class InsertRowApplierTest {

  private File _tempDir;

  @BeforeMethod
  public void setUp() {
    _tempDir = new File(FileUtils.getTempDirectory(), "insert-row-applier-test-" + System.currentTimeMillis());
    _tempDir.mkdirs();
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testConfirmAppliedPersistsUtf8TrackingFile()
      throws IOException {
    InsertRowApplier applier = new InsertRowApplier(new NoOpPreparedStore(), _tempDir);
    String statementId = "stmt-\u4f60\u597d";

    applier.confirmApplied(statementId, 1, 0);

    byte[] trackingBytes =
        Files.readAllBytes(new File(new File(_tempDir, "insert"), "applied_statements.log").toPath());
    assertEquals(trackingBytes, (statementId + ":1:0\n").getBytes(StandardCharsets.UTF_8));

    InsertRowApplier recovered = new InsertRowApplier(new NoOpPreparedStore(), _tempDir);
    assertTrue(recovered.isApplied(statementId, 1, 0));
  }

  @Test
  public void testCleanAppliedEntriesRewritesUtf8TrackingFile() {
    InsertRowApplier applier = new InsertRowApplier(new NoOpPreparedStore(), _tempDir);
    String deletedStatementId = "stmt-\u4f60\u597d";
    String retainedStatementId = "stmt-\u041f\u0438\u043d\u043e";

    applier.confirmApplied(deletedStatementId, 0, 0);
    applier.confirmApplied(retainedStatementId, 1, 0);

    applier.cleanAppliedEntries(deletedStatementId);

    InsertRowApplier recovered = new InsertRowApplier(new NoOpPreparedStore(), _tempDir);
    assertFalse(recovered.isApplied(deletedStatementId, 0, 0));
    assertTrue(recovered.isApplied(retainedStatementId, 1, 0));
  }

  private static final class NoOpPreparedStore implements PreparedStore {
    @Override
    public void init(PinotConfiguration config, File dataDir) {
    }

    @Override
    public void store(String statementId, int partitionId, long sequenceNo, byte[] data) {
    }

    @Override
    public byte[] load(String statementId, int partitionId, long sequenceNo) {
      return null;
    }

    @Override
    public List<String> listPreparedStatements() {
      return Collections.emptyList();
    }

    @Override
    public void cleanup(String statementId) {
    }
  }
}
