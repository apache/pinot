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
package org.apache.pinot.controller.helix.core.ingest;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link FileInsertArtifactPublisher} deep-store staging behavior.
 */
public class FileInsertArtifactPublisherTest {

  private File _tempDir;
  private FileInsertArtifactPublisher _publisher;

  @BeforeMethod
  public void setUp() {
    _tempDir =
        new File(FileUtils.getTempDirectory(), "file-insert-artifact-publisher-test-" + System.currentTimeMillis());
    _tempDir.mkdirs();
    _publisher = new FileInsertArtifactPublisher();
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testStageArtifactWithSchemeLessControllerDataDir() {
    PinotConfiguration config =
        new PinotConfiguration(Collections.singletonMap(FileInsertArtifactPublisher.DEEP_STORE_URI_KEY,
            _tempDir.getAbsolutePath()));
    _publisher.init(config);

    URI stagingDir = _publisher.getStagingDir("stmt-1");
    assertEquals(stagingDir.getScheme(), "file");

    URI artifactUri = _publisher.stageArtifact("stmt-1", "segment.tar.gz",
        new ByteArrayInputStream("payload".getBytes(StandardCharsets.UTF_8)));

    assertEquals(artifactUri.getScheme(), "file");
    assertTrue(new File(artifactUri).exists());
    assertNotNull(_publisher.getStagedArtifacts("stmt-1"));

    _publisher.cleanupArtifacts("stmt-1");

    assertFalse(new File(stagingDir).exists());
    assertNull(_publisher.getStagedArtifacts("stmt-1"));
  }
}
