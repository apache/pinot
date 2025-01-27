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
package org.apache.pinot.segment.local.segment.index.readers;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.segment.creator.impl.nullvalue.NullValueVectorCreator;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class NullValueVectorReaderImplTest implements PinotBuffersAfterMethodCheckRule {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "NullValueVectorReaderImplTest");
  private static final String COLUMN_NAME = "test";

  @BeforeClass
  public void setup()
      throws IOException {
    if (TEMP_DIR.exists()) {
      FileUtils.deleteQuietly(TEMP_DIR);
    }
    TEMP_DIR.mkdir();
    NullValueVectorCreator creator = new NullValueVectorCreator(TEMP_DIR, COLUMN_NAME);
    for (int i = 0; i < 100; i++) {
      creator.setNull(i);
    }
    creator.seal();
  }

  @Test
  public void testNullValueVectorReader()
      throws IOException {
    Assert.assertEquals(TEMP_DIR.list().length, 1);
    File nullValueFile = new File(TEMP_DIR, TEMP_DIR.list()[0]);
    try (PinotDataBuffer buffer = PinotDataBuffer.loadBigEndianFile(nullValueFile)) {
      NullValueVectorReader reader = new NullValueVectorReaderImpl(buffer);
      for (int i = 0; i < 100; i++) {
        Assert.assertTrue(reader.isNull(i));
      }
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
