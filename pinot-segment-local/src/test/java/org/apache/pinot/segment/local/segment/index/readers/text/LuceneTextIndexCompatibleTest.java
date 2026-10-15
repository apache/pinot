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
package org.apache.pinot.segment.local.segment.index.readers.text;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCombined;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class LuceneTextIndexCompatibleTest {

  @Test
  public void testLucene80IndexReader()
      throws IOException {
    File fixturePath =
        new File(LuceneTextIndexCompatibleTest.class.getClassLoader().getResource("data/lucene_80_index").getPath());
    File indexPath = Files.createTempDirectory("lucene80-compatibility").toFile();
    try {
      FileUtils.copyDirectory(fixturePath, indexPath);
      try (LuceneTextIndexReader reader = new LuceneTextIndexReader("Text", indexPath, 1000, Map.of())) {
        assertEquals(reader.getDocIds("1", null).getCardinality(), 35);
      }

      File combinedFile = new File(indexPath, "Text.combined.index");
      LuceneTextIndexCombined.combineLuceneIndexFiles(new File(indexPath, "Text.lucene.index"),
          combinedFile.getAbsolutePath());
      try (PinotDataBuffer buffer = PinotDataBuffer.mapFile(combinedFile, true, 0, combinedFile.length(),
          ByteOrder.LITTLE_ENDIAN, "lucene80-compatibility");
          LuceneTextIndexReader reader = new LuceneTextIndexReader("Text", buffer, 1000,
              new TextIndexConfigBuilder().build())) {
        assertEquals(reader.getDocIds("1", null).getCardinality(), 35);
      }
    } finally {
      FileUtils.deleteDirectory(indexPath);
    }
  }
}
