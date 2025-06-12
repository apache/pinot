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
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.vector.HnswVectorIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.vector.HnswVectorIndexReader;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class HnswVectorIndexCreatorTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), HnswVectorIndexCreatorTest.class.toString());

  @BeforeMethod
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(INDEX_DIR);
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Test
  public void testIndexWriterReader()
      throws IOException {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "1536");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    try (HnswVectorIndexCreator creator = new HnswVectorIndexCreator("foo", INDEX_DIR, config)) {
      float[] values1 = new float[] {5.0F, 42.0F, 54.33333F, 42.24F, 1001.045F};
      creator.add(values1);
      float[] values2 = new float[] {42.0F, 23423.0F, 42431.32532F, 6785676.3242F, 42.3F};
      creator.add(values2);
      float[] values3 = new float[] {1.0F, 2.0F, 3.0F, 4.0F, 5.0F};
      creator.add(values3);
      float[] values4 = new float[] {42.678F, 23423423.0F, 42431.32523432F, 6723485.3242F, 42342.3F};
      creator.add(values4);
      creator.seal();
    }

    // Use TextIndex reader to validate that reads work
    try (HnswVectorIndexReader reader = new HnswVectorIndexReader("foo", INDEX_DIR, 4, config)) {
      int[] matchedDocIds = reader.getDocIds(new float[]{5.0F, 42.0F, 54.33333F, 42.24F, 3413.4F}, 3).toArray();
      Assert.assertEquals(matchedDocIds.length, 3);
      Assert.assertEquals(matchedDocIds[0], 0);
      Assert.assertEquals(matchedDocIds[1], 2);
      Assert.assertEquals(matchedDocIds[1], 2);

      matchedDocIds = reader.getDocIds(new float[]{1.0F, 2.0F, 3.0F, 4.0F, 5.0F}, 1).toArray();
      Assert.assertEquals(matchedDocIds.length, 1);
      Assert.assertEquals(matchedDocIds[0], 2);

      matchedDocIds = reader.getDocIds(new float[]{42.678F, 23423423.0F, 42431.32523432F, 6723485.3242F, 41.3F}, 1)
          .toArray();
      Assert.assertEquals(matchedDocIds.length, 1);
      Assert.assertEquals(matchedDocIds[0], 3);

      matchedDocIds = reader.getDocIds(new float[]{42.0F, 23423.0F, 42431.32532F, 6785676.3242F, 42.3F}, 1)
          .toArray();
      Assert.assertEquals(matchedDocIds.length, 1);
      Assert.assertEquals(matchedDocIds[0], 1);
    }
  }
}
