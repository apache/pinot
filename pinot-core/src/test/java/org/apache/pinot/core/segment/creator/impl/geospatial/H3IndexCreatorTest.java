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
package org.apache.pinot.core.segment.creator.impl.geospatial;

import com.google.common.collect.Lists;
import com.uber.h3core.H3Core;
import java.io.File;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.segment.index.readers.geospatial.H3IndexReader;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class H3IndexCreatorTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "H3IndexCreatorTest");
  private static final String COLUMN_NAME = "geo_col";

  @BeforeClass
  public void setUp()
      throws Exception {
    if (TEMP_DIR.exists()) {
      FileUtils.deleteQuietly(TEMP_DIR);
    }
    TEMP_DIR.mkdir();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @Test
  public void testH3IndexCreation()
      throws Exception {
    FieldSpec spec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.STRING, true);
    int resolution = 5;
    H3IndexCreator creator = new H3IndexCreator(TEMP_DIR, spec, new H3IndexResolution(Lists.newArrayList(resolution)));
    Random rand = new Random();
    H3Core h3Core = H3Core.newInstance();
    Map<Long, Integer> map = new HashMap<>();
    for (int i = 0; i < 10000; i++) {
      int lat = rand.nextInt(10);
      int lon = rand.nextInt(10);
      creator.add(i, lat, lon);
      long h3 = h3Core.geoToH3(lat, lon, resolution);
      Integer count = map.get(h3);
      if (count != null) {
        map.put(h3, count + 1);
      } else {
        map.put(h3, 1);
      }
    }
    creator.seal();

    File h3IndexFile = new File(TEMP_DIR, COLUMN_NAME + ".h3.index");
    PinotDataBuffer h3IndexBuffer =
        PinotDataBuffer.mapFile(h3IndexFile, true, 0, h3IndexFile.length(), ByteOrder.BIG_ENDIAN, "H3 index file");
    H3IndexReader reader = new H3IndexReader(h3IndexBuffer);
    for (Map.Entry<Long, Integer> entry : map.entrySet()) {
      Long h3 = entry.getKey();
      ImmutableRoaringBitmap docIds = reader.getDocIds(h3);
      Assert.assertEquals((int) map.get(h3), docIds.getCardinality());
    }
  }
}
