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
package org.apache.pinot.core.segment.index;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.geospatial.GeometryUtils;
import org.apache.pinot.core.realtime.impl.geospatial.MutableH3Index;
import org.apache.pinot.core.segment.creator.GeoSpatialIndexCreator;
import org.apache.pinot.core.segment.creator.impl.V1Constants;
import org.apache.pinot.core.segment.creator.impl.inv.geospatial.H3IndexResolution;
import org.apache.pinot.core.segment.creator.impl.inv.geospatial.OffHeapH3IndexCreator;
import org.apache.pinot.core.segment.creator.impl.inv.geospatial.OnHeapH3IndexCreator;
import org.apache.pinot.core.segment.index.readers.H3IndexReader;
import org.apache.pinot.core.segment.index.readers.geospatial.ImmutableH3IndexReader;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.core.util.H3Utils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class H3IndexTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "H3IndexCreatorTest");
  private static final Random RANDOM = new Random();

  @BeforeClass
  public void setUp()
      throws Exception {
    if (TEMP_DIR.exists()) {
      FileUtils.forceDelete(TEMP_DIR);
    }
    FileUtils.forceMkdir(TEMP_DIR);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @Test
  public void testH3Index()
      throws Exception {
    int numUniqueH3Ids = 123_456;
    Map<Long, Integer> expectedCardinalities = new HashMap<>();
    String onHeapColumnName = "onHeap";
    String offHeapColumnName = "offHeap";
    int resolution = 5;
    H3IndexResolution h3IndexResolution = new H3IndexResolution(Collections.singletonList(resolution));

    try (MutableH3Index mutableH3Index = new MutableH3Index(h3IndexResolution)) {
      try (GeoSpatialIndexCreator onHeapCreator = new OnHeapH3IndexCreator(TEMP_DIR, onHeapColumnName,
          h3IndexResolution);
          GeoSpatialIndexCreator offHeapCreator = new OffHeapH3IndexCreator(TEMP_DIR, offHeapColumnName,
              h3IndexResolution)) {
        while (expectedCardinalities.size() < numUniqueH3Ids) {
          double longitude = RANDOM.nextDouble() * 360 - 180;
          double latitude = RANDOM.nextDouble() * 180 - 90;
          Point point = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(longitude, latitude));
          onHeapCreator.add(point);
          offHeapCreator.add(point);
          mutableH3Index.add(point);
          long h3Id = H3Utils.H3_CORE.geoToH3(latitude, longitude, resolution);
          expectedCardinalities.merge(h3Id, 1, Integer::sum);
        }
        onHeapCreator.seal();
        offHeapCreator.seal();
      }

      File onHeapH3IndexFile = new File(TEMP_DIR, onHeapColumnName + V1Constants.Indexes.H3_INDEX_FILE_EXTENSION);
      File offHeapH3IndexFile = new File(TEMP_DIR, offHeapColumnName + V1Constants.Indexes.H3_INDEX_FILE_EXTENSION);
      try (PinotDataBuffer onHeapDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(onHeapH3IndexFile);
          PinotDataBuffer offHeapDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(offHeapH3IndexFile);
          H3IndexReader onHeapIndexReader = new ImmutableH3IndexReader(onHeapDataBuffer);
          H3IndexReader offHeapIndexReader = new ImmutableH3IndexReader(offHeapDataBuffer)) {
        H3IndexReader[] indexReaders = new H3IndexReader[]{onHeapIndexReader, offHeapIndexReader, mutableH3Index};
        for (H3IndexReader indexReader : indexReaders) {
          Assert.assertEquals(indexReader.getH3IndexResolution().getLowestResolution(), resolution);
          for (Map.Entry<Long, Integer> entry : expectedCardinalities.entrySet()) {
            Assert.assertEquals(indexReader.getDocIds(entry.getKey()).getCardinality(), (int) entry.getValue());
          }
        }
      }
    }
  }
}
