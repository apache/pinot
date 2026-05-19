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
package org.apache.pinot.segment.local.segment.index.loader.invertedindex;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for {@link VectorIndexHandler} backend-drift handling.
 */
public class VectorIndexHandlerTest {
  private static final String COLUMN = "embedding";

  @Test
  public void testNeedUpdateIndicesReturnsTrueWhenVectorBackendChanges()
      throws Exception {
    File indexDir = createSegmentDirWithVectorIndex(V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
    try {
      SegmentDirectory segmentDirectory = mockSegmentDirectory(indexDir);
      SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
      when(reader.toSegmentDirectory()).thenReturn(segmentDirectory);

      VectorIndexHandler handler = createHandler(segmentDirectory, vectorIndexConfig("IVF_PQ"));

      assertTrue(handler.needUpdateIndices(reader));
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  @Test
  public void testUpdateIndicesRemovesIndexWhenVectorBackendChanges()
      throws Exception {
    File indexDir = createSegmentDirWithVectorIndex(V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
    try {
      SegmentDirectory segmentDirectory = mockSegmentDirectory(indexDir);
      SegmentDirectory.Writer writer = mock(SegmentDirectory.Writer.class);
      when(writer.toSegmentDirectory()).thenReturn(segmentDirectory);

      VectorIndexHandler handler = createHandler(segmentDirectory, vectorIndexConfig("IVF_PQ"));
      handler.updateIndices(writer);

      verify(writer).removeIndex(COLUMN, StandardIndexes.vector());
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  private static VectorIndexHandler createHandler(SegmentDirectory segmentDirectory,
      VectorIndexConfig vectorIndexConfig) {
    FieldIndexConfigs fieldIndexConfigs =
        new FieldIndexConfigs.Builder().add(StandardIndexes.vector(), vectorIndexConfig).build();
    return new VectorIndexHandler(segmentDirectory, Map.of(COLUMN, fieldIndexConfigs), mock(TableConfig.class),
        mock(Schema.class));
  }

  private static SegmentDirectory mockSegmentDirectory(File indexDir) {
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn("testSegment");
    when(segmentMetadata.getIndexDir()).thenReturn(indexDir);
    when(segmentMetadata.getTotalDocs()).thenReturn(10);
    when(segmentMetadata.getAllColumns()).thenReturn(new TreeSet<>(Set.of(COLUMN)));
    when(segmentMetadata.getColumnMetadataMap()).thenReturn(new TreeMap<>());

    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    when(segmentDirectory.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(segmentDirectory.getColumnsWithIndex(StandardIndexes.vector())).thenReturn(Set.of(COLUMN));
    return segmentDirectory;
  }

  private static VectorIndexConfig vectorIndexConfig(String backend) {
    return new VectorIndexConfig(false, backend, 4, 1, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN,
        Map.of("nlist", "2", "pqM", "2", "pqNbits", "4", "trainSampleSize", "8"));
  }

  private static File createSegmentDirWithVectorIndex(String suffix)
      throws Exception {
    File indexDir = new File(FileUtils.getTempDirectory(), "vector-index-handler-test-" + System.nanoTime());
    FileUtils.deleteQuietly(indexDir);
    assertTrue(indexDir.mkdirs());
    File v3Dir = new File(indexDir, SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    assertTrue(v3Dir.mkdir());
    FileUtils.touch(new File(v3Dir, COLUMN + suffix));
    return indexDir;
  }
}
