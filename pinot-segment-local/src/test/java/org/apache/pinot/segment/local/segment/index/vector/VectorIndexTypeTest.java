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
package org.apache.pinot.segment.local.segment.index.vector;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class VectorIndexTypeTest {

  @Test
  public void testReaderFactoryReturnsNullWhenConfiguredBackendArtifactIsMissing()
      throws Exception {
    File indexDir = new File(FileUtils.getTempDirectory(), "vector-index-type-test-" + System.nanoTime());
    FileUtils.deleteQuietly(indexDir);
    try {
      Assert.assertTrue(indexDir.mkdirs());
      FileUtils.touch(new File(indexDir, "embedding" + V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION));

      SegmentDirectory segmentDirectory = Mockito.mock(SegmentDirectory.class);
      SegmentDirectory.Reader segmentReader = Mockito.mock(SegmentDirectory.Reader.class);
      Mockito.when(segmentDirectory.getPath()).thenReturn(indexDir.toPath());
      Mockito.when(segmentReader.toSegmentDirectory()).thenReturn(segmentDirectory);
      Mockito.when(segmentReader.hasIndexFor("embedding", StandardIndexes.vector())).thenReturn(true);

      ColumnMetadata metadata = Mockito.mock(ColumnMetadata.class);
      Mockito.when(metadata.getColumnName()).thenReturn("embedding");
      Mockito.when(metadata.getDataType()).thenReturn(FieldSpec.DataType.FLOAT);
      Mockito.when(metadata.getFieldSpec())
          .thenReturn(new DimensionFieldSpec("embedding", FieldSpec.DataType.FLOAT, false));

      VectorIndexConfig vectorIndexConfig = new VectorIndexConfig(false, "IVF_PQ", 4, 1,
          VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN,
          Map.of("nlist", "2", "pqM", "2", "pqNbits", "4", "trainSampleSize", "8"));
      FieldIndexConfigs fieldIndexConfigs =
          new FieldIndexConfigs.Builder().add(StandardIndexes.vector(), vectorIndexConfig).build();

      Assert.assertNull(StandardIndexes.vector().getReaderFactory()
          .createIndexReader(segmentReader, fieldIndexConfigs, metadata));
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  @Test
  public void testReaderFactoryReturnsNullWhenVectorIndexIsDisabled()
      throws Exception {
    File indexDir = new File(FileUtils.getTempDirectory(), "vector-index-type-disabled-test-" + System.nanoTime());
    FileUtils.deleteQuietly(indexDir);
    try {
      Assert.assertTrue(indexDir.mkdirs());
      FileUtils.touch(new File(indexDir, "embedding" + V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION));

      SegmentDirectory segmentDirectory = Mockito.mock(SegmentDirectory.class);
      SegmentDirectory.Reader segmentReader = Mockito.mock(SegmentDirectory.Reader.class);
      Mockito.when(segmentDirectory.getPath()).thenReturn(indexDir.toPath());
      Mockito.when(segmentReader.toSegmentDirectory()).thenReturn(segmentDirectory);
      Mockito.when(segmentReader.hasIndexFor("embedding", StandardIndexes.vector())).thenReturn(true);

      ColumnMetadata metadata = Mockito.mock(ColumnMetadata.class);
      Mockito.when(metadata.getColumnName()).thenReturn("embedding");
      Mockito.when(metadata.getDataType()).thenReturn(FieldSpec.DataType.FLOAT);
      Mockito.when(metadata.getFieldSpec())
          .thenReturn(new DimensionFieldSpec("embedding", FieldSpec.DataType.FLOAT, false));

      FieldIndexConfigs fieldIndexConfigs =
          new FieldIndexConfigs.Builder().add(StandardIndexes.vector(), VectorIndexConfig.DISABLED).build();

      Assert.assertNull(StandardIndexes.vector().getReaderFactory()
          .createIndexReader(segmentReader, fieldIndexConfigs, metadata));
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /**
   * Regression: with {@code storeInSegmentFile=true} but no consolidated entry in columns.psf (e.g.
   * a legacy sidecar that the handler has not yet absorbed), the IVF reader factory must fall back
   * to the on-disk sidecar and return a usable reader — mirroring the HNSW fallback — rather than
   * returning null and silently disabling the index (forcing an exact scan).
   */
  @Test
  public void testReaderFactoryFallsBackToSidecarWhenStoreInSegmentFileButNoConsolidatedEntry()
      throws Exception {
    File indexDir = new File(FileUtils.getTempDirectory(), "vector-index-type-fallback-test-" + System.nanoTime());
    FileUtils.deleteQuietly(indexDir);
    try {
      Assert.assertTrue(indexDir.mkdirs());
      int dimension = 4;
      // Build a real IVF_FLAT sidecar at embedding.vector.ivfflat.index in the segment dir.
      Map<String, String> creatorProps = new HashMap<>();
      creatorProps.put("vectorIndexType", "IVF_FLAT");
      creatorProps.put("vectorDimension", String.valueOf(dimension));
      creatorProps.put("vectorDistanceFunction", "EUCLIDEAN");
      creatorProps.put("nlist", "2");
      creatorProps.put("trainingSeed", "42");
      VectorIndexConfig creatorConfig = new VectorIndexConfig(false, "IVF_FLAT", dimension, 1,
          VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, creatorProps);
      try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator("embedding", indexDir, creatorConfig)) {
        for (int i = 0; i < 8; i++) {
          creator.add(new float[] {i, i + 1, i + 2, i + 3});
        }
        creator.seal();
      }
      Assert.assertTrue(
          new File(indexDir, "embedding" + V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION).exists(),
          "test setup: IVF_FLAT sidecar must exist");

      SegmentDirectory segmentDirectory = Mockito.mock(SegmentDirectory.class);
      SegmentDirectory.Reader segmentReader = Mockito.mock(SegmentDirectory.Reader.class);
      Mockito.when(segmentDirectory.getPath()).thenReturn(indexDir.toPath());
      Mockito.when(segmentReader.toSegmentDirectory()).thenReturn(segmentDirectory);
      Mockito.when(segmentReader.hasIndexFor("embedding", StandardIndexes.vector())).thenReturn(true);
      // No consolidated typed entry yet: getConsolidatedVectorEntry sees the store's "absent" signal
      // and returns null, which must trigger the sidecar fallback (not a null reader).
      Mockito.when(segmentReader.getIndexFor("embedding", StandardIndexes.vector()))
          .thenThrow(new RuntimeException("Could not find index for column: embedding, type: vector"));

      ColumnMetadata metadata = Mockito.mock(ColumnMetadata.class);
      Mockito.when(metadata.getColumnName()).thenReturn("embedding");
      Mockito.when(metadata.getDataType()).thenReturn(FieldSpec.DataType.FLOAT);
      Mockito.when(metadata.getFieldSpec())
          .thenReturn(new DimensionFieldSpec("embedding", FieldSpec.DataType.FLOAT, false));

      Map<String, String> props = new HashMap<>(creatorProps);
      props.put(VectorIndexConfig.STORE_IN_SEGMENT_FILE, "true");
      VectorIndexConfig storeInSegmentFileConfig = new VectorIndexConfig(false, "IVF_FLAT", dimension, 1,
          VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, props);
      FieldIndexConfigs fieldIndexConfigs =
          new FieldIndexConfigs.Builder().add(StandardIndexes.vector(), storeInSegmentFileConfig).build();

      VectorIndexReader reader = StandardIndexes.vector().getReaderFactory()
          .createIndexReader(segmentReader, fieldIndexConfigs, metadata);
      Assert.assertNotNull(reader, "IVF reader must fall back to the on-disk sidecar, not return null");
      // The reader owns the sidecar mmap (ownsBuffer=true) and must close without error.
      reader.close();
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }
}
