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
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
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
}
