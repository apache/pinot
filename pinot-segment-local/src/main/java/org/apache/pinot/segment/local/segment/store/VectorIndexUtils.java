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
package org.apache.pinot.segment.local.segment.store;

import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.pinot.segment.local.segment.creator.impl.vector.lucene95.Lucene95Codec;
import org.apache.pinot.segment.local.segment.creator.impl.vector.lucene95.Lucene95HnswVectorsFormat;
import org.apache.pinot.segment.spi.V1Constants.Indexes;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;


public class VectorIndexUtils {
  private VectorIndexUtils() {
  }

  static void cleanupVectorIndex(File segDir, String column) {
    // Remove the lucene index file and potentially the docId mapping file.
    File luceneIndexFile = new File(segDir, column + Indexes.VECTOR_HNSW_INDEX_FILE_EXTENSION);
    FileUtils.deleteQuietly(luceneIndexFile);
    File luceneMappingFile = new File(segDir, column + Indexes.VECTOR_HNSW_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    FileUtils.deleteQuietly(luceneMappingFile);

    // Remove the native index file
    File nativeIndexFile = new File(segDir, column + Indexes.VECTOR_INDEX_FILE_EXTENSION);
    FileUtils.deleteQuietly(nativeIndexFile);
  }

  static boolean hasVectorIndex(File segDir, String column) {
    return new File(segDir, column + Indexes.VECTOR_HNSW_INDEX_FILE_EXTENSION).exists() || new File(segDir,
        column + Indexes.VECTOR_INDEX_FILE_EXTENSION).exists();
  }

  public static VectorSimilarityFunction toSimilarityFunction(
      VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    switch (distanceFunction) {
      case COSINE:
        return VectorSimilarityFunction.COSINE;
      case INNER_PRODUCT:
        return VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
      case EUCLIDEAN:
        return VectorSimilarityFunction.EUCLIDEAN;
      case DOT_PRODUCT:
        return VectorSimilarityFunction.DOT_PRODUCT;
      default:
        throw new IllegalArgumentException("Unknown distance function: " + distanceFunction);
    }
  }

  public static IndexWriterConfig getIndexWriterConfig(VectorIndexConfig vectorIndexConfig) {
    IndexWriterConfig indexWriterConfig = new IndexWriterConfig();

    double maxBufferSizeMB = Double.parseDouble(vectorIndexConfig.getProperties()
        .getOrDefault("maxBufferSizeMB", String.valueOf(IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB)));
    boolean commit = Boolean.parseBoolean(vectorIndexConfig.getProperties()
        .getOrDefault("commit", String.valueOf(IndexWriterConfig.DEFAULT_COMMIT_ON_CLOSE)));
    boolean useCompoundFile = Boolean.parseBoolean(vectorIndexConfig.getProperties()
        .getOrDefault("useCompoundFile", String.valueOf(IndexWriterConfig.DEFAULT_USE_COMPOUND_FILE_SYSTEM)));
    indexWriterConfig.setRAMBufferSizeMB(maxBufferSizeMB);
    indexWriterConfig.setCommitOnClose(commit);
    indexWriterConfig.setUseCompoundFile(useCompoundFile);

    int maxCon = Integer.parseInt(vectorIndexConfig.getProperties()
        .getOrDefault("maxCon", String.valueOf(Lucene95HnswVectorsFormat.DEFAULT_MAX_CONN)));
    int beamWidth = Integer.parseInt(vectorIndexConfig.getProperties()
        .getOrDefault("beamWidth", String.valueOf(Lucene95HnswVectorsFormat.DEFAULT_BEAM_WIDTH)));
    int maxDimensions = Integer.parseInt(vectorIndexConfig.getProperties()
        .getOrDefault("maxDimensions", String.valueOf(Lucene95HnswVectorsFormat.DEFAULT_MAX_DIMENSIONS)));

    Lucene95HnswVectorsFormat knnVectorsFormat =
        new Lucene95HnswVectorsFormat(maxCon, beamWidth, maxDimensions);

    Lucene95Codec.Mode mode = Lucene95Codec.Mode.valueOf(vectorIndexConfig.getProperties()
        .getOrDefault("mode", Lucene95Codec.Mode.BEST_SPEED.name()));
    indexWriterConfig.setCodec(new Lucene95Codec(mode, knnVectorsFormat));
    return indexWriterConfig;
  }
}
