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
package org.apache.pinot.segment.local.segment.creator.impl.vector;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;
import org.apache.pinot.segment.local.segment.creator.impl.vector.lucene99.HnswVectorIndexCombined;
import org.apache.pinot.segment.local.segment.store.VectorIndexUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfigValidator;
import org.apache.pinot.segment.spi.index.creator.VectorIndexCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// This is used to create Lucene based HNSW index.
/// Used for both offline from [org.apache.pinot.segment.local.segment.creator.impl.SegmentColumnarIndexCreator]
/// and realtime from [org.apache.pinot.segment.local.realtime.impl.vector.MutableVectorIndex]
public class HnswVectorIndexCreator implements VectorIndexCreator {
  private static final Logger LOGGER = LoggerFactory.getLogger(HnswVectorIndexCreator.class);
  public static final String VECTOR_INDEX_DOC_ID_COLUMN_NAME = "DocID";
  /// Default heap budget for vectors buffered for a parallel build, so the default doc limit shrinks as the dimension
  /// grows (about 10k docs at 1536 dims) instead of holding a fixed number of vectors of any size.
  private static final long DEFAULT_PARALLEL_BUILD_MAX_BYTES = 64L * 1024 * 1024;
  private static final int DEFAULT_PARALLEL_BUILD_MAX_DOCS = 50_000;
  private static final long PART_SHUTDOWN_TIMEOUT_SECONDS = 60;

  private final String _vectorColumn;
  private final VectorSimilarityFunction _vectorSimilarityFunction;
  private final int _vectorDimension;
  // When true, close() packs the Lucene directory into a single combined file and removes the dir.
  private final boolean _storeInSegmentFile;
  private final File _segmentIndexDir;
  private final File _hnswIndexDir;
  private final Directory _indexDirectory;
  private final IndexWriter _indexWriter;

  /// With `buildThreads` > 1 (at most [VectorIndexConfigValidator#MAX_HNSW_BUILD_THREADS]), vectors are buffered and
  /// indexed from that many threads on seal: each thread fills its own Lucene segment, so the graphs build in
  /// parallel. With the default `forceMerge=true` they are then merged into one graph using the same number of merge
  /// workers. With `forceMerge=false` the graphs are kept as separate Lucene segments and searched together: the
  /// build is faster, but each query searches every graph, so query latency is higher (2-3x in one benchmark with 4
  /// threads). The number of graphs is at least `buildThreads`, and more when a thread's vectors exceed
  /// `maxBufferSizeMB` and Lucene flushes it more than once.
  private final int _buildThreads;
  private final boolean _forceMerge;
  private final VectorIndexConfig _vectorIndexConfig;
  /// Above this many vectors the buffered ones are indexed on the calling thread and buffering stops, so large
  /// segments keep the single-writer path. Defaults to as many vectors as fit in 64 MB, at most 50,000. While
  /// buffering, the build holds the vectors on heap plus up to one `maxBufferSizeMB` per thread.
  private final int _parallelBuildMaxDocs;
  /// Runs the graph merge workers on seal, when building in parallel with `forceMerge=true`
  @Nullable
  private final ExecutorService _mergeExecutor;
  @Nullable
  private List<float[]> _bufferedVectors;

  private int _nextDocId = 0;

  public HnswVectorIndexCreator(String column, File segmentIndexDir, VectorIndexConfig vectorIndexConfig) {
    _vectorColumn = column;
    _vectorDimension = vectorIndexConfig.getVectorDimension();
    _vectorSimilarityFunction = VectorIndexUtils.toSimilarityFunction(vectorIndexConfig.getVectorDistanceFunction());
    _storeInSegmentFile = vectorIndexConfig.isStoreInSegmentFile();
    _segmentIndexDir = segmentIndexDir;
    Map<String, String> properties = vectorIndexConfig.getProperties();
    // Clamped as well as validated, since configs written before validation covered these keys skip it
    _buildThreads = Math.min(VectorIndexConfigValidator.MAX_HNSW_BUILD_THREADS,
        Math.max(1, Integer.parseInt(properties.getOrDefault("buildThreads", "1"))));
    _forceMerge = Boolean.parseBoolean(properties.getOrDefault("forceMerge", "true"));
    String parallelBuildMaxDocs = properties.get("parallelBuildMaxDocs");
    _parallelBuildMaxDocs = parallelBuildMaxDocs != null ? Integer.parseInt(parallelBuildMaxDocs)
        : (int) Math.max(1, Math.min(DEFAULT_PARALLEL_BUILD_MAX_DOCS,
            DEFAULT_PARALLEL_BUILD_MAX_BYTES / ((long) Float.BYTES * _vectorDimension)));
    _bufferedVectors = _buildThreads > 1 ? new ArrayList<>() : null;
    _mergeExecutor = _buildThreads > 1 && _forceMerge ? Executors.newFixedThreadPool(_buildThreads) : null;
    _vectorIndexConfig = vectorIndexConfig;
    // Segment generation is always in V1 and later we convert (as part of post creation processing) to V3 if
    // segmentVersion is set to V3 in SegmentGeneratorConfig.
    _hnswIndexDir =
        new File(segmentIndexDir, _vectorColumn + V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);

    Directory indexDirectory = null;
    IndexWriter indexWriter = null;
    try {
      indexDirectory = FSDirectory.open(_hnswIndexDir.toPath());
      LOGGER.info("Creating HNSW index for column: {} at path: {} with {} for segment: {}", column,
          _hnswIndexDir.getAbsolutePath(), vectorIndexConfig.getProperties(), segmentIndexDir.getAbsolutePath());
      indexWriter = new IndexWriter(indexDirectory, _mergeExecutor != null
          ? VectorIndexUtils.getIndexWriterConfig(vectorIndexConfig, _buildThreads, _mergeExecutor)
          : VectorIndexUtils.getIndexWriterConfig(vectorIndexConfig));
    } catch (Exception e) {
      // IndexWriter does not close the Directory passed to it, so both need to be closed.
      try {
        IOUtils.close(indexWriter, indexDirectory);
      } catch (Exception closeEx) {
        e.addSuppressed(closeEx);
      }
      if (_mergeExecutor != null) {
        _mergeExecutor.shutdownNow();
      }
      throw new RuntimeException(
          "Caught exception while instantiating the HnswVectorIndexCreator for column: " + column, e);
    }
    _indexDirectory = indexDirectory;
    _indexWriter = indexWriter;
  }

  @Override
  public void add(Object[] values, @Nullable int[] dictIds) {
    float[] floatValues = new float[_vectorDimension];
    for (int i = 0; i < values.length; i++) {
      floatValues[i] = (Float) values[i];
    }
    add(floatValues);
  }

  @Override
  public void add(float[] document) {
    if (_bufferedVectors != null) {
      // Callers may reuse the array for the next row
      _bufferedVectors.add(document.clone());
      if (_bufferedVectors.size() <= _parallelBuildMaxDocs) {
        return;
      }
      for (float[] buffered : _bufferedVectors) {
        addDocument(buffered, _nextDocId++);
      }
      _bufferedVectors = null;
      return;
    }
    addDocument(document, _nextDocId++);
  }

  private Document toDocument(float[] vector, int docId) {
    Document docToIndex = new Document();
    docToIndex.add(new XKnnFloatVectorField(_vectorColumn, vector, _vectorSimilarityFunction));
    docToIndex.add(new StoredField(VECTOR_INDEX_DOC_ID_COLUMN_NAME, docId));
    return docToIndex;
  }

  private void addDocument(float[] vector, int docId) {
    try {
      _indexWriter.addDocument(toDocument(vector, docId));
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while adding a new document to the HNSW index for column: " + _vectorColumn, e);
    }
  }

  /// Indexes the buffered vectors from `_buildThreads` threads, each a contiguous range of doc ids written by its own
  /// IndexWriter, so each graph is also finished (flushed) on its own thread; the per-thread indexes are then attached
  /// to the main index as separate Lucene segments without rebuilding them.
  private void addBufferedInParallel()
      throws Exception {
    int numDocs = _bufferedVectors.size();
    int numThreads = Math.min(_buildThreads, Math.max(1, numDocs));
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    Directory[] partDirectories = new Directory[numThreads];
    File[] partDirs = new File[numThreads];
    try {
      List<Future<?>> futures = new ArrayList<>(numThreads);
      for (int t = 0; t < numThreads; t++) {
        int from = (int) ((long) numDocs * t / numThreads);
        int to = (int) ((long) numDocs * (t + 1) / numThreads);
        partDirs[t] = new File(_segmentIndexDir, _vectorColumn + ".hnsw.part" + t);
        partDirectories[t] = FSDirectory.open(partDirs[t].toPath());
        Directory partDirectory = partDirectories[t];
        futures.add(executor.submit(() -> {
          try (IndexWriter partWriter = new IndexWriter(partDirectory,
              VectorIndexUtils.getIndexWriterConfig(_vectorIndexConfig))) {
            for (int docId = from; docId < to; docId++) {
              partWriter.addDocument(toDocument(_bufferedVectors.get(docId), docId));
            }
            partWriter.commit();
          }
          return null;
        }));
      }
      for (Future<?> future : futures) {
        future.get();
      }
      _indexWriter.addIndexes(partDirectories);
    } finally {
      // On failure the other part writers may still be running: wait for them before closing and deleting their
      // directories, so no files are written after the cleanup
      executor.shutdownNow();
      if (!executor.awaitTermination(PART_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        LOGGER.warn("HNSW part writers for column: {} did not stop within {}s", _vectorColumn,
            PART_SHUTDOWN_TIMEOUT_SECONDS);
      }
      IOUtils.close(partDirectories);
      for (File partDir : partDirs) {
        if (partDir != null) {
          FileUtils.deleteQuietly(partDir);
        }
      }
    }
    _nextDocId = numDocs;
    _bufferedVectors.clear();
  }

  @Override
  public void seal() {
    try {
      LOGGER.info("Sealing HNSW index for column: {} (buildThreads: {}, forceMerge: {})", _vectorColumn,
          _buildThreads, _forceMerge);
      if (_bufferedVectors != null) {
        addBufferedInParallel();
      }
      if (_forceMerge) {
        _indexWriter.forceMerge(1);
      }
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while sealing the HNSW index for column: " + _vectorColumn, e);
    } finally {
      shutDownMergeExecutor();
    }
  }

  private void shutDownMergeExecutor() {
    if (_mergeExecutor != null) {
      _mergeExecutor.shutdownNow();
    }
  }

  @Override
  public void close()
      throws IOException {
    // Covers a creator closed without seal()
    shutDownMergeExecutor();
    try {
      // Based on the commit flag set in IndexWriterConfig, closing the writer will decide to commit or not.
      // IndexWriter does not close the Directory passed to it, so both need to be closed.
      IOUtils.close(_indexWriter, _indexDirectory);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while closing the HNSW index for column: " + _vectorColumn, e);
    }
    if (_storeInSegmentFile) {
      // Pack the Lucene directory into a single combined file so the V2→V3 converter can absorb it
      // into columns.psf as a typed entry (same pattern as LuceneTextIndexCreator).
      String combinedPath = new File(_segmentIndexDir,
          _vectorColumn + V1Constants.Indexes.VECTOR_HNSW_COMBINED_INDEX_FILE_EXTENSION).getAbsolutePath();
      try {
        HnswVectorIndexCombined.combineHnswIndexFiles(_hnswIndexDir, combinedPath, _segmentIndexDir, _vectorColumn);
        FileUtils.deleteDirectory(_hnswIndexDir);
        LOGGER.info("Packed HNSW index for column {} into combined file and removed Lucene directory", _vectorColumn);
      } catch (IOException e) {
        LOGGER.error("Failed to combine HNSW index for column: {}", _vectorColumn, e);
        throw e;
      }
    }
  }
}
