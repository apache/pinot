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

import com.google.common.io.Closer;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.pinot.segment.local.segment.creator.impl.vector.lucene99.HnswVectorIndexCombined;
import org.apache.pinot.segment.local.segment.store.VectorIndexUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.creator.VectorIndexCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// This is used to create Lucene based HNSW index.
/// Builds the HNSW index for immutable segments during generation and index rebuilds.
public class HnswVectorIndexCreator implements VectorIndexCreator {
  private static final Logger LOGGER = LoggerFactory.getLogger(HnswVectorIndexCreator.class);
  public static final String VECTOR_INDEX_DOC_ID_COLUMN_NAME = "DocID";
  /// Default heap budget for vectors buffered for a parallel build, so the default doc limit shrinks as the dimension
  /// grows (about 10k docs at 1536 dims) instead of holding a fixed number of vectors of any size.
  private static final long DEFAULT_PARALLEL_BUILD_MAX_BYTES = 64L * 1024 * 1024;
  private static final int DEFAULT_PARALLEL_BUILD_MAX_DOCS = 50_000;

  private final String _vectorColumn;
  private final VectorSimilarityFunction _vectorSimilarityFunction;
  private final int _vectorDimension;
  // When true, close() packs the Lucene directory into a single combined file and removes the dir.
  private final boolean _storeInSegmentFile;
  private final File _segmentIndexDir;
  private final File _hnswIndexDir;
  private final Directory _indexDirectory;
  private final IndexWriter _indexWriter;

  private final int _buildThreads;
  private final boolean _forceMerge;
  private final boolean _concurrentMerge;
  private final VectorIndexConfig _vectorIndexConfig;
  /// Above this many vectors the buffered ones are indexed on the calling thread and buffering stops, so large
  /// segments keep the single-writer path. Defaults to as many vectors as fit in 64 MB, at most 50,000. While
  /// buffering, the build holds the vectors on heap plus up to one `maxBufferSizeMB` per thread.
  private final int _parallelBuildMaxDocs;
  /// Runs the graph merge workers on seal, when building in parallel with `forceMerge=true` and `concurrentMerge=true`
  @Nullable
  private final ExecutorService _mergeExecutor;
  @Nullable
  private List<float[]> _bufferedVectors;

  private int _nextDocId = 0;
  private boolean _sealed;
  private boolean _failed;

  public HnswVectorIndexCreator(String column, File segmentIndexDir, VectorIndexConfig vectorIndexConfig) {
    _vectorColumn = column;
    _vectorDimension = vectorIndexConfig.getVectorDimension();
    _vectorSimilarityFunction = VectorIndexUtils.toSimilarityFunction(vectorIndexConfig.getVectorDistanceFunction());
    _storeInSegmentFile = vectorIndexConfig.isStoreInSegmentFile();
    _segmentIndexDir = segmentIndexDir;
    Map<String, String> properties = vectorIndexConfig.getProperties();
    _buildThreads = vectorIndexConfig.getHnswBuildThreads();
    _forceMerge = vectorIndexConfig.isHnswForceMerge();
    _concurrentMerge = vectorIndexConfig.isHnswConcurrentMerge();
    String parallelBuildMaxDocs = properties != null ? properties.get("parallelBuildMaxDocs") : null;
    _parallelBuildMaxDocs = parallelBuildMaxDocs != null ? Integer.parseInt(parallelBuildMaxDocs)
        : (int) Math.max(1, Math.min(DEFAULT_PARALLEL_BUILD_MAX_DOCS,
            DEFAULT_PARALLEL_BUILD_MAX_BYTES / ((long) Float.BYTES * _vectorDimension)));
    _bufferedVectors = _buildThreads > 1 ? new ArrayList<>() : null;
    _mergeExecutor = _concurrentMerge ? Executors.newFixedThreadPool(_buildThreads) : null;
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
      indexWriter = new IndexWriter(indexDirectory, VectorIndexUtils.getImmutableIndexWriterConfig(vectorIndexConfig,
          _segmentIndexDir.toPath(), _mergeExecutor));
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
    if (_failed || _sealed) {
      throw new IllegalStateException("Cannot add to a failed or sealed HNSW index for column: " + _vectorColumn);
    }
    if (_bufferedVectors != null) {
      if (_bufferedVectors.size() < _parallelBuildMaxDocs) {
        // Callers may reuse the array for the next row.
        _bufferedVectors.add(document.clone());
        return;
      }
      for (float[] buffered : _bufferedVectors) {
        addDocument(buffered);
      }
      _bufferedVectors = null;
    }
    addDocument(document);
  }

  private Document toDocument(float[] vector, int docId) {
    Document docToIndex = new Document();
    docToIndex.add(new XKnnFloatVectorField(_vectorColumn, vector, _vectorSimilarityFunction));
    docToIndex.add(new StoredField(VECTOR_INDEX_DOC_ID_COLUMN_NAME, docId));
    return docToIndex;
  }

  private void addDocument(float[] vector) {
    try {
      _indexWriter.addDocument(toDocument(vector, _nextDocId));
      _nextDocId++;
    } catch (ThreadInterruptedException e) {
      _failed = true;
      Thread.currentThread().interrupt();
      throw e;
    } catch (Exception e) {
      _failed = true;
      throw new RuntimeException(
          "Caught exception while adding a new document to the HNSW index for column: " + _vectorColumn, e);
    }
  }

  /// Indexes the buffered vectors from `_buildThreads` threads, each a contiguous range of doc ids written by its own
  /// IndexWriter, so each graph is also finished (flushed) on its own thread; the per-thread indexes are then attached
  /// to the main index as separate Lucene segments without rebuilding them.
  private void addBufferedInParallel(List<float[]> vectors)
      throws Exception {
    int numDocs = vectors.size();
    int numThreads = Math.min(_buildThreads, numDocs);
    if (numThreads == 0) {
      return;
    }
    // Recovery deletes the unfinished main index, including scratch left by a process crash.
    try (Closer parts = Closer.create()) {
      Path scratch = Files.createTempDirectory(_hnswIndexDir.toPath(), "hnsw-parts-");
      parts.register(() -> IOUtils.rm(scratch));
      Directory[] directories = new Directory[numThreads];
      for (int t = 0; t < numThreads; t++) {
        directories[t] = parts.register(FSDirectory.open(scratch.resolve(Integer.toString(t))));
      }
      try (ExecutorService executor = Executors.newFixedThreadPool(numThreads)) {
        ExecutorCompletionService<Void> completed = new ExecutorCompletionService<>(executor);
        try {
          for (int t = 0; t < numThreads; t++) {
            int from = (int) ((long) numDocs * t / numThreads);
            int to = (int) ((long) numDocs * (t + 1) / numThreads);
            Directory directory = directories[t];
            completed.submit(() -> {
              writePart(directory, vectors, from, to);
              return null;
            });
          }
          for (int t = 0; t < numThreads; t++) {
            completed.take().get();
          }
        } finally {
          // Cancel outstanding work on failure. ExecutorService.close() joins every worker before scratch is released.
          executor.shutdownNow();
        }
      }
      if (Thread.interrupted()) {
        throw new InterruptedException("HNSW part build interrupted");
      }
      // addIndexes calls maybeMerge. Keep all parts intact until the optional final forced merge.
      MergePolicy mergePolicy = _indexWriter.getConfig().getMergePolicy();
      _indexWriter.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
      _indexWriter.addIndexes(directories);
      if (_forceMerge) {
        _indexWriter.getConfig().setMergePolicy(mergePolicy);
      }
    }
  }

  private void writePart(Directory directory, List<float[]> vectors, int from, int to)
      throws IOException, InterruptedException {
    try (IndexWriter writer = new IndexWriter(directory, VectorIndexUtils.getIndexWriterConfig(_vectorIndexConfig)
        .setMergePolicy(NoMergePolicy.INSTANCE).setCommitOnClose(false))) {
      for (int docId = from; docId < to; docId++) {
        if (Thread.currentThread().isInterrupted()) {
          throw new InterruptedException("HNSW part build interrupted");
        }
        writer.addDocument(toDocument(vectors.get(docId), docId));
      }
      writer.commit();
    }
  }

  @Override
  public void seal() {
    if (_sealed) {
      return;
    }
    if (_failed) {
      throw new IllegalStateException("Previous HNSW build failed for column: " + _vectorColumn);
    }
    try {
      LOGGER.info("Sealing HNSW index for column: {} (buildThreads: {}, forceMerge: {}, concurrentMerge: {})",
          _vectorColumn, _buildThreads, _forceMerge, _concurrentMerge);
      if (_bufferedVectors != null) {
        addBufferedInParallel(_bufferedVectors);
      }
      if (_forceMerge) {
        _indexWriter.forceMerge(1);
      }
      _sealed = true;
    } catch (InterruptedException | ThreadInterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while sealing the HNSW index for column: " + _vectorColumn, e);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while sealing the HNSW index for column: " + _vectorColumn, e);
    } finally {
      _failed = !_sealed;
      _bufferedVectors = null;
    }
  }

  @Override
  public void close()
      throws IOException {
    _bufferedVectors = null;
    boolean abort = _failed || (_buildThreads > 1 && !_sealed);
    // Rollback must finish even when seal restored the caller's interrupt.
    boolean interrupted = abort && Thread.interrupted();
    try (_indexDirectory) {
      if (abort) {
        _indexWriter.rollback();
        return;
      }
      _indexWriter.close();
    } finally {
      if (_mergeExecutor != null) {
        _mergeExecutor.close();
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
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
