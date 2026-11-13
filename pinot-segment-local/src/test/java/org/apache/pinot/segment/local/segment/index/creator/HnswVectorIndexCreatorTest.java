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

import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.pinot.common.function.scalar.VectorFunctions;
import org.apache.pinot.segment.local.segment.creator.impl.vector.HnswVectorIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.vector.lucene99.HnswVectorsFormat;
import org.apache.pinot.segment.local.segment.index.readers.vector.HnswVectorIndexReader;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class HnswVectorIndexCreatorTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), HnswVectorIndexCreatorTest.class.toString());
  private VectorIndexConfig _config;

  @BeforeMethod
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(INDEX_DIR);

    Map<String, String> properties = new HashMap<>();

    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "1536");

    _config = new VectorIndexConfig(properties);
    try (HnswVectorIndexCreator creator = new HnswVectorIndexCreator("foo", INDEX_DIR, _config)) {
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
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Test
  public void testMmapWriterSelection()
      throws IOException {
    HnswVectorsFormat mmapFormat = new HnswVectorsFormat(16, 100, 1024, INDEX_DIR.toPath());
    HnswVectorsFormat heapFormat = new HnswVectorsFormat(16, 100, 1024);
    IOContext forcedMerge = new IOContext(new MergeInfo(4, 0, false, 1));
    assertFalse(usesHeapWriter(mmapFormat, forcedMerge));
    assertTrue(usesHeapWriter(heapFormat, forcedMerge));
    assertTrue(usesHeapWriter(mmapFormat, IOContext.DEFAULT));
    assertTrue(usesHeapWriter(mmapFormat, new IOContext(new MergeInfo(4, 0, false, -1))));
  }

  private static boolean usesHeapWriter(HnswVectorsFormat format, IOContext context)
      throws IOException {
    try (ByteBuffersDirectory directory = new ByteBuffersDirectory()) {
      SegmentInfo segmentInfo = new SegmentInfo(directory, Version.LATEST, Version.LATEST, "_0", 4, false, false,
          Codec.getDefault(), Map.of(), StringHelper.randomId(), Map.of(), null);
      SegmentWriteState state = new SegmentWriteState(InfoStream.NO_OUTPUT, directory, segmentInfo, FieldInfos.EMPTY,
          null, context);
      try (KnnVectorsWriter writer = format.fieldsWriter(state)) {
        return writer instanceof Lucene99HnswVectorsWriter;
      }
    }
  }

  @Test
  public void testForcedMergeIndexCanBeRead()
      throws IOException {
    float[][] vectors = new float[128][1024];
    Random random = new Random(42);
    for (float[] vector : vectors) {
      for (int dim = 0; dim < vector.length; dim++) {
        vector[dim] = random.nextFloat() - 0.5f;
      }
    }
    MergedIndex heapIndex = mergeAndReadGraph("embeddingHeap", vectors, true);
    MergedIndex mmapIndex = mergeAndReadGraph("embeddingMmap", vectors, false);
    assertEquals(mmapIndex.graph(), heapIndex.graph());
    assertEquals(mmapIndex.nearestDocs(), heapIndex.nearestDocs());
  }

  private static MergedIndex mergeAndReadGraph(String column, float[][] vectors, boolean optOut)
      throws IOException {
    // A low connection limit exercises pruning during the final merge.
    Map<String, String> properties = new HashMap<>(Map.of("vectorDimension", "1024", "maxCon", "4",
        "maxBufferSizeMB", "0.15"));
    if (optOut) {
      properties.put(VectorIndexConfig.MMAP_GRAPH_ON_MERGE, "false");
    }
    VectorIndexConfig config = new VectorIndexConfig(properties);
    Path path = INDEX_DIR.toPath().resolve(column + V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
    try (HnswVectorIndexCreator creator = new HnswVectorIndexCreator(column, INDEX_DIR, config);
        FSDirectory directory = FSDirectory.open(path)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      long leaves = Arrays.stream(directory.listAll()).filter(name -> name.endsWith(".si")).count();
      assertTrue(leaves > 1 && leaves < 10, "Expected multiple Lucene segments below the background merge threshold");
      creator.seal();
    }
    int[] nearestDocs = new int[vectors.length];
    try (HnswVectorIndexReader reader = new HnswVectorIndexReader(column, INDEX_DIR, vectors.length, config)) {
      reader.setEfSearch(vectors.length);
      for (int i = 0; i < vectors.length; i++) {
        int[] matches = reader.getDocIds(vectors[i], 1).toArray();
        assertEquals(matches.length, 1);
        nearestDocs[i] = matches[0];
      }
    }
    try (FSDirectory directory = FSDirectory.open(path); DirectoryReader reader = DirectoryReader.open(directory)) {
      assertEquals(reader.leaves().size(), 1);
      assertEquals(reader.numDocs(), vectors.length);
      SegmentReader segment = (SegmentReader) reader.leaves().getFirst().reader();
      PerFieldKnnVectorsFormat.FieldsReader fields = (PerFieldKnnVectorsFormat.FieldsReader) segment.getVectorReader();
      HnswGraph graph = ((HnswGraphProvider) fields.getFieldReader(column)).getGraph(column);
      List<Map<Integer, List<Integer>>> levels = new ArrayList<>(graph.numLevels());
      for (int level = 0; level < graph.numLevels(); level++) {
        Map<Integer, List<Integer>> edges = new HashMap<>();
        HnswGraph.NodesIterator nodes = graph.getNodesOnLevel(level);
        while (nodes.hasNext()) {
          int node = nodes.nextInt();
          graph.seek(level, node);
          List<Integer> neighbors = new ArrayList<>();
          for (int neighbor = graph.nextNeighbor(); neighbor != NO_MORE_DOCS; neighbor = graph.nextNeighbor()) {
            neighbors.add(neighbor);
          }
          edges.put(node, neighbors);
        }
        levels.add(edges);
      }
      return new MergedIndex(levels, nearestDocs);
    }
  }

  private record MergedIndex(List<Map<Integer, List<Integer>>> graph, int[] nearestDocs) {
  }

  @Test
  public void testIndexWriterReaderWithTop3()
      throws IOException {
    // Use VectorIndex reader to validate that reads work
    try (HnswVectorIndexReader reader = new HnswVectorIndexReader("foo", INDEX_DIR, 4, _config)) {
      int[] matchedDocIds = reader.getDocIds(new float[]{5.0F, 42.0F, 54.33333F, 42.24F, 3413.4F}, 3).toArray();
      // Expect to get 3 matching docIds since topK = 3 is used
      Assert.assertEquals(matchedDocIds.length, 3);
      Assert.assertEquals(matchedDocIds[0], 0);
      Assert.assertEquals(matchedDocIds[1], 2);
      Assert.assertEquals(matchedDocIds[1], 2);
    }
  }

  @Test
  public void testIndexWriterReaderWithTop1()
      throws IOException {
    // Use VectorIndex reader to validate that reads work
    try (HnswVectorIndexReader reader = new HnswVectorIndexReader("foo", INDEX_DIR, 4, _config)) {
      int[] matchedDocIds = reader.getDocIds(new float[]{1.0F, 2.0F, 3.0F, 4.0F, 5.0F}, 1).toArray();
      // Expect to get 1 matching docId since topK = 1 is used
      Assert.assertEquals(matchedDocIds.length, 1);
      Assert.assertEquals(matchedDocIds[0], 2);
    }
  }

  @Test
  public void testFilteredReaderReturnsKAllowedDocuments()
      throws IOException {
    MutableRoaringBitmap allowedDocIds = new MutableRoaringBitmap();
    allowedDocIds.add(1);
    allowedDocIds.add(3);
    try (HnswVectorIndexReader reader = new HnswVectorIndexReader("foo", INDEX_DIR, 4, _config)) {
      float[] queryVector = {5.0F, 42.0F, 54.33333F, 42.24F, 1001.045F};
      Assert.assertEquals(reader.getDocIds(queryVector, 1).toArray(), new int[]{0},
          "The nearest physical document should be outside the allowed set");
      int[] matchedDocIds = reader.getDocIds(queryVector, 2, allowedDocIds).toArray();
      Assert.assertEquals(matchedDocIds, new int[]{1, 3});
    }
  }

  /// A null bitmap must not fall through to an unfiltered search, which would return doc ids outside the filter.
  /// The reader rejects it with its contract message rather than a bare NullPointerException from inside Lucene.
  @Test
  public void testFilteredReaderRejectsNullBitmap()
      throws IOException {
    try (HnswVectorIndexReader reader = new HnswVectorIndexReader("foo", INDEX_DIR, 4, _config)) {
      float[] queryVector = {5.0F, 42.0F, 54.33333F, 42.24F, 1001.045F};
      NullPointerException thrown = Assert.expectThrows(NullPointerException.class,
          () -> reader.getDocIds(queryVector, 2, (ImmutableRoaringBitmap) null));
      Assert.assertNotNull(thrown.getMessage(), "The rejection must carry the pre-filter contract message");
      Assert.assertTrue(thrown.getMessage().contains("must not be null"),
          "Expected the pre-filter contract message, got: " + thrown.getMessage());
    }
  }

  /// An empty filter admits nothing, so the search is skipped entirely. Asserting only that the result is empty
  /// would not discriminate -- Lucene returns no hits for a zero-match filter anyway. Disabling the bounded queue
  /// without an efSearch makes runtime-control validation throw when a query is actually built, so reaching an
  /// empty result here proves the short-circuit ran before query construction.
  @Test
  public void testFilteredReaderShortCircuitsEmptyBitmapBeforeBuildingQuery()
      throws IOException {
    try (HnswVectorIndexReader reader = new HnswVectorIndexReader("foo", INDEX_DIR, 4, _config)) {
      float[] queryVector = {5.0F, 42.0F, 54.33333F, 42.24F, 1001.045F};
      reader.setUseBoundedQueue(false);
      Assert.assertTrue(reader.getDocIds(queryVector, 2, new MutableRoaringBitmap()).isEmpty(),
          "An empty filter must short-circuit before query construction");
    }
  }

  @Test
  public void testEfSearchChangesRuntimeSearchBehavior()
      throws IOException {
    try (HnswVectorIndexReader reader = new HnswVectorIndexReader("foo", INDEX_DIR, 4, _config)) {
      reader.setEfSearch(1);
      int[] matchedDocIds = reader.getDocIds(new float[]{5.0F, 42.0F, 54.33333F, 42.24F, 3413.4F}, 3).toArray();
      Assert.assertEquals(matchedDocIds.length, 1,
          "efSearch=1 should cap the Lucene HNSW visit budget and reduce the returned result set");
      Assert.assertEquals(reader.getIndexDebugInfo().get("effectiveEfSearch"), 1);
    }
  }

  @Test
  public void testDisableBoundedQueueRequiresEfSearch()
      throws IOException {
    try (HnswVectorIndexReader reader = new HnswVectorIndexReader("foo", INDEX_DIR, 4, _config)) {
      reader.setUseBoundedQueue(false);
      IllegalArgumentException error = Assert.expectThrows(IllegalArgumentException.class,
          () -> reader.getDocIds(new float[]{1.0F, 2.0F, 3.0F, 4.0F, 5.0F}, 2));
      Assert.assertTrue(error.getMessage().contains("vectorEfSearch"));
    }
  }

  @Test
  public void testRuntimeControlDebugInfoReflectsOverrides()
      throws IOException {
    try (HnswVectorIndexReader reader = new HnswVectorIndexReader("foo", INDEX_DIR, 4, _config)) {
      reader.setEfSearch(6);
      reader.setUseRelativeDistance(false);
      reader.setUseBoundedQueue(false);

      Map<String, Object> debugInfo = reader.getIndexDebugInfo();
      Assert.assertEquals(debugInfo.get("effectiveEfSearch"), 6);
      Assert.assertEquals(debugInfo.get("effectiveHnswUseRelativeDistance"), Boolean.FALSE);
      Assert.assertEquals(debugInfo.get("effectiveHnswUseBoundedQueue"), Boolean.FALSE);
      Assert.assertEquals(debugInfo.get("supportsPreFilter"), reader.supportsPreFilter(),
          "Debug info must report the reader's actual pre-filter capability, not a hardcoded literal");
    }
  }

  /// A COSINE index returns the cosine distances of the documents each search returns, the same documents as
  /// `getDocIds`, and keeps nothing between searches; other distance functions return none.
  @Test
  public void testServesCosineDistances()
      throws IOException {
    File indexDir = new File(INDEX_DIR, "cosine");
    FileUtils.forceMkdir(indexDir);
    float[][] vectors = new float[200][8];
    Random random = new Random(7);
    for (float[] vector : vectors) {
      for (int d = 0; d < vector.length; d++) {
        vector[d] = random.nextFloat() - 0.5f;
      }
    }
    for (String distanceFunction : new String[]{"COSINE", "EUCLIDEAN"}) {
      Map<String, String> properties = new HashMap<>();
      properties.put("vectorIndexType", "HNSW");
      properties.put("vectorDimension", "8");
      properties.put("vectorDistanceFunction", distanceFunction);
      VectorIndexConfig config = new VectorIndexConfig(properties);
      FileUtils.cleanDirectory(indexDir);
      try (HnswVectorIndexCreator creator = new HnswVectorIndexCreator("baz", indexDir, config)) {
        for (float[] vector : vectors) {
          creator.add(vector);
        }
        creator.seal();
      }
      try (HnswVectorIndexReader reader = new HnswVectorIndexReader("baz", indexDir, vectors.length, config)) {
        float[] query = vectors[3];
        MutableRoaringBitmap allowed = new MutableRoaringBitmap();
        allowed.add(10L, 60L);
        if (distanceFunction.equals("EUCLIDEAN")) {
          Assert.assertNull(reader.getDocDistances(query, 5));
          Assert.assertNull(reader.getDocDistances(query, 5, allowed));
          continue;
        }
        Int2FloatMap distances = reader.getDocDistances(query, 20);
        Assert.assertEquals(sortedDocIds(distances), reader.getDocIds(query, 20).toArray());
        assertCosineDistances(distances, vectors, query);

        Int2FloatMap filtered = reader.getDocDistances(query, 5, allowed);
        Assert.assertEquals(sortedDocIds(filtered), reader.getDocIds(query, 5, allowed).toArray());
        for (int docId : filtered.keySet()) {
          Assert.assertTrue(allowed.contains(docId), "Filtered search returned doc outside the filter: " + docId);
        }
        assertCosineDistances(filtered, vectors, query);
        Assert.assertTrue(reader.getDocDistances(query, 5, new MutableRoaringBitmap()).isEmpty());

        // Stateless: a later search does not see the documents of an earlier one.
        Assert.assertEquals(reader.getDocDistances(query, 3).size(), 3);
      }
    }
  }

  /// The metric persisted in the index decides whether distances are served: a table config changed to another
  /// function without rebuilding the index must not convert the old scores as cosine scores.
  @Test
  public void testServesDistancesByThePersistedMetric()
      throws IOException {
    File indexDir = new File(INDEX_DIR, "metric");
    FileUtils.forceMkdir(indexDir);
    float[][] vectors = {{2, 0}, {0, 1}, {1, 1}, {3, 4}};
    float[] query = {1, 0};
    for (String[] builtAndLoaded : new String[][]{{"EUCLIDEAN", "COSINE"}, {"COSINE", "EUCLIDEAN"}}) {
      FileUtils.cleanDirectory(indexDir);
      try (HnswVectorIndexCreator creator =
          new HnswVectorIndexCreator("baz", indexDir, hnswConfig(builtAndLoaded[0]))) {
        for (float[] vector : vectors) {
          creator.add(vector);
        }
        creator.seal();
      }
      try (HnswVectorIndexReader reader =
          new HnswVectorIndexReader("baz", indexDir, vectors.length, hnswConfig(builtAndLoaded[1]))) {
        if (builtAndLoaded[0].equals("EUCLIDEAN")) {
          Assert.assertNull(reader.getDocDistances(query, 4));
          Assert.assertNull(reader.getDocDistances(query, 4, MutableRoaringBitmap.bitmapOf(0, 1)));
        } else {
          assertCosineDistances(reader.getDocDistances(query, 4), vectors, query);
        }
      }
    }
  }

  private static VectorIndexConfig hnswConfig(String distanceFunction) {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "2");
    properties.put("vectorDistanceFunction", distanceFunction);
    return new VectorIndexConfig(properties);
  }

  private static int[] sortedDocIds(Int2FloatMap distances) {
    int[] docIds = distances.keySet().toIntArray();
    Arrays.sort(docIds);
    return docIds;
  }

  private static void assertCosineDistances(Int2FloatMap distances, float[][] vectors, float[] query) {
    for (Int2FloatMap.Entry entry : distances.int2FloatEntrySet()) {
      Assert.assertEquals(entry.getFloatValue(), VectorFunctions.cosineDistance(vectors[entry.getIntKey()], query),
          1e-5);
    }
  }
}
