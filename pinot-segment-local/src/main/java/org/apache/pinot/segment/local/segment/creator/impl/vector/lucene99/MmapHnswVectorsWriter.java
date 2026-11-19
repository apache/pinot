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
package org.apache.pinot.segment.local.segment.creator.impl.vector.lucene99;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraph.NodesIterator;
import org.apache.lucene.util.packed.DirectMonotonicWriter;


/// Writes a final merge in the Lucene99 HNSW format using temporary mmap storage for graph construction.
/// Each instance belongs to one Lucene merge thread. Vector values are written by Lucene's flat vector writer.
final class MmapHnswVectorsWriter extends KnnVectorsWriter {
  // Match Lucene 9.12's Lucene99HnswVectorsFormat (version 0), which remains the reader.
  private static final String META_CODEC_NAME = "Lucene99HnswVectorsFormatMeta";
  private static final String INDEX_CODEC_NAME = "Lucene99HnswVectorsFormatIndex";
  private static final int FORMAT_VERSION = 0;
  private static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;
  private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(MmapHnswVectorsWriter.class);

  private final Path _scratchDirectory;
  private final int _maxConn;
  private final int _beamWidth;
  private final FlatVectorsWriter _flatWriter;
  private final IndexOutput _meta;
  private final IndexOutput _index;

  MmapHnswVectorsWriter(SegmentWriteState state, Path scratchDirectory, int maxConn, int beamWidth)
      throws IOException {
    _scratchDirectory = scratchDirectory;
    _maxConn = maxConn;
    _beamWidth = beamWidth;
    FlatVectorsWriter flatWriter = new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer())
        .fieldsWriter(state);
    IndexOutput meta = null;
    IndexOutput index = null;
    boolean success = false;
    try {
      meta = state.directory.createOutput(
          IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, "vem"), state.context);
      index = state.directory.createOutput(
          IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, "vex"), state.context);
      byte[] segmentId = state.segmentInfo.getId();
      CodecUtil.writeIndexHeader(meta, META_CODEC_NAME, FORMAT_VERSION, segmentId, state.segmentSuffix);
      CodecUtil.writeIndexHeader(index, INDEX_CODEC_NAME, FORMAT_VERSION, segmentId, state.segmentSuffix);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(meta, index, flatWriter);
      }
    }
    _flatWriter = flatWriter;
    _meta = meta;
    _index = index;
  }

  @Override
  public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) {
    throw new UnsupportedOperationException("The mmap HNSW writer is only used for final merges");
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) {
    throw new UnsupportedOperationException("The mmap HNSW writer is only used for final merges");
  }

  @Override
  public void mergeOneField(FieldInfo field, MergeState mergeState)
      throws IOException {
    try (CloseableRandomVectorScorerSupplier scorers = _flatWriter.mergeOneFieldToIndex(field, mergeState)) {
      int count = scorers.totalVectorCount();
      long offset = _index.getFilePointer();
      if (count == 0) {
        writeFieldMeta(field, offset, 0, 0);
        _meta.writeVInt(0);
        return;
      }
      try (MmapHnswGraph graph =
          MmapHnswGraphBuilder.build(_scratchDirectory, field, scorers, mergeState, _maxConn, _beamWidth)) {
        if (graph.size() != count) {
          throw new IOException("Expected " + count + " HNSW nodes for " + field.name + ", got " + graph.size());
        }
        writeGraph(field, offset, graph);
      }
    }
  }

  private void writeGraph(FieldInfo field, long offset, HnswGraph graph)
      throws IOException {
    int count = graph.size();
    int levels = graph.numLevels();
    int[][] nodesByLevel = new int[levels][];
    int[][] lengthsByLevel = new int[levels][];
    int[] neighbors = new int[2 * _maxConn];
    long totalNodes = 0;
    for (int level = 0; level < levels; level++) {
      // Level zero includes all vector ordinals. Higher levels are stored in ascending ordinal order.
      int[] nodes = level == 0 ? null : NodesIterator.getSortedNodes(graph.getNodesOnLevel(level));
      nodesByLevel[level] = nodes;
      int nodesOnLevel = nodes == null ? count : nodes.length;
      int[] lengths = new int[nodesOnLevel];
      lengthsByLevel[level] = lengths;
      totalNodes += nodesOnLevel;
      for (int i = 0; i < nodesOnLevel; i++) {
        long start = _index.getFilePointer();
        int node = nodes == null ? i : nodes[i];
        graph.seek(level, node);
        int numNeighbors = 0;
        for (int neighbor = graph.nextNeighbor(); neighbor != DocIdSetIterator.NO_MORE_DOCS;
            neighbor = graph.nextNeighbor()) {
          neighbors[numNeighbors++] = neighbor;
        }
        _index.writeVInt(numNeighbors);
        Arrays.sort(neighbors, 0, numNeighbors);
        writeDeltaEncoded(_index, neighbors, numNeighbors);
        lengths[i] = Math.toIntExact(_index.getFilePointer() - start);
      }
    }

    writeFieldMeta(field, offset, _index.getFilePointer() - offset, count);
    _meta.writeVInt(levels);
    for (int level = 1; level < levels; level++) {
      int[] nodes = nodesByLevel[level];
      _meta.writeVInt(nodes.length);
      writeDeltaEncoded(_meta, nodes, nodes.length);
    }
    writeNodeOffsets(totalNodes, lengthsByLevel);
  }

  private static void writeDeltaEncoded(IndexOutput output, int[] values, int count)
      throws IOException {
    int previous = 0;
    for (int i = 0; i < count; i++) {
      int value = values[i];
      output.writeVInt(value - previous);
      previous = value;
    }
  }

  private void writeFieldMeta(FieldInfo field, long offset, long length, int count)
      throws IOException {
    _meta.writeInt(field.number);
    _meta.writeInt(field.getVectorEncoding().ordinal());
    int similarityOrdinal = Lucene99HnswVectorsReader.SIMILARITY_FUNCTIONS
        .indexOf(field.getVectorSimilarityFunction());
    if (similarityOrdinal < 0) {
      throw new IllegalArgumentException("Invalid vector similarity function: " + field.getVectorSimilarityFunction());
    }
    _meta.writeInt(similarityOrdinal);
    _meta.writeVLong(offset);
    _meta.writeVLong(length);
    _meta.writeVInt(field.getVectorDimension());
    _meta.writeInt(count);
    _meta.writeVInt(_maxConn);
  }

  private void writeNodeOffsets(long totalNodes, int[][] lengthsByLevel)
      throws IOException {
    long start = _index.getFilePointer();
    _meta.writeLong(start);
    _meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);
    DirectMonotonicWriter offsets =
        DirectMonotonicWriter.getInstance(_meta, _index, totalNodes, DIRECT_MONOTONIC_BLOCK_SHIFT);
    long offset = 0;
    for (int[] lengths : lengthsByLevel) {
      for (int length : lengths) {
        offsets.add(offset);
        offset += length;
      }
    }
    offsets.finish();
    _meta.writeLong(_index.getFilePointer() - start);
  }

  @Override
  public void finish()
      throws IOException {
    _flatWriter.finish();
    _meta.writeInt(-1);
    CodecUtil.writeFooter(_meta);
    CodecUtil.writeFooter(_index);
  }

  @Override
  public long ramBytesUsed() {
    return SHALLOW_SIZE + _flatWriter.ramBytesUsed();
  }

  @Override
  public void close()
      throws IOException {
    IOUtils.close(_meta, _index, _flatWriter);
  }
}
