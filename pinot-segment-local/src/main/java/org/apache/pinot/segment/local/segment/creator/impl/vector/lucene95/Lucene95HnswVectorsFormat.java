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
package org.apache.pinot.segment.local.segment.creator.impl.vector.lucene95;

import java.io.IOException;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.hnsw.HnswGraph;


/**
 * Lucene 9.5 vector format, which encodes numeric vector values and an optional associated graph
 * connecting the documents having values. The graph is used to power HNSW search. The format
 * consists of three files:
 *
 * <h2>.vec (vector data) file</h2>
 *
 * <p>For each field:
 *
 * <ul>
 *   <li>Vector data ordered by field, document ordinal, and vector dimension. When the
 *       vectorEncoding is BYTE, each sample is stored as a single byte. When it is FLOAT32, each
 *       sample is stored as an IEEE float in little-endian byte order.
 *   <li>DocIds encoded by {@link IndexedDISI#writeBitSet(DocIdSetIterator, IndexOutput, byte)},
 *       note that only in sparse case
 *   <li>OrdToDoc was encoded by {@link org.apache.lucene.util.packed.DirectMonotonicWriter}, note
 *       that only in sparse case
 * </ul>
 *
 * <h2>.vex (vector index)</h2>
 *
 * <p>Stores graphs connecting the documents for each field organized as a list of nodes' neighbours
 * as following:
 *
 * <ul>
 *   <li>For each level:
 *       <ul>
 *         <li>For each node:
 *             <ul>
 *               <li><b>[vint]</b> the number of neighbor nodes
 *               <li><b>array[vint]</b> the delta encoded neighbor ordinals
 *             </ul>
 *       </ul>
 *   <li>After all levels are encoded memory offsets for each node's neighbor nodes encoded by
 *       {@link org.apache.lucene.util.packed.DirectMonotonicWriter} are appened to the end of the
 *       file.
 * </ul>
 *
 * <h2>.vem (vector metadata) file</h2>
 *
 * <p>For each field:
 *
 * <ul>
 *   <li><b>[int32]</b> field number
 *   <li><b>[int32]</b> vector similarity function ordinal
 *   <li><b>[vlong]</b> offset to this field's vectors in the .vec file
 *   <li><b>[vlong]</b> length of this field's vectors, in bytes
 *   <li><b>[vlong]</b> offset to this field's index in the .vex file
 *   <li><b>[vlong]</b> length of this field's index data, in bytes
 *   <li><b>[vint]</b> dimension of this field's vectors
 *   <li><b>[int]</b> the number of documents having values for this field
 *   <li><b>[int8]</b> if equals to -1, dense – all documents have values for a field. If equals to
 *       0, sparse – some documents missing values.
 *   <li>DocIds were encoded by {@link IndexedDISI#writeBitSet(DocIdSetIterator, IndexOutput, byte)}
 *   <li>OrdToDoc was encoded by {@link org.apache.lucene.util.packed.DirectMonotonicWriter}, note
 *       that only in sparse case
 *   <li><b>[vint]</b> the maximum number of connections (neigbours) that each node can have
 *   <li><b>[vint]</b> number of levels in the graph
 *   <li>Graph nodes by level. For each level
 *       <ul>
 *         <li><b>[vint]</b> the number of nodes on this level
 *         <li><b>array[vint]</b> for levels greater than 0 list of nodes on this level, stored as
 *             the level 0th delta encoded nodes' ordinals.
 *       </ul>
 * </ul>
 *
 * @lucene.experimental
 */
public final class Lucene95HnswVectorsFormat extends KnnVectorsFormat {

  static final String META_CODEC_NAME = "Lucene95HnswVectorsFormatMeta";
  static final String VECTOR_DATA_CODEC_NAME = "Lucene95HnswVectorsFormatData";
  static final String VECTOR_INDEX_CODEC_NAME = "Lucene95HnswVectorsFormatIndex";
  static final String META_EXTENSION = "vem";
  static final String VECTOR_DATA_EXTENSION = "vec";
  static final String VECTOR_INDEX_EXTENSION = "vex";

  public static final int VERSION_START = 0;
  public static final int VERSION_CURRENT = VERSION_START;

  /**
   * A maximum configurable maximum max conn.
   *
   * <p>NOTE: We eagerly populate `float[MAX_CONN*2]` and `int[MAX_CONN*2]`, so exceptionally large
   * numbers here will use an inordinate amount of heap
   */
  private static final int MAXIMUM_MAX_CONN = 512;
  /** Default number of maximum connections per node */
  public static final int DEFAULT_MAX_CONN = 16;

  /**
   * The maximum size of the queue to maintain while searching during graph construction This
   * maximum value preserves the ratio of the DEFAULT_BEAM_WIDTH/DEFAULT_MAX_CONN i.e. `6.25 * 16 =
   * 3200`
   */
  private static final int MAXIMUM_BEAM_WIDTH = 3200;

  /**
   * The maximum number of dimensions supported by this format. This is a limitation of the
   * underlying implementation of {@link HnswGraph} which uses a fixed size array to store the
   * vector values.
   */
  public static final int DEFAULT_MAX_DIMENSIONS = 2048;

  /**
   * Default number of the size of the queue maintained while searching during a graph construction.
   */
  public static final int DEFAULT_BEAM_WIDTH = 100;

  static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

  /**
   * Controls how many of the nearest neighbor candidates are connected to the new node. Defaults to
   * {@link Lucene95HnswVectorsFormat#DEFAULT_MAX_CONN}. See {@link HnswGraph} for more details.
   */
  private final int _maxConn;

  /**
   * The number of candidate neighbors to track while searching the graph for each newly inserted
   * node. Defaults to to {@link Lucene95HnswVectorsFormat#DEFAULT_BEAM_WIDTH}. See {@link
   * HnswGraph} for details.
   */
  private final int _beamWidth;

  private final int _maxDimensions;

  /** Constructs a format using default graph construction parameters */
  public Lucene95HnswVectorsFormat() {
    this(DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH, DEFAULT_MAX_DIMENSIONS);
  }

  /**
   * Constructs a format using the given graph construction parameters.
   *
   * @param maxConn the maximum number of connections to a node in the HNSW graph
   * @param beamWidth the size of the queue maintained during graph construction.
   */
  public Lucene95HnswVectorsFormat(int maxConn, int beamWidth, int maxDimensions) {
    super("Lucene95HnswVectorsFormat");
    if (maxConn <= 0 || maxConn > MAXIMUM_MAX_CONN) {
      throw new IllegalArgumentException(
          "maxConn must be postive and less than or equal to"
              + MAXIMUM_MAX_CONN
              + "; maxConn="
              + maxConn);
    }
    if (beamWidth <= 0 || beamWidth > MAXIMUM_BEAM_WIDTH) {
      throw new IllegalArgumentException(
          "beamWidth must be postive and less than or equal to"
              + MAXIMUM_BEAM_WIDTH
              + "; beamWidth="
              + beamWidth);
    }
    if (maxDimensions <= 0 || maxDimensions > DEFAULT_MAX_DIMENSIONS) {
      throw new IllegalArgumentException(
          "maxDimensions must be postive and less than or equal to"
              + DEFAULT_MAX_DIMENSIONS
              + "; maxDimensions="
              + maxDimensions);
    }
    _maxDimensions = maxDimensions;
    _maxConn = maxConn;
    _beamWidth = beamWidth;
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state)
      throws IOException {
    return new Lucene95HnswVectorsWriter(state, _maxConn, _beamWidth);
  }

  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state)
      throws IOException {
    return new Lucene95HnswVectorsReader(state);
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    return _maxDimensions;
  }

  @Override
  public String toString() {
    return "Lucene95HnswVectorsFormat(name=Lucene95HnswVectorsFormat, maxConn="
        + _maxConn
        + ", beamWidth="
        + _beamWidth
        + ")";
  }
}
