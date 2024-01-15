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
import org.apache.lucene.codecs.lucene95.Lucene95HnswVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.hnsw.HnswGraph;


/**
 * Extend Lucene 9.5 vector format to support HNSW graph
 * The major change here is to allow higher dimension vectors
 */
public final class HnswVectorsFormat extends KnnVectorsFormat {
  /**
   * The maximum number of dimensions supported by this format. This is a limitation of the
   * underlying implementation of {@link HnswGraph} which uses a fixed size array to store the
   * vector values.
   */
  public static final int DEFAULT_MAX_DIMENSIONS = 2048;

  private final int _maxDimensions;
  private final Lucene95HnswVectorsFormat _delegate;

  /**
   * Constructs a format using the given graph construction parameters.
   *
   * @param maxConn       the maximum number of connections to a node in the HNSW graph
   * @param beamWidth     the size of the queue maintained during graph construction.
   * @param maxDimensions the maximum number of dimensions supported by this format
   */
  public HnswVectorsFormat(int maxConn, int beamWidth, int maxDimensions) {
    super("Lucene95HnswVectorsFormat");
    if (maxDimensions <= 0 || maxDimensions > DEFAULT_MAX_DIMENSIONS) {
      throw new IllegalArgumentException(
          "maxDimensions must be postive and less than or equal to"
              + DEFAULT_MAX_DIMENSIONS
              + "; maxDimensions="
              + maxDimensions);
    }
    _delegate = new Lucene95HnswVectorsFormat(maxConn, beamWidth);
    _maxDimensions = maxDimensions;
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state)
      throws IOException {
    return _delegate.fieldsWriter(state);
  }

  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state)
      throws IOException {
    return _delegate.fieldsReader(state);
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    return _maxDimensions;
  }

  @Override
  public String toString() {
    return "HnswVectorsFormat(name=HnswVectorsFormat, maxDimensions="
        + _maxDimensions
        + ", delegate="
        + _delegate
        + ")";
  }
}
