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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.OrdinalTranslatedKnnCollector;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.packed.DirectMonotonicReader;


/**
 * Reads vectors from the index segments along with index data structures supporting KNN search.
 *
 * @lucene.experimental
 */
public final class Lucene95HnswVectorsReader extends KnnVectorsReader {

  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(Lucene95HnswVectorsFormat.class);

  private final FieldInfos _fieldInfos;
  private final Map<String, FieldEntry> _fields = new HashMap<>();
  private final IndexInput _vectorData;
  private final IndexInput _vectorIndex;

  Lucene95HnswVectorsReader(SegmentReadState state)
      throws IOException {
    _fieldInfos = state.fieldInfos;
    int versionMeta = readMetadata(state);
    boolean success = false;
    try {
      _vectorData =
          openDataInput(
              state,
              versionMeta,
              Lucene95HnswVectorsFormat.VECTOR_DATA_EXTENSION,
              Lucene95HnswVectorsFormat.VECTOR_DATA_CODEC_NAME);
      _vectorIndex =
          openDataInput(
              state,
              versionMeta,
              Lucene95HnswVectorsFormat.VECTOR_INDEX_EXTENSION,
              Lucene95HnswVectorsFormat.VECTOR_INDEX_CODEC_NAME);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  private int readMetadata(SegmentReadState state)
      throws IOException {
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, Lucene95HnswVectorsFormat.META_EXTENSION);
    int versionMeta = -1;
    try (ChecksumIndexInput meta =
        state.directory.openChecksumInput(metaFileName, IOContext.READONCE)) {
      Throwable priorE = null;
      try {
        versionMeta =
            CodecUtil.checkIndexHeader(
                meta,
                Lucene95HnswVectorsFormat.META_CODEC_NAME,
                Lucene95HnswVectorsFormat.VERSION_START,
                Lucene95HnswVectorsFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix);
        readFields(meta, state.fieldInfos);
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(meta, priorE);
      }
    }
    return versionMeta;
  }

  private static IndexInput openDataInput(
      SegmentReadState state, int versionMeta, String fileExtension, String codecName)
      throws IOException {
    String fileName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, fileExtension);
    IndexInput in = state.directory.openInput(fileName, state.context);
    boolean success = false;
    try {
      int versionVectorData =
          CodecUtil.checkIndexHeader(
              in,
              codecName,
              Lucene95HnswVectorsFormat.VERSION_START,
              Lucene95HnswVectorsFormat.VERSION_CURRENT,
              state.segmentInfo.getId(),
              state.segmentSuffix);
      if (versionMeta != versionVectorData) {
        throw new CorruptIndexException(
            "Format versions mismatch: meta="
                + versionMeta
                + ", "
                + codecName
                + "="
                + versionVectorData,
            in);
      }
      CodecUtil.retrieveChecksum(in);
      success = true;
      return in;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(in);
      }
    }
  }

  private void readFields(ChecksumIndexInput meta, FieldInfos infos)
      throws IOException {
    for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
      FieldInfo info = infos.fieldInfo(fieldNumber);
      if (info == null) {
        throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
      }
      FieldEntry fieldEntry = readField(meta);
      validateFieldEntry(info, fieldEntry);
      _fields.put(info.name, fieldEntry);
    }
  }

  private void validateFieldEntry(FieldInfo info, FieldEntry fieldEntry) {
    int dimension = info.getVectorDimension();
    if (dimension != fieldEntry._dimension) {
      throw new IllegalStateException(
          "Inconsistent vector dimension for field=\""
              + info.name
              + "\"; "
              + dimension
              + " != "
              + fieldEntry._dimension);
    }

    int byteSize;
    switch (info.getVectorEncoding()) {
      case BYTE:
        byteSize = Byte.BYTES;
        break;
      case FLOAT32:
        byteSize = Float.BYTES;
        break;
      default:
        throw new AssertionError();
    }
    long vectorBytes = Math.multiplyExact((long) dimension, byteSize);
    long numBytes = Math.multiplyExact(vectorBytes, fieldEntry._size);
    if (numBytes != fieldEntry._vectorDataLength) {
      throw new IllegalStateException(
          "Vector data length "
              + fieldEntry._vectorDataLength
              + " not matching size="
              + fieldEntry._size
              + " * dim="
              + dimension
              + " * byteSize="
              + byteSize
              + " = "
              + numBytes);
    }
  }

  private VectorSimilarityFunction readSimilarityFunction(DataInput input)
      throws IOException {
    int similarityFunctionId = input.readInt();
    if (similarityFunctionId < 0
        || similarityFunctionId >= VectorSimilarityFunction.values().length) {
      throw new CorruptIndexException(
          "Invalid similarity function id: " + similarityFunctionId, input);
    }
    return VectorSimilarityFunction.values()[similarityFunctionId];
  }

  private VectorEncoding readVectorEncoding(DataInput input)
      throws IOException {
    int encodingId = input.readInt();
    if (encodingId < 0 || encodingId >= VectorEncoding.values().length) {
      throw new CorruptIndexException("Invalid vector encoding id: " + encodingId, input);
    }
    return VectorEncoding.values()[encodingId];
  }

  private FieldEntry readField(IndexInput input)
      throws IOException {
    VectorEncoding vectorEncoding = readVectorEncoding(input);
    VectorSimilarityFunction similarityFunction = readSimilarityFunction(input);
    return new FieldEntry(input, vectorEncoding, similarityFunction);
  }

  @Override
  public long ramBytesUsed() {
    return Lucene95HnswVectorsReader.SHALLOW_SIZE
        + RamUsageEstimator.sizeOfMap(
        _fields, RamUsageEstimator.shallowSizeOfInstance(FieldEntry.class));
  }

  @Override
  public void checkIntegrity()
      throws IOException {
    CodecUtil.checksumEntireFile(_vectorData);
    CodecUtil.checksumEntireFile(_vectorIndex);
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field)
      throws IOException {
    FieldEntry fieldEntry = _fields.get(field);
    if (fieldEntry._vectorEncoding != VectorEncoding.FLOAT32) {
      throw new IllegalArgumentException(
          "field=\""
              + field
              + "\" is encoded as: "
              + fieldEntry._vectorEncoding
              + " expected: "
              + VectorEncoding.FLOAT32);
    }
    return OffHeapFloatVectorValues.load(fieldEntry, _vectorData);
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field)
      throws IOException {
    FieldEntry fieldEntry = _fields.get(field);
    if (fieldEntry._vectorEncoding != VectorEncoding.BYTE) {
      throw new IllegalArgumentException(
          "field=\""
              + field
              + "\" is encoded as: "
              + fieldEntry._vectorEncoding
              + " expected: "
              + VectorEncoding.FLOAT32);
    }
    return OffHeapByteVectorValues.load(fieldEntry, _vectorData);
  }

  @Override
  public void search(String field, float[] target, KnnCollector knnCollector, Bits acceptDocs)
      throws IOException {
    FieldEntry fieldEntry = _fields.get(field);

    if (fieldEntry.size() == 0
        || knnCollector.k() == 0
        || fieldEntry._vectorEncoding != VectorEncoding.FLOAT32) {
      return;
    }

    OffHeapFloatVectorValues
        vectorValues = OffHeapFloatVectorValues.load(fieldEntry, _vectorData);
    RandomVectorScorer scorer =
        RandomVectorScorer.createFloats(vectorValues, fieldEntry._similarityFunction, target);
    HnswGraphSearcher.search(
        scorer,
        new OrdinalTranslatedKnnCollector(knnCollector, vectorValues::ordToDoc),
        getGraph(fieldEntry),
        vectorValues.getAcceptOrds(acceptDocs));
  }

  @Override
  public void search(String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs)
      throws IOException {
    FieldEntry fieldEntry = _fields.get(field);

    if (fieldEntry.size() == 0
        || knnCollector.k() == 0
        || fieldEntry._vectorEncoding != VectorEncoding.BYTE) {
      return;
    }

    OffHeapByteVectorValues
        vectorValues = OffHeapByteVectorValues.load(fieldEntry, _vectorData);
    RandomVectorScorer scorer =
        RandomVectorScorer.createBytes(vectorValues, fieldEntry._similarityFunction, target);
    HnswGraphSearcher.search(
        scorer,
        new OrdinalTranslatedKnnCollector(knnCollector, vectorValues::ordToDoc),
        getGraph(fieldEntry),
        vectorValues.getAcceptOrds(acceptDocs));
  }

  /**
   * Get knn graph values; used for testing
   */
  public HnswGraph getGraph(String field)
      throws IOException {
    FieldInfo info = _fieldInfos.fieldInfo(field);
    if (info == null) {
      throw new IllegalArgumentException("No such field '" + field + "'");
    }
    FieldEntry entry = _fields.get(field);
    if (entry != null && entry._vectorIndexLength > 0) {
      return getGraph(entry);
    } else {
      return HnswGraph.EMPTY;
    }
  }

  private HnswGraph getGraph(FieldEntry entry)
      throws IOException {
    return new OffHeapHnswGraph(entry, _vectorIndex);
  }

  @Override
  public void close()
      throws IOException {
    IOUtils.close(_vectorData, _vectorIndex);
  }

  static class FieldEntry implements Accountable {
    private static final long SHALLOW_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(FieldEntry.class);
    final VectorSimilarityFunction _similarityFunction;
    final VectorEncoding _vectorEncoding;
    final long _vectorDataOffset;
    final long _vectorDataLength;
    final long _vectorIndexOffset;
    final long _vectorIndexLength;
    final int _m;
    final int _numLevels;
    final int _dimension;
    final int _size;
    final int[][] _nodesByLevel;
    // for each level the start offsets in vectorIndex file from where to read neighbours
    final DirectMonotonicReader.Meta _offsetsMeta;
    final long _offsetsOffset;
    final int _offsetsBlockShift;
    final long _offsetsLength;

    // the following four variables used to read docIds encoded by IndexDISI
    // special values of docsWithFieldOffset are -1 and -2
    // -1 : dense
    // -2 : empty
    // other: sparse
    final long _docsWithFieldOffset;
    final long _docsWithFieldLength;
    final short _jumpTableEntryCount;
    final byte _denseRankPower;

    // the following four variables used to read ordToDoc encoded by DirectMonotonicWriter
    // note that only spare case needs to store ordToDoc
    final long _addressesOffset;
    final int _blockShift;
    final DirectMonotonicReader.Meta _meta;
    final long _addressesLength;

    FieldEntry(
        IndexInput input,
        VectorEncoding vectorEncoding,
        VectorSimilarityFunction similarityFunction)
        throws IOException {
      _similarityFunction = similarityFunction;
      _vectorEncoding = vectorEncoding;
      _vectorDataOffset = input.readVLong();
      _vectorDataLength = input.readVLong();
      _vectorIndexOffset = input.readVLong();
      _vectorIndexLength = input.readVLong();
      _dimension = input.readVInt();
      _size = input.readInt();

      _docsWithFieldOffset = input.readLong();
      _docsWithFieldLength = input.readLong();
      _jumpTableEntryCount = input.readShort();
      _denseRankPower = input.readByte();

      // dense or empty
      if (_docsWithFieldOffset == -1 || _docsWithFieldOffset == -2) {
        _addressesOffset = 0;
        _blockShift = 0;
        _meta = null;
        _addressesLength = 0;
      } else {
        // sparse
        _addressesOffset = input.readLong();
        _blockShift = input.readVInt();
        _meta = DirectMonotonicReader.loadMeta(input, _size, _blockShift);
        _addressesLength = input.readLong();
      }

      // read nodes by level
      _m = input.readVInt();
      _numLevels = input.readVInt();
      _nodesByLevel = new int[_numLevels][];
      long numberOfOffsets = 0;
      for (int level = 0; level < _numLevels; level++) {
        if (level > 0) {
          int numNodesOnLevel = input.readVInt();
          numberOfOffsets += numNodesOnLevel;
          _nodesByLevel[level] = new int[numNodesOnLevel];
          _nodesByLevel[level][0] = input.readVInt();
          for (int i = 1; i < numNodesOnLevel; i++) {
            _nodesByLevel[level][i] = _nodesByLevel[level][i - 1] + input.readVInt();
          }
        } else {
          numberOfOffsets += _size;
        }
      }
      if (numberOfOffsets > 0) {
        _offsetsOffset = input.readLong();
        _offsetsBlockShift = input.readVInt();
        _offsetsMeta = DirectMonotonicReader.loadMeta(input, numberOfOffsets, _offsetsBlockShift);
        _offsetsLength = input.readLong();
      } else {
        _offsetsOffset = 0;
        _offsetsBlockShift = 0;
        _offsetsMeta = null;
        _offsetsLength = 0;
      }
    }

    int size() {
      return _size;
    }

    @Override
    public long ramBytesUsed() {
      return SHALLOW_SIZE
          + Arrays.stream(_nodesByLevel).mapToLong(nodes -> RamUsageEstimator.sizeOf(nodes)).sum()
          + RamUsageEstimator.sizeOf(_meta)
          + RamUsageEstimator.sizeOf(_offsetsMeta);
    }
  }

  /**
   * Read the nearest-neighbors graph from the index input
   */
  private static final class OffHeapHnswGraph extends HnswGraph {

    final IndexInput _dataIn;
    final int[][] _nodesByLevel;
    final int _numLevels;
    final int _entryNode;
    final int _size;
    int _arcCount;
    int _arcUpTo;
    int _arc;
    private final DirectMonotonicReader _graphLevelNodeOffsets;
    private final long[] _graphLevelNodeIndexOffsets;
    // Allocated to be M*2 to track the current neighbors being explored
    private int[] _currentNeighborsBuffer;

    OffHeapHnswGraph(FieldEntry entry, IndexInput vectorIndex)
        throws IOException {
      _dataIn =
          vectorIndex.slice("graph-data", entry._vectorIndexOffset, entry._vectorIndexLength);
      _nodesByLevel = entry._nodesByLevel;
      _numLevels = entry._numLevels;
      _entryNode = _numLevels > 1 ? _nodesByLevel[_numLevels - 1][0] : 0;
      _size = entry.size();
      final RandomAccessInput addressesData =
          vectorIndex.randomAccessSlice(entry._offsetsOffset, entry._offsetsLength);
      _graphLevelNodeOffsets =
          DirectMonotonicReader.getInstance(entry._offsetsMeta, addressesData);
      _currentNeighborsBuffer = new int[entry._m * 2];
      _graphLevelNodeIndexOffsets = new long[_numLevels];
      _graphLevelNodeIndexOffsets[0] = 0;
      for (int i = 1; i < _numLevels; i++) {
        // nodesByLevel is `null` for the zeroth level as we know its size
        int nodeCount = _nodesByLevel[i - 1] == null ? _size : _nodesByLevel[i - 1].length;
        _graphLevelNodeIndexOffsets[i] = _graphLevelNodeIndexOffsets[i - 1] + nodeCount;
      }
    }

    @Override
    public void seek(int level, int targetOrd)
        throws IOException {
      int targetIndex =
          level == 0
              ? targetOrd
              : Arrays.binarySearch(_nodesByLevel[level], 0, _nodesByLevel[level].length, targetOrd);
      assert targetIndex >= 0;
      // unsafe; no bounds checking
      _dataIn.seek(_graphLevelNodeOffsets.get(targetIndex + _graphLevelNodeIndexOffsets[level]));
      _arcCount = _dataIn.readVInt();
      if (_arcCount > 0) {
        if (_arcCount > _currentNeighborsBuffer.length) {
          _currentNeighborsBuffer = ArrayUtil.grow(_currentNeighborsBuffer, _arcCount);
        }
        _currentNeighborsBuffer[0] = _dataIn.readVInt();
        for (int i = 1; i < _arcCount; i++) {
          _currentNeighborsBuffer[i] = _currentNeighborsBuffer[i - 1] + _dataIn.readVInt();
        }
      }
      _arc = -1;
      _arcUpTo = 0;
    }

    @Override
    public int size() {
      return _size;
    }

    @Override
    public int nextNeighbor()
        throws IOException {
      if (_arcUpTo >= _arcCount) {
        return DocIdSetIterator.NO_MORE_DOCS;
      }
      _arc = _currentNeighborsBuffer[_arcUpTo];
      ++_arcUpTo;
      return _arc;
    }

    @Override
    public int numLevels()
        throws IOException {
      return _numLevels;
    }

    @Override
    public int entryNode()
        throws IOException {
      return _entryNode;
    }

    @Override
    public NodesIterator getNodesOnLevel(int level) {
      if (level == 0) {
        return new ArrayNodesIterator(size());
      } else {
        return new ArrayNodesIterator(_nodesByLevel[level], _nodesByLevel[level].length);
      }
    }
  }
}
