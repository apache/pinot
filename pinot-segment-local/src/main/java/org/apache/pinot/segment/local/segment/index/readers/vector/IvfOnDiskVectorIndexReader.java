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
package org.apache.pinot.segment.local.segment.index.readers.vector;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.common.function.scalar.VectorFunctions;
import org.apache.pinot.segment.local.segment.index.vector.IvfFlatVectorIndexCreator;
import org.apache.pinot.segment.local.segment.index.vector.VectorQuantizationUtils;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.creator.VectorQuantizerType;
import org.apache.pinot.segment.spi.index.reader.ApproximateRadiusVectorIndexReader;
import org.apache.pinot.segment.spi.index.reader.FilterAwareVectorIndexReader;
import org.apache.pinot.segment.spi.index.reader.NprobeAware;
import org.apache.pinot.segment.spi.index.reader.VectorQuantizer;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Disk-backed reader for IVF_FLAT index files using FileChannel-based random-access I/O.
 *
 * <p>Unlike {@link IvfFlatVectorIndexReader} which loads all data into heap, this reader
 * keeps the index file open and performs positional reads via {@link FileChannel}.
 * Centroids are always loaded into heap for fast probe selection; inverted list data
 * (doc IDs and vectors) is read on demand from the file.</p>
 *
 * <p>This reader can read standard IVF_FLAT index files (magic 0x49564646). The on-disk
 * format is identical — only the runtime access pattern differs.</p>
 *
 * <h3>Thread safety</h3>
 * <p>Thread-safe for concurrent reads. Query-scoped nprobe overrides use ThreadLocal.
 * FileChannel positional reads are thread-safe; each read specifies an absolute offset.</p>
 */
public class IvfOnDiskVectorIndexReader
    implements FilterAwareVectorIndexReader, ApproximateRadiusVectorIndexReader, NprobeAware {
  private static final Logger LOGGER = LoggerFactory.getLogger(IvfOnDiskVectorIndexReader.class);

  static final int DEFAULT_NPROBE = 4;

  // Header fields (loaded into heap)
  private final int _dimension;
  private final int _numVectors;
  private final int _nlist;
  private final VectorIndexConfig.VectorDistanceFunction _distanceFunction;
  private final int _indexFormatVersion;
  private final VectorQuantizerType _quantizerType;
  private final VectorQuantizer _quantizer;
  private final int _encodedBytesPerVector;
  private final String _column;
  private final int _defaultNprobe;

  // Centroids in heap for fast probe selection
  private final float[][] _centroids;

  // FileChannel for random-access reads of inverted list data (avoids MappedByteBuffer 2GB limit)
  private final RandomAccessFile _raf;
  private final FileChannel _channel;

  // Offsets to each inverted list in the file
  private final long[] _listOffsets;
  // Cached list sizes (loaded at init to avoid repeated mmap reads for sizes)
  private final int[] _listSizes;

  // Observability: per-centroid access count
  private final AtomicLong[] _centroidAccessCounts;
  private final AtomicLong _totalSearches = new AtomicLong(0);
  private final AtomicLong _filteredSearches = new AtomicLong(0);
  private final AtomicLong _unfilteredSearches = new AtomicLong(0);

  private final ThreadLocal<Integer> _nprobeOverride = new ThreadLocal<>();

  /**
   * Opens an IVF_FLAT index file for FileChannel-based random-access reading.
   *
   * @param column   the column name
   * @param indexDir the segment index directory
   * @param config   the vector index configuration
   */
  public IvfOnDiskVectorIndexReader(String column, File indexDir, VectorIndexConfig config) {
    _column = column;

    File indexFile = SegmentDirectoryPaths.findVectorIndexIndexFile(indexDir, column, config);
    if (indexFile == null || !indexFile.exists()) {
      throw new IllegalStateException(
          "Failed to find IVF index file for column: " + column + " in dir: " + indexDir);
    }

    try {
      _raf = new RandomAccessFile(indexFile, "r");
      _channel = _raf.getChannel();

      // --- Read Header via FileChannel (6 ints = 24 bytes) ---
      ByteBuffer headerBuf = ByteBuffer.allocate(24);
      readFully(_channel, headerBuf, 0);
      headerBuf.flip();

      int magic = headerBuf.getInt();
      Preconditions.checkState(magic == IvfFlatVectorIndexCreator.MAGIC,
          "Invalid IVF magic: 0x%s, expected 0x%s",
          Integer.toHexString(magic), Integer.toHexString(IvfFlatVectorIndexCreator.MAGIC));

      int version = headerBuf.getInt();
      Preconditions.checkState(version == IvfFlatVectorIndexCreator.FORMAT_VERSION,
          "Unsupported IVF format version: %s, expected: %s",
          version, IvfFlatVectorIndexCreator.FORMAT_VERSION);
      _indexFormatVersion = version;

      _dimension = headerBuf.getInt();
      _numVectors = headerBuf.getInt();
      _nlist = headerBuf.getInt();
      int distanceFunctionOrdinal = headerBuf.getInt();
      VectorIndexConfig.VectorDistanceFunction[] allFunctions = VectorIndexConfig.VectorDistanceFunction.values();
      Preconditions.checkState(distanceFunctionOrdinal >= 0 && distanceFunctionOrdinal < allFunctions.length,
          "Invalid distance function ordinal: %s", distanceFunctionOrdinal);
      _distanceFunction = allFunctions[distanceFunctionOrdinal];

      long centroidsOffset = 24;
      ByteBuffer quantizerHeader = ByteBuffer.allocate(8);
      readFully(_channel, quantizerHeader, 24);
      quantizerHeader.flip();

      int quantizerTypeOrdinal = quantizerHeader.getInt();
      VectorQuantizerType[] allQuantizerTypes = VectorQuantizerType.values();
      Preconditions.checkState(quantizerTypeOrdinal >= 0 && quantizerTypeOrdinal < allQuantizerTypes.length,
          "Invalid quantizer type ordinal: %s", quantizerTypeOrdinal);
      _quantizerType = allQuantizerTypes[quantizerTypeOrdinal];

      int quantizerParamsLength = quantizerHeader.getInt();
      Preconditions.checkState(quantizerParamsLength >= 0,
          "Invalid quantizer params length: %s", quantizerParamsLength);
      byte[] quantizerParams = new byte[quantizerParamsLength];
      if (quantizerParamsLength > 0) {
        ByteBuffer quantizerParamsBuf = ByteBuffer.wrap(quantizerParams);
        readFully(_channel, quantizerParamsBuf, 32);
      }
      _quantizer = VectorQuantizationUtils.createReadQuantizer(_quantizerType, _dimension, quantizerParams);
      _encodedBytesPerVector = _quantizer.getEncodedBytesPerVector();
      centroidsOffset = 32L + quantizerParamsLength;

      _defaultNprobe = Math.min(DEFAULT_NPROBE, _nlist);

      // --- Read Centroids into heap via FileChannel ---
      int centroidsBytesTotal = _nlist * _dimension * Float.BYTES;
      ByteBuffer centroidsBuf = ByteBuffer.allocate(centroidsBytesTotal);
      readFully(_channel, centroidsBuf, centroidsOffset);
      centroidsBuf.flip();

      _centroids = new float[_nlist][_dimension];
      for (int c = 0; c < _nlist; c++) {
        for (int d = 0; d < _dimension; d++) {
          _centroids[c][d] = centroidsBuf.getFloat();
        }
      }

      // --- Read inverted list offsets from footer via FileChannel ---
      // Footer: last 8 bytes = offsetToOffsets
      long fileSize = _channel.size();
      ByteBuffer footerBuf = ByteBuffer.allocate(8);
      readFully(_channel, footerBuf, fileSize - 8);
      footerBuf.flip();
      long offsetToOffsets = footerBuf.getLong();

      ByteBuffer offsetsBuf = ByteBuffer.allocate(_nlist * Long.BYTES);
      readFully(_channel, offsetsBuf, offsetToOffsets);
      offsetsBuf.flip();

      _listOffsets = new long[_nlist];
      for (int c = 0; c < _nlist; c++) {
        _listOffsets[c] = offsetsBuf.getLong();
      }

      // --- Pre-read list sizes via FileChannel ---
      _listSizes = new int[_nlist];
      ByteBuffer sizeBuf = ByteBuffer.allocate(4);
      for (int c = 0; c < _nlist; c++) {
        sizeBuf.clear();
        readFully(_channel, sizeBuf, _listOffsets[c]);
        sizeBuf.flip();
        _listSizes[c] = sizeBuf.getInt();
      }

      // --- Init observability ---
      _centroidAccessCounts = new AtomicLong[_nlist];
      for (int c = 0; c < _nlist; c++) {
        _centroidAccessCounts[c] = new AtomicLong(0);
      }

      LOGGER.info("Opened IVF_ON_DISK index for column: {}: {} vectors, {} centroids, dim={}, file={}, "
              + "formatVersion={}, quantizer={}",
          column, _numVectors, _nlist, _dimension, indexFile.getAbsolutePath(), _indexFormatVersion, _quantizerType);
    } catch (Exception e) {
      // Close resources if construction fails partway through (e.g., IllegalStateException
      // from Preconditions.checkState is not caught by IOException alone)
      closeQuietly();
      if (e instanceof IOException) {
        throw new RuntimeException(
            "Failed to open IVF index for column: " + column + " from dir: " + indexDir, e);
      }
      throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
    }
  }

  @Override
  public MutableRoaringBitmap getDocIds(float[] searchQuery, int topK) {
    _unfilteredSearches.incrementAndGet();
    return searchInternal(searchQuery, topK, null);
  }

  @Override
  public ImmutableRoaringBitmap getDocIds(float[] searchQuery, int topK, ImmutableRoaringBitmap preFilterBitmap) {
    Preconditions.checkArgument(preFilterBitmap != null, "preFilterBitmap must not be null");
    if (preFilterBitmap.isEmpty()) {
      return new MutableRoaringBitmap();
    }
    _filteredSearches.incrementAndGet();
    return searchInternal(searchQuery, topK, preFilterBitmap);
  }

  @Override
  public ImmutableRoaringBitmap getDocIdsWithinApproximateRadius(float[] searchQuery, float threshold,
      int maxCandidates) {
    Preconditions.checkArgument(searchQuery.length == _dimension,
        "Query dimension mismatch: expected %s, got %s", _dimension, searchQuery.length);
    Preconditions.checkArgument(maxCandidates > 0, "maxCandidates must be positive, got: %s", maxCandidates);

    if (_numVectors == 0 || _nlist == 0) {
      return new MutableRoaringBitmap();
    }

    _unfilteredSearches.incrementAndGet();
    _totalSearches.incrementAndGet();
    int effectiveNprobe = Math.min(getNprobe(), _nlist);
    int[] probeCentroids = findClosestCentroids(searchQuery, effectiveNprobe);
    int effectiveMaxCandidates = Math.min(maxCandidates, _numVectors);
    PriorityQueue<ScoredDoc> maxHeap = new PriorityQueue<>(effectiveMaxCandidates,
        (a, b) -> Float.compare(b._distance, a._distance));

    for (int probeIdx : probeCentroids) {
      _centroidAccessCounts[probeIdx].incrementAndGet();
      scanInvertedListWithinApproximateRadius(
          probeIdx, searchQuery, threshold, effectiveMaxCandidates, maxHeap);
    }

    MutableRoaringBitmap result = new MutableRoaringBitmap();
    for (ScoredDoc doc : maxHeap) {
      result.add(doc._docId);
    }
    return result;
  }

  private MutableRoaringBitmap searchInternal(float[] searchQuery, int topK,
      ImmutableRoaringBitmap preFilterBitmap) {
    Preconditions.checkArgument(searchQuery.length == _dimension,
        "Query dimension mismatch: expected %s, got %s", _dimension, searchQuery.length);
    Preconditions.checkArgument(topK > 0, "topK must be positive, got: %s", topK);

    if (_numVectors == 0 || _nlist == 0) {
      return new MutableRoaringBitmap();
    }

    _totalSearches.incrementAndGet();
    int effectiveNprobe = Math.min(getNprobe(), _nlist);
    int[] probeCentroids = findClosestCentroids(searchQuery, effectiveNprobe);

    int effectiveTopK = Math.min(topK, _numVectors);
    PriorityQueue<ScoredDoc> maxHeap = new PriorityQueue<>(effectiveTopK,
        (a, b) -> Float.compare(b._distance, a._distance));

    for (int probeIdx : probeCentroids) {
      _centroidAccessCounts[probeIdx].incrementAndGet();
      scanInvertedList(probeIdx, searchQuery, effectiveTopK, maxHeap, preFilterBitmap);
    }

    MutableRoaringBitmap result = new MutableRoaringBitmap();
    for (ScoredDoc doc : maxHeap) {
      result.add(doc._docId);
    }
    return result;
  }

  /**
   * Reads and scans an inverted list from the file via FileChannel.
   *
   * <p>Uses a ThreadLocal ByteBuffer to avoid per-call heap allocation for the list data,
   * and reuses a single float[] for reading each document vector.</p>
   *
   * @param centroidIdx index of the centroid
   * @param query query vector
   * @param topK max results to keep
   * @param maxHeap priority queue for top-K tracking
   * @param preFilterBitmap optional filter bitmap; if non-null, only matching docs are scored
   */
  private void scanInvertedList(int centroidIdx, float[] query, int topK,
      PriorityQueue<ScoredDoc> maxHeap, ImmutableRoaringBitmap preFilterBitmap) {
    int listSize = _listSizes[centroidIdx];
    if (listSize == 0) {
      return;
    }

    // Position after the listSize int
    long dataOffset = _listOffsets[centroidIdx] + 4;

    // Read entire inverted list data via FileChannel: docIds (int[]) + encoded vectors
    int listBytes = listSize * (Integer.BYTES + _encodedBytesPerVector);
    ByteBuffer buf = getOrResizeThreadLocalBuffer(listBytes);
    try {
      readFully(_channel, buf, dataOffset);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to read inverted list for centroid " + centroidIdx + " in column: " + _column, e);
    }
    buf.flip();

    // Read doc IDs
    int[] docIds = new int[listSize];
    for (int i = 0; i < listSize; i++) {
      docIds[i] = buf.getInt();
    }

    // Reuse per-doc buffers to avoid allocation churn in the hot loop.
    byte[] encodedVector = new byte[_encodedBytesPerVector];

    // Read and score vectors
    for (int i = 0; i < listSize; i++) {
      int docId = docIds[i];

      // In filter-aware mode, skip decoding and distance computation for rejected docs.
      if (preFilterBitmap != null && !preFilterBitmap.contains(docId)) {
        skipStoredVector(buf);
        continue;
      }

      float dist = readDistanceForCurrentVector(buf, query, encodedVector);
      offer(maxHeap, docId, dist, topK);
    }
  }

  private void scanInvertedListWithinApproximateRadius(int centroidIdx, float[] query, float threshold,
      int maxCandidates, PriorityQueue<ScoredDoc> maxHeap) {
    int listSize = _listSizes[centroidIdx];
    if (listSize == 0) {
      return;
    }

    long dataOffset = _listOffsets[centroidIdx] + 4;
    int listBytes = listSize * (Integer.BYTES + _encodedBytesPerVector);
    ByteBuffer buf = getOrResizeThreadLocalBuffer(listBytes);
    try {
      readFully(_channel, buf, dataOffset);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to read inverted list for centroid " + centroidIdx + " in column: " + _column, e);
    }
    buf.flip();

    int[] docIds = new int[listSize];
    for (int i = 0; i < listSize; i++) {
      docIds[i] = buf.getInt();
    }

    byte[] encodedVector = new byte[_encodedBytesPerVector];
    for (int i = 0; i < listSize; i++) {
      int docId = docIds[i];
      float distance = readDistanceForCurrentVector(buf, query, encodedVector);
      if (distance <= threshold) {
        offer(maxHeap, docId, distance, maxCandidates);
      }
    }
  }

  protected float readDistanceForCurrentVector(ByteBuffer buf, float[] query, byte[] encodedVector) {
    buf.get(encodedVector);
    return _quantizer.computeDistance(query, encodedVector, _distanceFunction);
  }

  private void skipStoredVector(ByteBuffer buf) {
    buf.position(buf.position() + _encodedBytesPerVector);
  }

  // ThreadLocal ByteBuffer for scanInvertedList to avoid per-call heap allocation.
  // Buffers larger than MAX_THREAD_LOCAL_BUFFER_SIZE are allocated fresh per call
  // to avoid pinning excessive memory on the thread.
  private static final int MAX_THREAD_LOCAL_BUFFER_SIZE = 4 * 1024 * 1024; // 4 MB
  private static final ThreadLocal<ByteBuffer> SCAN_BUFFER = new ThreadLocal<>();

  /**
   * Returns a ByteBuffer of at least the given capacity, cleared and ready for use.
   * Uses a thread-local buffer when the required capacity is within the size cap.
   * Allocates a fresh buffer for oversized requests to avoid pinning large allocations.
   */
  private static ByteBuffer getOrResizeThreadLocalBuffer(int requiredCapacity) {
    if (requiredCapacity > MAX_THREAD_LOCAL_BUFFER_SIZE) {
      // Allocate a fresh buffer for oversized requests — do not cache in ThreadLocal
      ByteBuffer buf = ByteBuffer.allocate(requiredCapacity);
      buf.limit(requiredCapacity);
      return buf;
    }
    ByteBuffer buf = SCAN_BUFFER.get();
    if (buf == null || buf.capacity() < requiredCapacity) {
      buf = ByteBuffer.allocate(requiredCapacity);
      SCAN_BUFFER.set(buf);
    } else {
      buf.clear();
    }
    buf.limit(requiredCapacity);
    return buf;
  }

  // -----------------------------------------------------------------------
  // NprobeAware implementation
  // -----------------------------------------------------------------------

  @Override
  public void setNprobe(int nprobe) {
    if (nprobe < 1) {
      throw new IllegalArgumentException("nprobe must be >= 1, got: " + nprobe);
    }
    _nprobeOverride.set(Math.min(nprobe, _nlist));
  }

  @Override
  public void clearNprobe() {
    _nprobeOverride.remove();
  }

  public int getNprobe() {
    Integer override = _nprobeOverride.get();
    return override != null ? override : _defaultNprobe;
  }

  // -----------------------------------------------------------------------
  // I/O helpers
  // -----------------------------------------------------------------------

  /**
   * Reads a ByteBuffer fully from the FileChannel at the given position.
   * Unlike {@link FileChannel#read(ByteBuffer, long)}, this method retries until
   * the buffer is completely filled or EOF is reached.
   */
  private static void readFully(FileChannel channel, ByteBuffer buf, long position)
      throws IOException {
    long pos = position;
    while (buf.hasRemaining()) {
      int bytesRead = channel.read(buf, pos);
      if (bytesRead < 0) {
        throw new IOException("Unexpected end of file at position " + pos);
      }
      pos += bytesRead;
    }
  }

  // -----------------------------------------------------------------------
  // Distance / centroid helpers
  // -----------------------------------------------------------------------

  private float computeDistance(float[] a, float[] b) {
    switch (_distanceFunction) {
      case EUCLIDEAN:
      case L2:
        return (float) VectorFunctions.euclideanDistance(a, b);
      case COSINE:
        return (float) VectorFunctions.cosineDistance(a, b);
      case INNER_PRODUCT:
      case DOT_PRODUCT:
        return (float) -VectorFunctions.dotProduct(a, b);
      default:
        throw new IllegalArgumentException("Unsupported distance function: " + _distanceFunction);
    }
  }

  private int[] findClosestCentroids(float[] query, int n) {
    int[] bestIndices = new int[n];
    float[] bestDistances = new float[n];
    Arrays.fill(bestDistances, Float.POSITIVE_INFINITY);

    for (int c = 0; c < _nlist; c++) {
      float distance = computeDistance(query, _centroids[c]);
      if (distance >= bestDistances[n - 1]) {
        continue;
      }
      int insertPos = n - 1;
      while (insertPos > 0 && distance < bestDistances[insertPos - 1]) {
        bestDistances[insertPos] = bestDistances[insertPos - 1];
        bestIndices[insertPos] = bestIndices[insertPos - 1];
        insertPos--;
      }
      bestDistances[insertPos] = distance;
      bestIndices[insertPos] = c;
    }
    return bestIndices;
  }

  private static void offer(PriorityQueue<ScoredDoc> heap, int docId, float distance, int maxCandidates) {
    if (heap.size() < maxCandidates) {
      heap.offer(new ScoredDoc(docId, distance));
    } else if (distance < heap.peek()._distance) {
      heap.poll();
      heap.offer(new ScoredDoc(docId, distance));
    }
  }

  // -----------------------------------------------------------------------
  // Debug / observability
  // -----------------------------------------------------------------------

  @Override
  public Map<String, Object> getIndexDebugInfo() {
    Map<String, Object> info = new LinkedHashMap<>();
    info.put("backend", "IVF_ON_DISK");
    info.put("column", _column);
    info.put("dimension", _dimension);
    info.put("numVectors", _numVectors);
    info.put("nlist", _nlist);
    info.put("distanceFunction", _distanceFunction.name());
    info.put("effectiveNprobe", getNprobe());
    info.put("indexFormatVersion", _indexFormatVersion);
    info.put("quantizer", _quantizerType.name());
    info.put("encodedBytesPerVector", _encodedBytesPerVector);
    info.put("totalSearches", _totalSearches.get());
    info.put("filteredSearches", _filteredSearches.get());
    info.put("unfilteredSearches", _unfilteredSearches.get());
    info.put("supportsPreFilter", true);
    info.put("storageMode", "fileChannel");

    // Compute cache warmth estimate: fraction of centroids accessed at least once
    long accessedCentroids = 0;
    for (AtomicLong count : _centroidAccessCounts) {
      if (count.get() > 0) {
        accessedCentroids++;
      }
    }
    info.put("warmCentroidRatio", _nlist > 0 ? (double) accessedCentroids / _nlist : 0.0);

    // List size stats
    int minListSize = Integer.MAX_VALUE;
    int maxListSize = 0;
    int emptyLists = 0;
    for (int size : _listSizes) {
      if (size == 0) {
        emptyLists++;
      }
      minListSize = Math.min(minListSize, size);
      maxListSize = Math.max(maxListSize, size);
    }
    if (_nlist > 0) {
      info.put("avgDocsPerList", _numVectors > 0 ? (double) _numVectors / _nlist : 0.0);
      info.put("minListSize", minListSize == Integer.MAX_VALUE ? 0 : minListSize);
      info.put("maxListSize", maxListSize);
      info.put("emptyLists", emptyLists);
    }
    return info;
  }

  @Override
  public void close() throws IOException {
    clearNprobe();
    _channel.close();
    _raf.close();
  }

  /**
   * Best-effort cleanup of I/O resources. Used during constructor failure to prevent resource leaks.
   */
  private void closeQuietly() {
    try {
      if (_channel != null) {
        _channel.close();
      }
    } catch (Exception ignored) {
      // best-effort
    }
    try {
      if (_raf != null) {
        _raf.close();
      }
    } catch (Exception ignored) {
      // best-effort
    }
  }

  // -----------------------------------------------------------------------
  // Accessors for testing
  // -----------------------------------------------------------------------

  @VisibleForTesting
  public int getDimension() {
    return _dimension;
  }

  @VisibleForTesting
  public int getNumVectors() {
    return _numVectors;
  }

  @VisibleForTesting
  public int getNlist() {
    return _nlist;
  }

  @VisibleForTesting
  public long getTotalSearches() {
    return _totalSearches.get();
  }

  private static final class ScoredDoc {
    final int _docId;
    final float _distance;

    ScoredDoc(int docId, float distance) {
      _docId = docId;
      _distance = distance;
    }
  }
}
