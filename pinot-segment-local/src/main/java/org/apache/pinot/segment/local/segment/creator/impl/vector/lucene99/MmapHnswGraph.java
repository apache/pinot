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

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;


/// Mutable merge graph with neighbor IDs and scores stored in a confined temporary mapping.
final class MmapHnswGraph extends HnswGraph implements Closeable {
  private static final int WINDOW_BYTES = 1 << 20;

  private final FileChannel _channel;
  private final ByteBuffer _reservationBuffer = ByteBuffer.allocate(64 << 10);
  private final Arena _arena = Arena.ofConfined();
  private final MmapHnswNeighborList[][] _nodes;
  private final int _maxConn;

  private int[] _levelCounts = new int[4];
  private int[][] _upperLevelNodes;
  private MemorySegment _window;
  private int _windowPosition = WINDOW_BYTES;
  private long _nextFileOffset;
  private int _entryNode = -1;
  private int _numLevels;
  private MmapHnswNeighborList _current;
  private int _currentNeighbor;

  static MmapHnswGraph open(Path directory, int vectorCount, int maxConn)
      throws IOException {
    Path file;
    try {
      file = Files.createTempFile(directory, "hnsw-merge-", ".adjacency");
    } catch (IOException e) {
      throw scratchFailure(e);
    }
    FileChannel channel = null;
    try {
      channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE,
          StandardOpenOption.DELETE_ON_CLOSE);
      return new MmapHnswGraph(channel, vectorCount, maxConn);
    } catch (IOException | RuntimeException | Error e) {
      IOUtils.closeWhileHandlingException(channel);
      try {
        Files.deleteIfExists(file);
      } catch (IOException cleanup) {
        e.addSuppressed(cleanup);
      }
      if (e instanceof IOException ioException) {
        throw scratchFailure(ioException);
      }
      throw e;
    }
  }

  private MmapHnswGraph(FileChannel channel, int vectorCount, int maxConn) {
    _channel = channel;
    _nodes = new MmapHnswNeighborList[vectorCount][];
    _maxConn = maxConn;
  }

  boolean contains(int node) {
    return _nodes[node] != null;
  }

  void addNode(int node, int topLevel)
      throws IOException {
    assert !contains(node);
    if (topLevel >= _levelCounts.length) {
      _levelCounts = Arrays.copyOf(_levelCounts, topLevel + 1);
    }
    MmapHnswNeighborList[] levels = new MmapHnswNeighborList[topLevel + 1];
    for (int level = topLevel; level >= 0; level--) {
      // Allow one unused slot for the candidate before pruning an overfull neighbor list.
      levels[level] = allocate(neighborLimit(level) + 1);
      _levelCounts[level]++;
    }
    _nodes[node] = levels;
  }

  void setEntryNode(int node, int level) {
    _entryNode = node;
    _numLevels = level + 1;
  }

  int neighborLimit(int level) {
    return level == 0 ? 2 * _maxConn : _maxConn;
  }

  MmapHnswNeighborList neighbors(int level, int node) {
    return _nodes[node][level];
  }

  @Override
  public void seek(int level, int target) {
    _current = neighbors(level, target);
    _currentNeighbor = 0;
  }

  @Override
  public int nextNeighbor() {
    return _currentNeighbor < _current.size() ? _current.node(_currentNeighbor++) : DocIdSetIterator.NO_MORE_DOCS;
  }

  @Override
  public int size() {
    return _levelCounts[0];
  }

  @Override
  public int maxNodeId() {
    return _nodes.length - 1;
  }

  @Override
  public int numLevels() {
    return _numLevels;
  }

  @Override
  public int entryNode() {
    return _entryNode;
  }

  @Override
  public NodesIterator getNodesOnLevel(int level) {
    assert size() == _nodes.length : "graph build is incomplete";
    if (level == 0) {
      return new ArrayNodesIterator(_nodes.length);
    }
    if (_upperLevelNodes == null) {
      _upperLevelNodes = new int[_numLevels][];
      int[] offsets = new int[_numLevels];
      for (int upperLevel = 1; upperLevel < _numLevels; upperLevel++) {
        _upperLevelNodes[upperLevel] = new int[_levelCounts[upperLevel]];
      }
      for (int node = 0; node < _nodes.length; node++) {
        for (int upperLevel = 1; upperLevel < _nodes[node].length; upperLevel++) {
          _upperLevelNodes[upperLevel][offsets[upperLevel]++] = node;
        }
      }
    }
    return new ArrayNodesIterator(_upperLevelNodes[level], _levelCounts[level]);
  }

  private MmapHnswNeighborList allocate(int capacity)
      throws IOException {
    int bytes = capacity * MmapHnswNeighborList.EDGE_BYTES;
    assert bytes <= WINDOW_BYTES;
    if (_windowPosition + bytes > WINDOW_BYTES) {
      try {
        reserveWindow();
        _window = _channel.map(FileChannel.MapMode.READ_WRITE, _nextFileOffset, WINDOW_BYTES, _arena);
      } catch (IOException e) {
        throw scratchFailure(e);
      }
      _nextFileOffset += WINDOW_BYTES;
      _windowPosition = 0;
    }
    MmapHnswNeighborList list = new MmapHnswNeighborList(_window, _windowPosition, capacity);
    _windowPosition += bytes;
    return list;
  }

  private static IOException scratchFailure(IOException cause) {
    return new IOException("Failed to allocate local HNSW merge scratch space. Check the build directory or set "
        + VectorIndexConfig.MMAP_GRAPH_ON_MERGE + " to false", cause);
  }

  private void reserveWindow()
      throws IOException {
    // Write and force the range before mapping so disk allocation failure can surface as IOException.
    long end = _nextFileOffset + WINDOW_BYTES;
    for (long offset = _nextFileOffset; offset < end; ) {
      _reservationBuffer.clear();
      _reservationBuffer.limit((int) Math.min(_reservationBuffer.capacity(), end - offset));
      while (_reservationBuffer.hasRemaining()) {
        int written = _channel.write(_reservationBuffer, offset);
        if (written == 0) {
          throw new IOException("Failed to reserve HNSW merge adjacency at offset " + offset);
        }
        offset += written;
      }
    }
    _channel.force(false);
  }

  @Override
  public void close()
      throws IOException {
    try (_channel; _arena) {
      // Resources close in reverse order, unmapping before the channel closes.
    }
  }
}
