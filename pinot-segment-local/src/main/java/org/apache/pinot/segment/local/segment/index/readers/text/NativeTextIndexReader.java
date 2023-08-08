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
package org.apache.pinot.segment.local.segment.index.readers.text;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.pinot.segment.local.segment.creator.impl.text.NativeTextIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.BitmapInvertedIndexReader;
import org.apache.pinot.segment.local.utils.nativefst.FST;
import org.apache.pinot.segment.local.utils.nativefst.FSTHeader;
import org.apache.pinot.segment.local.utils.nativefst.ImmutableFST;
import org.apache.pinot.segment.local.utils.nativefst.utils.RegexpMatcher;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NativeTextIndexReader implements TextIndexReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(NativeTextIndexReader.class);

  private final String _column;
  private final PinotDataBuffer _buffer;

  private FST _fst;
  private BitmapInvertedIndexReader _invertedIndex;

  public NativeTextIndexReader(String column, File indexDir) {
    _column = column;
    try {
      String desc = "Native text index buffer: " + column;
      File indexFile = getTextIndexFile(indexDir);
      //TODO: Pass the load mode in (Direct, MMap)
      _buffer =
          PinotDataBuffer.mapFile(indexFile, /* readOnly */ true, 0, indexFile.length(), ByteOrder.BIG_ENDIAN, desc);
      populateIndexes();
    } catch (Exception e) {
      LOGGER.error("Failed to instantiate native text index reader for column {}, exception {}", column,
          e.getMessage());
      throw new RuntimeException(e);
    }
  }

  private File getTextIndexFile(File segmentIndexDir) {
    // will return null if file does not exist
    File file = SegmentDirectoryPaths.findNativeTextIndexIndexFile(segmentIndexDir, _column);
    if (file == null) {
      throw new IllegalStateException("Failed to find text index file for column: " + _column);
    }
    return file;
  }

  private void populateIndexes() {
    int fstMagic = _buffer.getInt(0);
    Preconditions.checkState(fstMagic == FSTHeader.FST_MAGIC, "Invalid native text index magic header: %s", fstMagic);
    int version = _buffer.getInt(4);
    Preconditions.checkState(version == NativeTextIndexCreator.VERSION, "Unsupported native text index version: %s",
        version);

    int fstDataLength = _buffer.getInt(8);
    long invertedIndexLength = _buffer.getLong(12);
    int numBitMaps = _buffer.getInt(20);

    long fstDataStartOffset = NativeTextIndexCreator.HEADER_LENGTH;
    long fstDataEndOffset = fstDataStartOffset + fstDataLength;
    ByteBuffer byteBuffer = _buffer.toDirectByteBuffer(fstDataStartOffset, fstDataLength);
    try {
      _fst = FST.read(new ByteBufferInputStream(Collections.singletonList(byteBuffer)), ImmutableFST.class, true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    long invertedIndexEndOffset = fstDataEndOffset + invertedIndexLength;
    _invertedIndex =
        new BitmapInvertedIndexReader(_buffer.view(fstDataEndOffset, invertedIndexEndOffset, ByteOrder.BIG_ENDIAN),
            numBitMaps);
  }

  @Override
  public ImmutableRoaringBitmap getDictIds(String searchQuery) {
    throw new UnsupportedOperationException("");
  }

  @Override
  public MutableRoaringBitmap getDocIds(String searchQuery) {
    try {
      MutableRoaringBitmap matchingDocIds = new MutableRoaringBitmap();
      RegexpMatcher.regexMatch(searchQuery, _fst, dictId -> matchingDocIds.or(_invertedIndex.getDocIds(dictId)));
      return matchingDocIds;
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while running query: " + searchQuery, e);
    }
  }

  @Override
  public void close()
      throws IOException {
    _buffer.close();
  }
}
