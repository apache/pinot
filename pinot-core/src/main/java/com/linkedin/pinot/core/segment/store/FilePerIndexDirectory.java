/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.core.segment.store;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class FilePerIndexDirectory extends ColumnIndexDirectory {
  private static Logger LOGGER = LoggerFactory.getLogger(FilePerIndexDirectory.class);

  protected FilePerIndexDirectory(File segmentDirectory, SegmentMetadataImpl metadata, ReadMode readMode) {
    super(segmentDirectory, metadata, readMode);
  }

  @Override
  public PinotDataBuffer getDictionaryBufferFor(String column)
      throws IOException {
    String dictFilename = metadata.getDictionaryFileName(column, metadata.getVersion());
    return mapForReads(dictFilename, "dictionary.reader");
  }

  @Override
  public PinotDataBuffer newDictionaryBuffer(String column, int size)
      throws IOException {
    String dictFilename = metadata.getDictionaryFileName(column, metadata.getVersion());
    return mapForWrites(dictFilename, size, "dictionary.writer");
  }

  @Override
  public PinotDataBuffer getForwardIndexBufferFor(String column)
      throws IOException {
    String fwdIndexFilename = metadata.getForwardIndexFileName(column, metadata.getVersion());
    return mapForReads(fwdIndexFilename, "forward_index.reader");
  }

  @Override
  public PinotDataBuffer newForwardIndexBuffer(String column, int size)
      throws IOException {
    String fwdIndexFilename = metadata.getForwardIndexFileName(column, metadata.getVersion());
    return mapForWrites(fwdIndexFilename, size, "forward_index.writer");
  }

  @Override
  public PinotDataBuffer getInvertedIndexBufferFor(String column)
      throws IOException {
    String invertedIndexFname = metadata.getBitmapInvertedIndexFileName(column, metadata.getVersion());
    return mapForReads(invertedIndexFname, "inverted_index.reader");
  }

  @Override
  public PinotDataBuffer newInvertedIndexBuffer(String column, int size)
      throws IOException {
    String invertedIndexFname = metadata.getBitmapInvertedIndexFileName(column, metadata.getVersion());
    return mapForWrites(invertedIndexFname, size, "inverted_index.writer");
  }

  @Override
  public boolean hasIndexFor(String column, ColumnIndexType type) {
    String filename;
    switch(type) {
      case DICTIONARY:
        filename = metadata.getDictionaryFileName(column, metadata.getVersion());
        break;
      case FORWARD_INDEX:
        filename = metadata.getForwardIndexFileName(column, metadata.getVersion());
        break;
      case INVERTED_INDEX:
        filename = metadata.getBitmapInvertedIndexFileName(column, metadata.getVersion());
        break;
      default:
        throw new UnsupportedOperationException("Unknown column index type: " + type.name());
    }

    File indexFile = new File(filename);
    return indexFile.exists();
  }

  @Override
  public void close() {

  }

  private PinotDataBuffer mapForWrites(String filename, int size, String context)
      throws IOException {

    Preconditions.checkNotNull(filename);
    Preconditions.checkArgument(size >= 0, "File size must not be negative. Path: " + filename);

    File f = new File(segmentDirectory, filename);
    Preconditions.checkState(!f.exists(), "File: " + filename + " already exists");
    String allocContext = allocationContext(f, context);

    // always mmap for writes
    return PinotDataBuffer.fromFile(f, 0, size, ReadMode.mmap, FileChannel.MapMode.READ_WRITE, context);
  }

  private PinotDataBuffer mapForReads(String fileName, String context)
      throws IOException {
    Preconditions.checkNotNull(fileName);
    Preconditions.checkNotNull(context);

    File file = new File(segmentDirectory, fileName);
    String allocationContext = allocationContext(file, context);
    return PinotDataBuffer
        .fromFile(file, readMode, FileChannel.MapMode.READ_ONLY, allocationContext);
  }
}
