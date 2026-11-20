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
package org.apache.pinot.segment.local.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.apache.pinot.segment.local.segment.index.readers.text.LuceneTextIndexBufferReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


/// Detects indexes written in a newer, unreadable Lucene format before segment preprocessing replaces them.
/// Current/older writers only require a commit-header read, avoiding a second full index open on normal loads.
/// Corruption and ordinary I/O errors propagate instead of being treated as format incompatibility.
/// Stateless and thread-safe; callers retain ownership of supplied directories and buffers.
public final class LuceneIndexCompatibilityUtils {
  private LuceneIndexCompatibilityUtils() {
  }

  public static boolean isIncompatible(File indexDirectory)
      throws IOException {
    if (!indexDirectory.isDirectory()) {
      throw new FileNotFoundException("Lucene index directory does not exist: " + indexDirectory);
    }
    try (Directory directory = FSDirectory.open(indexDirectory.toPath())) {
      return isIncompatible(directory);
    }
  }

  public static boolean isIncompatible(PinotDataBuffer buffer, String column)
      throws IOException {
    try (Directory directory = LuceneTextIndexBufferReader.createLuceneDirectory(buffer, column)) {
      return isIncompatible(directory);
    }
  }

  public static boolean isIncompatible(Directory directory)
      throws IOException {
    String commit = SegmentInfos.getLastCommitSegmentsFileName(directory);
    if (commit == null) {
      throw new IndexNotFoundException("No Lucene commit found in " + directory);
    }
    Version writerVersion;
    try (IndexInput input = directory.openInput(commit, IOContext.READONCE)) {
      // Both Lucene 9.12 and 10.0 use this commit layout. Inspect its creation version before SegmentInfos,
      // whose constructor rejects a future major with IllegalArgumentException rather than a format exception.
      try {
        CodecUtil.checkHeader(input, "segments", SegmentInfos.VERSION_74, SegmentInfos.VERSION_86);
      } catch (IndexFormatTooNewException e) {
        CodecUtil.checksumEntireFile(input);
        return true;
      } catch (IndexFormatTooOldException e) {
        // Leave legacy-reader support (including the Lucene 8 expert-reader path) to the actual index reader.
        return false;
      }
      input.skipBytes(StringHelper.ID_LENGTH);
      CodecUtil.checkIndexHeaderSuffix(input,
          Long.toString(SegmentInfos.generationFromSegmentsFileName(commit), Character.MAX_RADIX));
      writerVersion = Version.fromBits(input.readVInt(), input.readVInt(), input.readVInt());
      int createdMajor = input.readVInt();
      if (createdMajor < 6 || createdMajor > writerVersion.major) {
        throw new CorruptIndexException("Invalid Lucene index creation version: " + createdMajor, input);
      }
      if (Version.LATEST.onOrAfter(writerVersion)) {
        return false;
      }
      CodecUtil.checksumEntireFile(input);
      if (createdMajor > Version.LATEST.major) {
        return true;
      }
    }
    // A newer writer can append to an older index without changing its creation version. Only replace it if
    // this runtime actually rejects the result; a metadata-only commit may leave every segment readable.
    try (DirectoryReader ignored = DirectoryReader.open(directory)) {
      return false;
    } catch (IndexFormatTooNewException e) {
      return true;
    } catch (IllegalArgumentException e) {
      // Lucene reports an unavailable built-in codec through this exception, not IndexFormatTooNewException.
      // Restrict recovery to the future writer's codec family; missing older/custom codec plugins still fail.
      String message = e.getMessage();
      if (message != null && message.matches("Could not load codec 'Lucene" + writerVersion.major + "[0-9]*'\\..*")) {
        return true;
      }
      throw e;
    }
  }
}
