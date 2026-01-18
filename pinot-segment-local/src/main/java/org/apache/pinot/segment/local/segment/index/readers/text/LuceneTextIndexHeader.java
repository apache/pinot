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

import java.util.List;
import java.util.Map;


/**
 * Metadata classes for Lucene text index buffer.
 * Contains BufferMetadata and FileInfo classes with proper getter APIs.
 */
public class LuceneTextIndexHeader {

  /**
   * Metadata class for buffer information
   */
  public static class TextIndexMetadata {
    private final String _magicNumber;
    private final int _version;
    private final long _totalSize;
    private final int _fileCount;
    private final List<String> _fileNames;
    private final Map<String, FileInfo> _fileInfoMap;

    public TextIndexMetadata(String magicNumber, int version, long totalSize, int fileCount, List<String> fileNames,
        Map<String, FileInfo> fileInfoMap) {
      _magicNumber = magicNumber;
      _version = version;
      _totalSize = totalSize;
      _fileCount = fileCount;
      _fileNames = fileNames;
      _fileInfoMap = fileInfoMap;
    }

    public String getMagicNumber() {
      return _magicNumber;
    }

    public int getVersion() {
      return _version;
    }

    public long getTotalSize() {
      return _totalSize;
    }

    public int getFileCount() {
      return _fileCount;
    }

    public List<String> getFileNames() {
      return _fileNames;
    }

    public Map<String, FileInfo> getFileInfoMap() {
      return _fileInfoMap;
    }
  }

  /**
   * File information class
   */
  public static class FileInfo {
    private final String _name;
    private final long _offset;
    private final long _size;

    public FileInfo(String name, long offset, long size) {
      _name = name;
      _offset = offset;
      _size = size;
    }

    public String getName() {
      return _name;
    }

    public long getOffset() {
      return _offset;
    }

    public long getSize() {
      return _size;
    }
  }
}
