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
package org.apache.pinot.spi.filesystem;

/**
 * FileMetadata contains the file path and many optional file attributes like mtime, length etc.
 */
public class FileMetadata {
  private final String _filePath;
  private final long _lastModifiedTime;
  private final long _length;
  private final boolean _isDirectory;

  private FileMetadata(String filePath, long lastModifiedTime, long length, boolean isDirectory) {
    _filePath = filePath;
    _lastModifiedTime = lastModifiedTime;
    _length = length;
    _isDirectory = isDirectory;
  }

  public String getFilePath() {
    return _filePath;
  }

  public long getLastModifiedTime() {
    return _lastModifiedTime;
  }

  public long getLength() {
    return _length;
  }

  public boolean isDirectory() {
    return _isDirectory;
  }

  @Override
  public String toString() {
    return "FileMetadata{" + "_filePath='" + _filePath + '\'' + ", _lastModifiedTime=" + _lastModifiedTime
        + ", _length=" + _length + ", _isDirectory=" + _isDirectory + '}';
  }

  public static class Builder {
    private String _filePath;
    private long _lastModifiedTime;
    private long _length;
    private boolean _isDirectory;

    public Builder setFilePath(String filePath) {
      _filePath = filePath;
      return this;
    }

    public Builder setLastModifiedTime(long lastModifiedTime) {
      _lastModifiedTime = lastModifiedTime;
      return this;
    }

    public Builder setLength(long length) {
      _length = length;
      return this;
    }

    public Builder setIsDirectory(boolean isDirectory) {
      _isDirectory = isDirectory;
      return this;
    }

    public FileMetadata build() {
      return new FileMetadata(_filePath, _lastModifiedTime, _length, _isDirectory);
    }
  }
}
