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

import com.google.common.base.Preconditions;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;


/**
 * FileInfo contains the file path and many optional file attributes like mtime, length etc.
 */
public class FileInfo {
  private final String _filePath;
  private final Long _lastModifiedTime;
  private final Long _length;
  private final Boolean _isDirectory;

  private FileInfo(String filePath, Long lastModifiedTime, Long length, Boolean isDirectory) {
    _filePath = filePath;
    _lastModifiedTime = lastModifiedTime;
    _length = length;
    _isDirectory = isDirectory;
  }

  public String getFilePath() {
    return _filePath;
  }

  public Optional<Long> getLastModifiedTime() {
    return Optional.ofNullable(_lastModifiedTime);
  }

  public Optional<Long> getLength() {
    return Optional.ofNullable(_length);
  }

  public Optional<Boolean> isDirectory() {
    return Optional.ofNullable(_isDirectory);
  }

  @Override
  public String toString() {
    return "FileInfo{" + "_filePath='" + _filePath + '\'' + ", _lastModifiedTime=" + _lastModifiedTime + ", _length="
        + _length + ", _isDirectory=" + _isDirectory + '}';
  }

  public static class Builder {
    private String _filePath;
    private Long _lastModifiedTime;
    private Long _length;
    private Boolean _isDirectory;

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

    public FileInfo build() {
      Preconditions.checkArgument(StringUtils.isNotEmpty(_filePath), "The filePath is required");
      return new FileInfo(_filePath, _lastModifiedTime, _length, _isDirectory);
    }
  }
}