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

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Custom Lucene Directory implementation that reads from a PinotDataBuffer.
 * This allows Lucene to work with combined text index buffers without creating
 * temporary files or directories.
 */
public class PinotBufferLuceneDirectory extends Directory {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotBufferLuceneDirectory.class);

  private final PinotDataBuffer _indexBuffer;
  private final java.util.Map<String, LuceneTextIndexHeader.FileInfo> _fileMap;
  private final String _column;
  private final Set<String> _fileNames;

  public PinotBufferLuceneDirectory(PinotDataBuffer indexBuffer, Map<String, LuceneTextIndexHeader.FileInfo> fileMap,
      String column) {
    _indexBuffer = indexBuffer;
    _fileMap = fileMap;
    _column = column;
    _fileNames = fileMap.keySet();
  }

  @Override
  public String[] listAll() {
    return _fileNames.toArray(new String[0]);
  }

  @Override
  public void deleteFile(String name)
      throws IOException {
    throw new UnsupportedOperationException("Delete not supported in read-only buffer directory");
  }

  @Override
  public long fileLength(String name)
      throws IOException {
    LuceneTextIndexHeader.FileInfo fileInfo = _fileMap.get(name);
    if (fileInfo == null) {
      throw new IOException("File not found: " + name);
    }
    return fileInfo.getSize();
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context)
      throws IOException {
    throw new UnsupportedOperationException("Write not supported in read-only buffer directory");
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
      throws IOException {
    throw new UnsupportedOperationException("Write not supported in read-only buffer directory");
  }

  @Override
  public void sync(Collection<String> names)
      throws IOException {
    // No-op for read-only directory
  }

  @Override
  public void syncMetaData()
      throws IOException {
    // No-op for read-only directory
  }

  @Override
  public void rename(String source, String dest)
      throws IOException {
    throw new UnsupportedOperationException("Write not supported in read-only buffer directory");
  }

  @Override
  public IndexInput openInput(String name, IOContext context)
      throws IOException {
    LuceneTextIndexHeader.FileInfo fileInfo = _fileMap.get(name);
    if (fileInfo == null) {
      throw new IOException("File not found: " + name);
    }
    return new PinotBufferIndexInput(name, _indexBuffer, fileInfo.getOffset(), fileInfo.getSize());
  }

  @Override
  public Lock obtainLock(String name)
      throws IOException {
    throw new UnsupportedOperationException("Locking not supported in read-only buffer directory");
  }

  @Override
  public void close()
      throws IOException {
    // No-op - buffer is managed externally
  }

  @Override
  public String toString() {
    return "PinotBufferLuceneDirectory(column=" + _column + ", files=" + _fileNames.size() + ")";
  }

  @Override
  public Set<String> getPendingDeletions()
      throws IOException {
    throw new UnsupportedOperationException("Pending deletions not supported in read-only buffer directory");
  }
}
