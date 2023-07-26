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
package org.apache.pinot.segment.local.realtime.impl.invertedindex;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.pinot.segment.local.utils.nativefst.mutablefst.MutableFST;
import org.apache.pinot.segment.local.utils.nativefst.mutablefst.MutableFSTImpl;
import org.apache.pinot.segment.local.utils.nativefst.utils.RealTimeRegexpMatcher;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

@Deprecated
public class NativeMutableFSTIndex implements TextIndexReader {
  private final MutableFST _fst;
  private final ReentrantReadWriteLock.ReadLock _readLock;
  private final ReentrantReadWriteLock.WriteLock _writeLock;
  private int _dictId;

  @Deprecated
  public NativeMutableFSTIndex(String columnName) {
    this();
  }

  public NativeMutableFSTIndex() {
    _fst = new MutableFSTImpl();

    ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    _readLock = readWriteLock.readLock();
    _writeLock = readWriteLock.writeLock();
  }

  public void add(String document) {
    _writeLock.lock();
    try {
      _fst.addPath(document, _dictId);
      _dictId++;
    } finally {
      _writeLock.unlock();
    }
  }

  public ImmutableRoaringBitmap getDictIds(String searchQuery) {
    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
    _readLock.lock();
    try {
      RealTimeRegexpMatcher.regexMatch(searchQuery, _fst, writer::add);
      return writer.get();
    } finally {
      _readLock.unlock();
    }
  }

  @Override
  public MutableRoaringBitmap getDocIds(String searchQuery) {
    throw new UnsupportedOperationException("getDocIds is not supported for NativeMutableFSTIndex");
  }

  @Override
  public void close()
      throws IOException {
  }
}
