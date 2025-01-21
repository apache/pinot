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
package org.apache.pinot.plugin.stream.push;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * Kinesis stream specific config
 */
public class PushBasedIngestionBuffer {
  private final BufferedRecord[] _buffer;
  private final int _capacity;

  private final ReadWriteLock _bufferLock = new ReentrantReadWriteLock();
  private int _writeOffset = 0;

  public PushBasedIngestionBuffer(int capacity) {
    _buffer = new BufferedRecord[capacity];
    _capacity = capacity;
  }

  public void append(BufferedRecord record) {
    _bufferLock.writeLock().lock();
    try {
      record.setOffset(_writeOffset);
      _buffer[_writeOffset % _capacity] = record;
      _writeOffset++;
    } finally {
      _bufferLock.writeLock().unlock();
    }
  }

  public void append(List<BufferedRecord> records) {
    _bufferLock.writeLock().lock();
    try {
      for (BufferedRecord record : records) {
        record.setOffset(_writeOffset);
        _buffer[_writeOffset % _capacity] = record;
        _writeOffset++;
      }
    } finally {
      _bufferLock.writeLock().unlock();
    }
  }

  public List<BufferedRecord> readNextBatch(long offset) {
    _bufferLock.readLock().lock();
    try {
      if (offset >= _writeOffset) {
        return Collections.emptyList();
      }
      List<BufferedRecord> messages = new ArrayList<>((int) (_writeOffset - offset));
      for (long i = offset; i < _writeOffset; i++) {
        messages.add(_buffer[(int) i % _capacity]);
      }
      return messages;
    } finally {
      _bufferLock.readLock().unlock();
    }
  }
}
