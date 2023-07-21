package org.apache.pinot.segment.spi.index.reader;

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
public class ForwardIndexByteRange {
  private final long _offset;
  private final int _size;
  // To tell if size uses bit or byte as unit, for fwd index reader reading values of fixed bits.
  private final boolean _isSizeOfBit;

  public static ForwardIndexByteRange newByteRange(long offset, int sizeInBytes) {
    return new ForwardIndexByteRange(offset, sizeInBytes, false);
  }

  public static ForwardIndexByteRange newBitRange(long offset, int sizeInBits) {
    return new ForwardIndexByteRange(offset, sizeInBits, true);
  }

  private ForwardIndexByteRange(long offset, int size, boolean isSizeOfBit) {
    _offset = offset;
    _size = size;
    _isSizeOfBit = isSizeOfBit;
  }

  public long getOffset() {
    return _offset;
  }

  public int getSize() {
    return _size;
  }

  public boolean isSizeOfBit() {
    return _isSizeOfBit;
  }

  @Override
  public String toString() {
    return "Range{" + "_offset=" + _offset + ", _size=" + _size + ", _isSizeOfBit=" + _isSizeOfBit + '}';
  }
}
