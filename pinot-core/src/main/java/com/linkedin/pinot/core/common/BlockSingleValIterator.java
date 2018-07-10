/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.common;

/**
 *
 * TODO: Split into two classes, one for iterator over data, another over dictionary id's.
 */
public abstract class BlockSingleValIterator implements BlockValIterator {

  char nextCharVal() {
    throw new UnsupportedOperationException();
  }

  public int nextIntVal() {
    throw new UnsupportedOperationException();
  }

  public float nextFloatVal() {
    throw new UnsupportedOperationException();
  }

  public long nextLongVal() {
    throw new UnsupportedOperationException();
  }

  public double nextDoubleVal() {
    throw new UnsupportedOperationException();
  }

  public byte[] nextBytesVal() {
    throw new UnsupportedOperationException();
  }

  public String nextStringVal() {
    throw new UnsupportedOperationException();
  }
}
