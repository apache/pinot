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
package com.linkedin.pinot.core.operator.dociditerators;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;


/**
 * Singleton class which extends {@link BlockDocIdIterator} that is empty, i.e. does not contain any document.
 */
public final class EmptyBlockDocIdIterator implements BlockDocIdIterator {
  private EmptyBlockDocIdIterator() {
  }

  private static final EmptyBlockDocIdIterator INSTANCE = new EmptyBlockDocIdIterator();

  public static EmptyBlockDocIdIterator getInstance() {
    return INSTANCE;
  }

  @Override
  public int next() {
    return Constants.EOF;
  }

  @Override
  public int advance(int targetDocId) {
    return Constants.EOF;
  }

  @Override
  public int currentDocId() {
    return Constants.EOF;
  }
}
