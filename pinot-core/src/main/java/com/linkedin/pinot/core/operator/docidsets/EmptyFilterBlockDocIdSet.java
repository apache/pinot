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
package com.linkedin.pinot.core.operator.docidsets;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.operator.dociditerators.EmptyBlockDocIdIterator;


/**
 * Singleton class which extends {@link FilterBlockDocIdSet} that is empty, i.e. does not contain any document.
 */
public final class EmptyFilterBlockDocIdSet implements FilterBlockDocIdSet {
  private EmptyFilterBlockDocIdSet() {
  }

  private static final EmptyFilterBlockDocIdSet INSTANCE = new EmptyFilterBlockDocIdSet();

  public static EmptyFilterBlockDocIdSet getInstance() {
    return INSTANCE;
  }

  @Override
  public int getMinDocId() {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getMaxDocId() {
    return Integer.MIN_VALUE;
  }

  @Override
  public void setStartDocId(int startDocId) {
  }

  @Override
  public void setEndDocId(int endDocId) {
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    return 0L;
  }

  @Override
  public BlockDocIdIterator iterator() {
    return EmptyBlockDocIdIterator.getInstance();
  }

  @Override
  public <T> T getRaw() {
    return null;
  }
}
