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
package org.apache.pinot.core.operator.docidsets;

import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.operator.dociditerators.MatchAllDocIdIterator;


public abstract class MatchAllDocIdSet implements BlockDocIdSet {
  protected final int _numDocs;

  public static MatchAllDocIdSet create(int numDocs, boolean ascending) {
    return ascending ? new Asc(numDocs) : new Desc(numDocs);
  }

  private MatchAllDocIdSet(int numDocs) {
    _numDocs = numDocs;
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    return 0L;
  }

  public static final class Asc extends MatchAllDocIdSet {
    private Asc(int numDocs) {
      super(numDocs);
    }

    @Override
    public MatchAllDocIdIterator iterator() {
      return MatchAllDocIdIterator.create(_numDocs, true);
    }

    @Override
    public boolean isAscending() {
      return true;
    }

    @Override
    public boolean isDescending() {
      return _numDocs == 0;
    }
  }

  public static final class Desc extends MatchAllDocIdSet {
    private Desc(int numDocs) {
      super(numDocs);
    }

    @Override
    public BlockDocIdIterator iterator() {
      return MatchAllDocIdIterator.create(_numDocs, false);
    }

    @Override
    public boolean isAscending() {
      return _numDocs == 0;
    }

    @Override
    public boolean isDescending() {
      return true;
    }
  }
}
