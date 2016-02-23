/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator.blocks;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.operator.dociditerators.EmptyDocIdIterator;
import com.linkedin.pinot.core.operator.docidsets.BitmapBasedBlockDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.SizeBasedDocIdSet;


public class BlockFactory {

  public static BlockDocIdSet getBlockDocIdSetBackedByBitmap(final ImmutableRoaringBitmap bitmap) {
    return new BitmapBasedBlockDocIdSet(bitmap);
  }

  public static BlockDocIdSet getDummyBlockDocIdSet(final int maxDocId) {

    return new SizeBasedDocIdSet(maxDocId);
  }

  public static BlockDocIdIterator emptyBlockDocIdSetIterator() {
    return new EmptyDocIdIterator();
  }
}
