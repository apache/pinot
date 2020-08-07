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
package org.apache.pinot.core.operator.dociditerators;

import java.util.List;
import org.apache.pinot.core.common.Constants;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * The interface for SV and MV scanning iterator
 */
public interface ValueBasedScanDocIdIterator extends ScanBasedDocIdIterator{

  /**
   * For docId >= bitmapIterator.peekNext()
   * Return the first docId that presents in both this ValueBasedDocIdIterator and the bitmapIterator
   * (During this process bitmapIterator.next() will be called until bitmapIterator.peekNext() == return value)
   */
  int fastAdvance(PeekableIntIterator bitmapIterator);

  /**
   * applyAnd that intersects the [resulting bitmap of intersected indices] and [ALL the SV and MV scan iterators]
   * Following the same manner as in AndDocIdIterator
   * @param docIds resulting bitmap of intersected indices
   * @param valueIters ALL the SV and MV scan iterators in this AND predicate
   * @return intersected docIds
   */
  static MutableRoaringBitmap applyAnd(ImmutableRoaringBitmap docIds, List<ValueBasedScanDocIdIterator> valueIters){
    MutableRoaringBitmap result = new MutableRoaringBitmap();
    PeekableIntIterator docIdIterator = docIds.getIntIterator();
    while (docIdIterator.hasNext()){
      int currentDocId=docIdIterator.peekNext();
      for (int i=0; i<valueIters.size(); i++){
        int next=valueIters.get(i).fastAdvance(docIdIterator);
        if(next == Constants.EOF) return result;
        else if(next > currentDocId){
          currentDocId=next;
          i=-1;
        }
      }
      result.add(currentDocId);
      docIdIterator.next();
    }
    return result;
  }
}
