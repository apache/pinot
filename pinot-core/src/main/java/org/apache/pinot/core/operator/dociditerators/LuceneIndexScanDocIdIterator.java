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

import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;
import org.apache.pinot.core.common.Constants;
import org.apache.pinot.core.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.core.segment.index.readers.LuceneTextIndexReader;
import org.apache.pinot.core.segment.index.readers.TextIndexReader;


public class LuceneIndexScanDocIdIterator implements IndexBasedDocIdIterator {

  private int _currentDocId = -1;
  private int _currentScoreDocIndex = 0;
  private int _startDocId;
  private int _endDocId;
  private final ScoreDoc[] _scoreDocs;
  private final LuceneTextIndexReader _textIndexReader;

  public LuceneIndexScanDocIdIterator(ScoreDoc[] scoreDocs, TextIndexReader textIndexReader) {
    _scoreDocs = scoreDocs;
    _textIndexReader = (LuceneTextIndexReader)textIndexReader;
  }

  public void setStartDocId(int startDocId) {
    _startDocId = startDocId;
  }

  public void setEndDocId(int endDocId) {
    _endDocId = endDocId;
  }

  @Override
  public int next() {
    if (_currentDocId == Constants.EOF || _currentScoreDocIndex >= _scoreDocs.length) {
      return Constants.EOF;
    }

    // The text match filter operator would have already searched
    // the lucene index to retrieve a set of matching documents.
    // All such documents are in ScoreDoc array.
    // We now get the next document from that array and get it's
    // document ID
    // Advance to startDocId if necessary

    int luceneDocId = _scoreDocs[_currentScoreDocIndex++].doc;
    Document document = _textIndexReader.getDocument(luceneDocId);
    _currentDocId = Integer.valueOf(document.get(LuceneTextIndexCreator.DOC_ID_COLUMN_NAME));

    return _currentDocId;
  }

  @Override
  public int advance(int targetDocId) {
    if (targetDocId <= _currentDocId) {
     return _currentDocId;
    }

    int curr = next();
    while(curr < targetDocId && curr != Constants.EOF) {
      curr = next();
    }
    return curr;
  }

  @Override
  public int currentDocId() {
    return _currentDocId;
  }
}
