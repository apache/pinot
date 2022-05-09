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
package org.apache.pinot.segment.local.segment.creator.impl.text;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;


/**
 * Parses a given piece of text using a Lucene analyzer.
 *
 * Primarily used for use cases where the Lucene analyzer needs to be reused
 */
public class TermsParser implements Closeable {

  private Analyzer _analyzer;
  private String _columnName;
  private ThreadLocal<TokenStream> _tokenStream = new ThreadLocal<>();
  private ThreadLocal<CharTermAttribute> _token = new ThreadLocal<>();

  public TermsParser(Analyzer analyzer, String columnName) {
    _analyzer = analyzer;
    _columnName = columnName;
  }

  public Iterable<String> parse(String document) {
    try {
      if (_tokenStream.get() != null) {
        _tokenStream.get().close();
      }

      _tokenStream.set(_analyzer.tokenStream(_columnName, document));
      _tokenStream.get().reset();

      if (_token.get() == null) {
        _token.set(_tokenStream.get().addAttribute(CharTermAttribute.class));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    _token.get().setEmpty();

    return () -> new TokenIterator(_tokenStream.get(), _token.get());
  }

  private static class TokenIterator implements Iterator<String> {

    private TokenStream _tokenStream;
    private CharTermAttribute _token;

    public TokenIterator(TokenStream tokenStream, CharTermAttribute token) {
      _tokenStream = tokenStream;
      _token = token;
    }

    @Override
    public boolean hasNext() {
      if (_token.length() == 0) {
        try {
          _tokenStream.incrementToken();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      return _token.length() > 0;
    }

    @Override
    public String next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      String result = new String(_token.buffer(), 0, _token.length());
      _token.setEmpty();
      return result;
    }
  }

  @Override
  public void close() {
    _analyzer.close();
  }
}
