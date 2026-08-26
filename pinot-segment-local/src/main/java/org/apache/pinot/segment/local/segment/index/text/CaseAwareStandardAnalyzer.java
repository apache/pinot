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
package org.apache.pinot.segment.local.segment.index.text;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;


/// A [org.apache.lucene.analysis.Analyzer] for standard text that is case-aware.
/// This analyzer supports both case-sensitive and case-insensitive modes, making it
/// suitable for use cases where case sensitivity is configurable.
///
/// It's directly copied from [org.apache.lucene.analysis.standard.StandardAnalyzer] but
/// allows case-sensitive tokenization.
///
/// The analyzer applies lowercasing to tokens only when the `caseSensitive` flag is set to
/// `false` (the default behavior, same as [org.apache.lucene.analysis.standard.StandardAnalyzer]).
/// When `caseSensitive` is `true`, tokens preserve their original case.
public class CaseAwareStandardAnalyzer extends StopwordAnalyzerBase {

  /// Default maximum allowed token length
  public static final int DEFAULT_MAX_TOKEN_LENGTH = 255;

  private int _maxTokenLength = DEFAULT_MAX_TOKEN_LENGTH;

  private final boolean _caseSensitive;

  /// Builds an analyzer with the given stop words.
  ///
  /// @param stopWords stop words
  public CaseAwareStandardAnalyzer(CharArraySet stopWords) {
    this(stopWords, false);
  }

  /// Builds an analyzer with no stop words.
  public CaseAwareStandardAnalyzer() {
    this(CharArraySet.EMPTY_SET, false);
  }

  /// Builds an analyzer with the given stop words.
  ///
  /// @param stopWords stop words
  public CaseAwareStandardAnalyzer(CharArraySet stopWords, boolean caseSensitive) {
    super(stopWords);
    _caseSensitive = caseSensitive;
  }

  /// Set the max allowed token length. Tokens larger than this will be chopped up at this token
  /// length and emitted as multiple tokens. If you need to skip such large tokens, you could
  /// increase this max length, and then use `LengthFilter` to remove long tokens. The default
  /// is [CaseAwareStandardAnalyzer#DEFAULT_MAX_TOKEN_LENGTH].
  public void setMaxTokenLength(int length) {
    _maxTokenLength = length;
  }

  /// Returns the current maximum token length
  ///
  /// @see #setMaxTokenLength
  public int getMaxTokenLength() {
    return _maxTokenLength;
  }

  /// Returns true if the analyzer is case sensitive
  public boolean isCaseSensitive() {
    return _caseSensitive;
  }

  @Override
  protected TokenStreamComponents createComponents(final String fieldName) {
    final StandardTokenizer tokenizer = new StandardTokenizer();
    tokenizer.setMaxTokenLength(_maxTokenLength);
    TokenStream tok;
    if (_caseSensitive) {
      tok = tokenizer;
    } else {
      tok = new LowerCaseFilter(tokenizer);
    }
    tok = new StopFilter(tok, stopwords);
    return new TokenStreamComponents(
        r -> {
          tokenizer.setMaxTokenLength(_maxTokenLength);
          tokenizer.setReader(r);
        },
        tok);
  }

  @Override
  protected TokenStream normalize(String fieldName, TokenStream in) {
    if (_caseSensitive) {
      return in;
    }
    return new LowerCaseFilter(in);
  }
}
