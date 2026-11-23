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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.CharTokenizer;


/// An [Analyzer] that splits on every non-alphanumeric character and, by default, lowercases the
/// result — so `email_server.rb` yields `email`, `server`, `rb`, and `HTTP2` yields `http2`.
///
/// It is useful for logs and identifier-heavy text where both punctuation and digits matter.
///
/// | Analyzer | `send_email` | `200` | `1YMWWN1N4O` |
/// |---|---|---|---|
/// | [org.apache.lucene.analysis.standard.StandardAnalyzer] (default) | `send_email` | `200` | `1ymwwn1n4o` |
/// | [org.apache.lucene.analysis.core.SimpleAnalyzer] | `send`, `email` | *(none)* | `ymwwn`, `n`, `o` |
/// | `AlphanumericAnalyzer` | `send`, `email` | `200` | `1ymwwn1n4o` |
///
/// [org.apache.lucene.analysis.standard.StandardAnalyzer] follows UAX#29, which keeps
/// `send_email` and `email_server.rb` whole, so a search for `email` does not match them.
/// [org.apache.lucene.analysis.core.SimpleAnalyzer] splits on non-letters, but because it is built
/// on [org.apache.lucene.analysis.core.LetterTokenizer] every digit is a *separator* and is
/// dropped: `200` and `8080` produce no terms at all and can never be matched, `http2` is indexed
/// as `http`, and an identifier such as `1YMWWN1N4O` shatters into letter fragments. Pinot applies
/// the configured analyzer to queries as well as to documents, so the term a user searches for is
/// discarded in exactly the same way.
///
/// For ASCII runs that fit within the split threshold, token boundaries follow the pattern
/// `[a-zA-Z0-9]+`. The tokenizer also accepts Unicode code points recognized by
/// [Character#isLetterOrDigit(int)], including accented letters, CJK ideographs and Arabic-Indic
/// digits. For example, `Café` yields `café` in the default lowercase mode.
///
/// ## Token length
///
/// The tokenizer emits a term when it reaches 255 UTF-16 code units, then begins another term if
/// the run continues. It keeps each code point whole, so a supplementary letter that crosses the
/// threshold can produce a term of 256 code units. For example, the last two characters of a
/// 257-character ASCII identifier become a separate searchable term.
///
/// A literal prefix can match only if a single indexed term starts with it. Wildcard operators
/// match patterns within each term, and fuzzy queries can match a shorter term within their
/// permitted edit distance.
///
/// ## Switching an existing single-column text index to this analyzer
///
/// The reader restores the analyzer settings saved in the text index. Changing only the analyzer
/// in the table configuration leaves existing single-column text indexes in place on reload.
/// After switching from [org.apache.lucene.analysis.core.SimpleAnalyzer], newly built segments
/// can match `200` as a term. Rebuild the existing text indexes or regenerate their segments to
/// make the new tokenization available across the table.
///
/// Select it per column with `luceneAnalyzerClass`, optionally preserving case:
///
/// ```json
/// "properties": {
///   "luceneAnalyzerClass": "org.apache.pinot.segment.local.segment.index.text.AlphanumericAnalyzer",
///   "luceneAnalyzerClassArgs": "true",
///   "luceneAnalyzerClassArgTypes": "java.lang.Boolean.TYPE"
/// }
/// ```
///
/// Instances are immutable and safe for concurrent use; per-thread token streams come from
/// [Analyzer]'s reuse strategy.
public class AlphanumericAnalyzer extends Analyzer {

  /// The default [CharTokenizer] split threshold in UTF-16 code units. A supplementary code point
  /// can carry a term one code unit past it.
  static final int TOKEN_SPLIT_THRESHOLD = CharTokenizer.DEFAULT_MAX_WORD_LEN;

  private final boolean _caseSensitive;

  /// Builds a case-insensitive analyzer, matching the default of
  /// [org.apache.lucene.analysis.standard.StandardAnalyzer].
  public AlphanumericAnalyzer() {
    this(false);
  }

  /// Builds an analyzer that preserves the original case when `caseSensitive` is `true`.
  ///
  /// @param caseSensitive whether tokens keep their original case
  public AlphanumericAnalyzer(boolean caseSensitive) {
    _caseSensitive = caseSensitive;
  }

  /// Returns true if this analyzer preserves the original case of its tokens.
  public boolean isCaseSensitive() {
    return _caseSensitive;
  }

  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    Tokenizer tokenizer = CharTokenizer.fromTokenCharPredicate(Character::isLetterOrDigit);
    return new TokenStreamComponents(tokenizer, _caseSensitive ? tokenizer : new LowerCaseFilter(tokenizer));
  }

  /// Applies the same case handling as document analysis to query text that Lucene normalizes
  /// without tokenizing. For example, `HTTP*` can match the indexed term `http2` in lowercase mode.
  @Override
  protected TokenStream normalize(String fieldName, TokenStream in) {
    return _caseSensitive ? in : new LowerCaseFilter(in);
  }
}
