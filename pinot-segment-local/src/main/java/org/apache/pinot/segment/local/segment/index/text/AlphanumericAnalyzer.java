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
/// This fills a gap between the two analyzers Pinot already offers for log and identifier-heavy
/// text, neither of which tokenizes the way most search engines do:
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
/// On ASCII input this is the behaviour of Elasticsearch's `char_group` tokenizer over whitespace,
/// punctuation and symbols plus `lowercase`, of ClickHouse's `splitByNonAlpha`, and of a
/// `[a-zA-Z0-9]+` pattern tokenizer. It is a good default for logs, URLs, stack traces, and
/// anything carrying status codes, ports, versions or generated identifiers.
///
/// Outside ASCII it is deliberately broader than those: a token character is whatever
/// [Character#isLetterOrDigit(int)] accepts, so accented letters, CJK ideographs and Arabic-Indic
/// digits are kept rather than treated as separators. `Café` yields `café`, where a `[a-zA-Z0-9]+`
/// pattern tokenizer would cut it down to `caf`.
///
/// ## Token length
///
/// A run of token characters longer than [#MAX_TOKEN_LENGTH] is **split** at that boundary — not
/// truncated and not dropped — which is the same limit and the same policy as
/// [org.apache.lucene.analysis.standard.StandardAnalyzer]'s default. Two consequences matter for
/// very long identifiers. The tail of an oversized run becomes a term in its own right, so a
/// 257-character identifier is also matched by a term query for its last two characters. And a
/// prefix, wildcard or fuzzy query longer than [#MAX_TOKEN_LENGTH] can never match, because such a
/// query is normalized as one term and no indexed term is longer than that.
///
/// ## Switching an existing column to this analyzer
///
/// A text index records the analyzer it was built with, and the Lucene reader restores that saved
/// configuration in preference to the current table config. A reload does not rebuild the index
/// either: the text index handler compares only the index storage format, never the analyzer. So
/// after a config-only switch from [org.apache.lucene.analysis.core.SimpleAnalyzer], segments built
/// earlier keep their old tokenization and still miss `200` while newly built segments match it.
/// Rebuild the text indexes or regenerate those segments for the change to take effect everywhere.
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

  /// The longest run of token characters emitted as a single term; a longer run is split at this
  /// boundary rather than truncated. This is the default of the [CharTokenizer] factory below, and
  /// matches [org.apache.lucene.analysis.standard.StandardAnalyzer]'s default token length.
  public static final int MAX_TOKEN_LENGTH = CharTokenizer.DEFAULT_MAX_WORD_LEN;

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

  /// Applies the same case folding to single-token input that [#createComponents(String)] applies
  /// to documents. Lucene calls this for the terms of multi-term queries — wildcard, prefix, range
  /// and fuzzy — which are never tokenized, so without it a query such as `HTTP*` would be matched
  /// against a lowercased index and find nothing.
  @Override
  protected TokenStream normalize(String fieldName, TokenStream in) {
    return _caseSensitive ? in : new LowerCaseFilter(in);
  }
}
