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
package org.apache.pinot.segment.local.segment.index.text.lucene.parsers;

import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.Token;
import org.apache.lucene.queryparser.complexPhrase.ComplexPhraseQueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.pinot.segment.local.utils.LuceneRegexpAutomaton;


/// Classic Lucene query parser with a limit on regular expression construction.
/// Like Lucene's parser, each instance should be used by one thread.
public final class BoundedRegexpQueryParser extends QueryParser {
  private final MultiTermClauses _multiTermClauses = new MultiTermClauses();

  public BoundedRegexpQueryParser(String field, Analyzer analyzer) {
    super(field, analyzer);
  }

  @Override
  protected Query newRegexpQuery(Term term) {
    LuceneRegexpAutomaton.validateConstructionBudget(term.text());
    return super.newRegexpQuery(term);
  }

  @Override
  protected Query getFieldQuery(String field, String text, boolean quoted)
      throws ParseException {
    Query query = super.getFieldQuery(field, text, quoted);
    _multiTermClauses.track(this, text, quoted, query);
    return query;
  }

  @Override
  protected void addClause(List<BooleanClause> clauses, int conjunction, int modifier, Query query) {
    if (!_multiTermClauses.add(clauses, conjunction, modifier, query)) {
      super.addClause(clauses, conjunction, modifier, query);
    }
  }

  /// Complex phrase parser with the same construction limit as the classic parser.
  /// Each instance should be used by one thread.
  public static final class ComplexPhrase extends ComplexPhraseQueryParser {
    private final MultiTermClauses _multiTermClauses = new MultiTermClauses();
    private int _parseDepth;

    public ComplexPhrase(String field, Analyzer analyzer) {
      super(field, analyzer);
    }

    @Override
    public Query parse(String expression)
        throws ParseException {
      _parseDepth++;
      try {
        return super.parse(expression);
      } finally {
        _parseDepth--;
      }
    }

    @Override
    protected Query getFieldQuery(String field, String text, boolean quoted)
        throws ParseException {
      Query query = super.getFieldQuery(field, text, quoted);
      // Lucene re-enters parse() for the contents of each quoted phrase. Only those clauses are rewritten as spans.
      if (_parseDepth > 1) {
        _multiTermClauses.track(this, text, quoted, query);
      }
      return query;
    }

    @Override
    protected void addClause(List<BooleanClause> clauses, int conjunction, int modifier, Query query) {
      if (!_multiTermClauses.add(clauses, conjunction, modifier, query)) {
        super.addClause(clauses, conjunction, modifier, query);
      }
    }

    @Override
    protected Query newRegexpQuery(Term term) {
      LuceneRegexpAutomaton.validateConstructionBudget(term.text());
      return super.newRegexpQuery(term);
    }
  }

  /// Restores Lucene 9's flattening of adjacent plain terms without splitting analyzer input.
  private static final class MultiTermClauses {
    private static final int NO_CONJUNCTION = 0;
    private static final int NO_MODIFIER = 0;

    private BooleanQuery _group;

    private void track(QueryParser parser, String text, boolean quoted, Query query) {
      _group = null;
      if (quoted || parser.getSplitOnWhitespace() || !(query instanceof BooleanQuery group)) {
        return;
      }
      // MultiTerm analyzes a string longer than the last token it consumed. Compare decoded lengths so
      // escaped whitespace inside an individual term does not turn into separate clauses.
      Token token = parser.getToken(0);
      if (token.kind == TERM && text.length() > decodedLength(token.image)
          && group.clauses().stream().allMatch(clause -> clause.query() instanceof TermQuery)) {
        _group = group;
      }
    }

    private boolean add(List<BooleanClause> clauses, int conjunction, int modifier, Query query) {
      BooleanQuery group = _group;
      _group = null;
      // MultiTerm immediately adds the same query returned by analysis. An explicit parenthesized group
      // is assembled afterward and does not carry this identity.
      if (group == null || query != group || conjunction != NO_CONJUNCTION || modifier != NO_MODIFIER) {
        return false;
      }
      clauses.addAll(group.clauses());
      return true;
    }

    private static int decodedLength(String token) {
      int length = 0;
      int position = 0;
      while (position < token.length()) {
        char character = token.charAt(position++);
        if (character == '\\' && position < token.length() && token.charAt(position++) == 'u') {
          position += 4;
        }
        length++;
      }
      return length;
    }
  }
}
