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
package org.apache.pinot.segment.spi.index.reader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.JsonUtils;


/// Corpus statistics a relevance similarity (BM25) needs for one search query, collected from one or more text
/// indexes: per index field the collection statistics (`maxDoc`, `docCount`, `sumTotalTermFreq`, `sumDocFreq`)
/// and, for every term of the analyzed query, its document frequency and total term frequency.
///
/// Statistics from different segments (or servers) are combined with [#merge], which sums every counter. Scoring
/// each segment with the merged statistics instead of the segment's own makes `TEXT_SCORE` values comparable across
/// segments, as if all documents were in one index (the approach of Elasticsearch's `dfs_query_then_fetch`).
///
/// The serialized form ([#toJson] / [#fromJson]) is compact JSON:
/// ```
/// {"fields":{"title":{"maxDoc":10,"docCount":10,"sumTotalTermFreq":42,"sumDocFreq":37,"terms":{"usb":[4,6]}}}}
/// ```
/// where each term maps to `[docFreq, totalTermFreq]`. Field and term names are kept in sorted order so equal
/// statistics serialize identically.
///
/// Not thread-safe: [#merge] mutates this instance.
public final class TextScoreStats {
  private static final String FIELDS = "fields";
  private static final String MAX_DOC = "maxDoc";
  private static final String DOC_COUNT = "docCount";
  private static final String SUM_TOTAL_TERM_FREQ = "sumTotalTermFreq";
  private static final String SUM_DOC_FREQ = "sumDocFreq";
  private static final String TERMS = "terms";

  private final Map<String, FieldStats> _fields = new TreeMap<>();

  /// Returns the statistics of `field`, creating empty ones if absent.
  public FieldStats getOrCreateField(String field) {
    return _fields.computeIfAbsent(field, k -> new FieldStats());
  }

  @Nullable
  public FieldStats getField(String field) {
    return _fields.get(field);
  }

  public Map<String, FieldStats> getFields() {
    return _fields;
  }

  /// Adds every counter of `other` into this instance and returns this instance.
  public TextScoreStats merge(TextScoreStats other) {
    for (Map.Entry<String, FieldStats> entry : other._fields.entrySet()) {
      getOrCreateField(entry.getKey()).merge(entry.getValue());
    }
    return this;
  }

  public String toJson() {
    ObjectNode fieldsNode = JsonUtils.newObjectNode();
    for (Map.Entry<String, FieldStats> entry : _fields.entrySet()) {
      FieldStats fieldStats = entry.getValue();
      ObjectNode fieldNode = fieldsNode.putObject(entry.getKey());
      fieldNode.put(MAX_DOC, fieldStats._maxDoc);
      fieldNode.put(DOC_COUNT, fieldStats._docCount);
      fieldNode.put(SUM_TOTAL_TERM_FREQ, fieldStats._sumTotalTermFreq);
      fieldNode.put(SUM_DOC_FREQ, fieldStats._sumDocFreq);
      ObjectNode termsNode = fieldNode.putObject(TERMS);
      for (Map.Entry<String, long[]> term : fieldStats._terms.entrySet()) {
        ArrayNode termNode = termsNode.putArray(term.getKey());
        termNode.add(term.getValue()[0]);
        termNode.add(term.getValue()[1]);
      }
    }
    ObjectNode root = JsonUtils.newObjectNode();
    root.set(FIELDS, fieldsNode);
    return root.toString();
  }

  /// Parses the output of [#toJson].
  ///
  /// @throws IllegalArgumentException if the string is not valid serialized statistics
  public static TextScoreStats fromJson(String json) {
    JsonNode root;
    try {
      root = JsonUtils.stringToJsonNode(json);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid text score statistics, expected the output of TEXT_SCORE_STATS: "
          + json, e);
    }
    JsonNode fieldsNode = root.get(FIELDS);
    if (fieldsNode == null || !fieldsNode.isObject()) {
      throw new IllegalArgumentException(
          "Invalid text score statistics, missing '" + FIELDS + "' object: " + json);
    }
    TextScoreStats stats = new TextScoreStats();
    Iterator<Map.Entry<String, JsonNode>> fields = fieldsNode.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      JsonNode fieldNode = field.getValue();
      FieldStats fieldStats = stats.getOrCreateField(field.getKey());
      fieldStats.addCollectionStats(getLong(fieldNode, MAX_DOC), getLong(fieldNode, DOC_COUNT),
          getLong(fieldNode, SUM_TOTAL_TERM_FREQ), getLong(fieldNode, SUM_DOC_FREQ));
      JsonNode termsNode = fieldNode.get(TERMS);
      if (termsNode != null) {
        Iterator<Map.Entry<String, JsonNode>> terms = termsNode.fields();
        while (terms.hasNext()) {
          Map.Entry<String, JsonNode> term = terms.next();
          JsonNode value = term.getValue();
          if (!value.isArray() || value.size() != 2) {
            throw new IllegalArgumentException(
                "Invalid text score statistics, term '" + term.getKey() + "' must map to [docFreq, totalTermFreq]");
          }
          fieldStats.addTermStats(term.getKey(), value.get(0).asLong(), value.get(1).asLong());
        }
      }
    }
    return stats;
  }

  private static long getLong(JsonNode node, String key) {
    JsonNode value = node.get(key);
    return value != null ? value.asLong() : 0;
  }

  @Override
  public boolean equals(Object o) {
    return this == o || (o instanceof TextScoreStats && _fields.equals(((TextScoreStats) o)._fields));
  }

  @Override
  public int hashCode() {
    return _fields.hashCode();
  }

  @Override
  public String toString() {
    return toJson();
  }

  /// Statistics of one index field. Counters follow Lucene's `CollectionStatistics` / `TermStatistics`.
  public static final class FieldStats {
    private long _maxDoc;
    private long _docCount;
    private long _sumTotalTermFreq;
    private long _sumDocFreq;
    /// term -> [docFreq, totalTermFreq]
    private final Map<String, long[]> _terms = new TreeMap<>();

    public void addCollectionStats(long maxDoc, long docCount, long sumTotalTermFreq, long sumDocFreq) {
      _maxDoc += maxDoc;
      _docCount += docCount;
      _sumTotalTermFreq += sumTotalTermFreq;
      _sumDocFreq += sumDocFreq;
    }

    public void addTermStats(String term, long docFreq, long totalTermFreq) {
      long[] counts = _terms.computeIfAbsent(term, k -> new long[2]);
      counts[0] += docFreq;
      counts[1] += totalTermFreq;
    }

    void merge(FieldStats other) {
      addCollectionStats(other._maxDoc, other._docCount, other._sumTotalTermFreq, other._sumDocFreq);
      for (Map.Entry<String, long[]> term : other._terms.entrySet()) {
        addTermStats(term.getKey(), term.getValue()[0], term.getValue()[1]);
      }
    }

    public long getMaxDoc() {
      return _maxDoc;
    }

    public long getDocCount() {
      return _docCount;
    }

    public long getSumTotalTermFreq() {
      return _sumTotalTermFreq;
    }

    public long getSumDocFreq() {
      return _sumDocFreq;
    }

    /// Returns `[docFreq, totalTermFreq]` of `term`, or `null` when the statistics do not cover it.
    @Nullable
    public long[] getTermStats(String term) {
      return _terms.get(term);
    }

    public Map<String, long[]> getTerms() {
      return _terms;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof FieldStats)) {
        return false;
      }
      FieldStats that = (FieldStats) o;
      if (_maxDoc != that._maxDoc || _docCount != that._docCount || _sumTotalTermFreq != that._sumTotalTermFreq
          || _sumDocFreq != that._sumDocFreq || !_terms.keySet().equals(that._terms.keySet())) {
        return false;
      }
      for (Map.Entry<String, long[]> term : _terms.entrySet()) {
        long[] otherCounts = that._terms.get(term.getKey());
        if (term.getValue()[0] != otherCounts[0] || term.getValue()[1] != otherCounts[1]) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_maxDoc, _docCount, _sumTotalTermFreq, _sumDocFreq, _terms.keySet());
    }
  }
}
