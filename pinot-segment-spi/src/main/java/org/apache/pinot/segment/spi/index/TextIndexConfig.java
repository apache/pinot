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

package org.apache.pinot.segment.spi.index;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.segment.spi.utils.CsvParser;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.utils.JsonUtils;


public class TextIndexConfig extends IndexConfig {
  private static final int LUCENE_INDEX_DEFAULT_MAX_BUFFER_SIZE_MB = 500;
  private static final boolean LUCENE_INDEX_DEFAULT_USE_COMPOUND_FILE = true;
  private static final boolean LUCENE_INDEX_ENABLE_PREFIX_SUFFIX_MATCH_IN_PHRASE_SEARCH = false;
  private static final boolean LUCENE_INDEX_REUSE_MUTABLE_INDEX = false;
  private static final int LUCENE_INDEX_NRT_CACHING_DIRECTORY_MAX_BUFFER_SIZE_MB = 0;
  private static final boolean LUCENE_INDEX_DEFAULT_USE_AND_FOR_MULTI_TERM_QUERIES = false;
  private static final boolean LUCENE_USE_LOG_BYTE_SIZE_MERGE_POLICY = false;
  private static final DocIdTranslatorMode LUCENE_TRANSLATOR_MODE = null;
  private static final boolean LUCENE_INDEX_DEFAULT_CASE_SENSITIVE_INDEX = false;
  private static final boolean LUCENE_INDEX_DEFAULT_STORE_IN_SEGMENT_FILE = false;

  // keep in sync with constructor!
  private static final List<String> PROPERTY_NAMES = List.of(
      "disabled", "rawValue", "queryCache", "useANDForMultiTermQueries", "stopWordsInclude", "stopWordsExclude",
      "luceneUseCompoundFile", "luceneMaxBufferSizeMB", "luceneAnalyzerClass", "luceneAnalyzerClassArgs",
      "luceneAnalyzerClassArgTypes", "luceneQueryParserClass", "enablePrefixSuffixMatchingInPhraseQueries",
      "reuseMutableIndex", "luceneNRTCachingDirectoryMaxBufferSizeMB", "useLogByteSizeMergePolicy",
      "docIdTranslatorMode", "caseSensitive", "storeInSegmentFile"
  );

  public static final TextIndexConfig DISABLED =
      new TextIndexConfig(true, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null);

  @Nullable
  private final Object _rawValueForTextIndex;
  private final Boolean _enableQueryCache;
  private final Boolean _useANDForMultiTermQueries;
  private final List<String> _stopWordsInclude;
  private final List<String> _stopWordsExclude;
  private final Boolean _luceneUseCompoundFile;
  private final Integer _luceneMaxBufferSizeMB;
  private final String _luceneAnalyzerClass;
  private final List<String> _luceneAnalyzerClassArgs;
  private final List<String> _luceneAnalyzerClassArgTypes;
  private final String _luceneQueryParserClass;
  private final Boolean _enablePrefixSuffixMatchingInPhraseQueries;
  private final Boolean _reuseMutableIndex;
  private final Integer _luceneNRTCachingDirectoryMaxBufferSizeMB;
  private final Boolean _useLogByteSizeMergePolicy;
  private final DocIdTranslatorMode _docIdTranslatorMode;
  private final Boolean _caseSensitive;
  private final Boolean _storeInSegmentFile;

  public enum DocIdTranslatorMode {
    // build and keep mapping
    Default,
    // build mapping and check if it can be dropped
    TryOptimize,
    // mapping is not necessary, use lucene ids
    Skip;

    static DocIdTranslatorMode of(String mode) {
      if (mode == null) {
        return null;
      }
      for (DocIdTranslatorMode value : values()) {
        if (value.name().equalsIgnoreCase(mode)) {
          return value;
        }
      }

      return null;
    }
  }

  public TextIndexConfig(Boolean disabled, Object rawValueForTextIndex, Boolean enableQueryCache,
      Boolean useANDForMultiTermQueries, List<String> stopWordsInclude, List<String> stopWordsExclude,
      Boolean luceneUseCompoundFile, Integer luceneMaxBufferSizeMB, String luceneAnalyzerClass,
      String luceneAnalyzerClassArgs, String luceneAnalyzerClassArgTypes, String luceneQueryParserClass,
      Boolean enablePrefixSuffixMatchingInPhraseQueries, Boolean reuseMutableIndex,
      Integer luceneNRTCachingDirectoryMaxBufferSizeMB, Boolean useLogByteSizeMergePolicy,
      DocIdTranslatorMode docIdTranslatorMode) {
    this(disabled, rawValueForTextIndex, enableQueryCache, useANDForMultiTermQueries,
        stopWordsInclude, stopWordsExclude, luceneUseCompoundFile, luceneMaxBufferSizeMB, luceneAnalyzerClass,
        luceneAnalyzerClassArgs, luceneAnalyzerClassArgTypes, luceneQueryParserClass,
        enablePrefixSuffixMatchingInPhraseQueries, reuseMutableIndex,
        luceneNRTCachingDirectoryMaxBufferSizeMB, useLogByteSizeMergePolicy, docIdTranslatorMode, null, null);
  }

  /// Binary-compat delegate for the pre-wrapper 17-arg primitive ctor (positions 3 + 4 were `boolean`
  /// pre-PR). Kept so existing compiled call sites continue to link.
  @Deprecated
  public TextIndexConfig(Boolean disabled, Object rawValueForTextIndex, boolean enableQueryCache,
      boolean useANDForMultiTermQueries, List<String> stopWordsInclude, List<String> stopWordsExclude,
      Boolean luceneUseCompoundFile, Integer luceneMaxBufferSizeMB, String luceneAnalyzerClass,
      String luceneAnalyzerClassArgs, String luceneAnalyzerClassArgTypes, String luceneQueryParserClass,
      Boolean enablePrefixSuffixMatchingInPhraseQueries, Boolean reuseMutableIndex,
      Integer luceneNRTCachingDirectoryMaxBufferSizeMB, Boolean useLogByteSizeMergePolicy,
      DocIdTranslatorMode docIdTranslatorMode) {
    this(disabled, rawValueForTextIndex, Boolean.valueOf(enableQueryCache),
        Boolean.valueOf(useANDForMultiTermQueries), stopWordsInclude, stopWordsExclude, luceneUseCompoundFile,
        luceneMaxBufferSizeMB, luceneAnalyzerClass, (Object) luceneAnalyzerClassArgs,
        (Object) luceneAnalyzerClassArgTypes, luceneQueryParserClass, enablePrefixSuffixMatchingInPhraseQueries,
        reuseMutableIndex, luceneNRTCachingDirectoryMaxBufferSizeMB, useLogByteSizeMergePolicy,
        docIdTranslatorMode, (Boolean) null, (Boolean) null);
  }

  /// Binary-compat delegate for the pre-wrapper 19-arg primitive ctor (positions 3 + 4 were `boolean`
  /// pre-PR). Kept so existing compiled call sites continue to link.
  @Deprecated
  public TextIndexConfig(Boolean disabled, Object rawValueForTextIndex, boolean enableQueryCache,
      boolean useANDForMultiTermQueries, List<String> stopWordsInclude, List<String> stopWordsExclude,
      Boolean luceneUseCompoundFile, Integer luceneMaxBufferSizeMB, String luceneAnalyzerClass,
      Object luceneAnalyzerClassArgs, Object luceneAnalyzerClassArgTypes, String luceneQueryParserClass,
      Boolean enablePrefixSuffixMatchingInPhraseQueries, Boolean reuseMutableIndex,
      Integer luceneNRTCachingDirectoryMaxBufferSizeMB, Boolean useLogByteSizeMergePolicy,
      DocIdTranslatorMode docIdTranslatorMode, Boolean caseSensitive, Boolean storeInSegmentFile) {
    this(disabled, rawValueForTextIndex, Boolean.valueOf(enableQueryCache),
        Boolean.valueOf(useANDForMultiTermQueries), stopWordsInclude, stopWordsExclude, luceneUseCompoundFile,
        luceneMaxBufferSizeMB, luceneAnalyzerClass, luceneAnalyzerClassArgs, luceneAnalyzerClassArgTypes,
        luceneQueryParserClass, enablePrefixSuffixMatchingInPhraseQueries, reuseMutableIndex,
        luceneNRTCachingDirectoryMaxBufferSizeMB, useLogByteSizeMergePolicy, docIdTranslatorMode,
        caseSensitive, storeInSegmentFile);
  }

  /// @deprecated Use the new constructor with storeInSegmentFile parameter instead.
  /// This constructor will be removed in a future version.
  @Deprecated
  public TextIndexConfig(Boolean luceneUseCompoundFile, Object rawValue,
                        boolean noRawData, boolean enableQueryCache, List<String> stopWordsInclude,
                        List<String> stopWordsExclude, Boolean useAndForMultiTermQueries,
                        Integer maxResultCacheSize, String stopWordsIncludeKey, String stopWordsExcludeKey,
                        String useAndForMultiTermQueriesKey, String maxResultCacheSizeKey,
                        Boolean enablePrefixSuffixMatching, Boolean enablePrefixSuffixPhraseMatching,
                        Integer maxResultCacheSizeKeyInt, Boolean storeInSegmentFile,
                        DocIdTranslatorMode docIdTranslatorMode, Boolean enablePrefixSuffixMatchingKey) {
    // Forward to wrapper @JsonCreator. Explicit Boolean.valueOf calls disambiguate from the deprecated
    // primitive 19-arg ctor.
    this(storeInSegmentFile, rawValue, Boolean.valueOf(enableQueryCache),
         Boolean.valueOf(useAndForMultiTermQueries != null ? useAndForMultiTermQueries : false),
         stopWordsInclude != null ? stopWordsInclude : new ArrayList<>(),
         stopWordsExclude != null ? stopWordsExclude : new ArrayList<>(),
         luceneUseCompoundFile, maxResultCacheSize, null, (Object) null, (Object) null, null,
         Boolean.valueOf(enablePrefixSuffixPhraseMatching != null ? enablePrefixSuffixPhraseMatching : false),
         Boolean.FALSE, Integer.valueOf(0), Boolean.FALSE, docIdTranslatorMode,
         Boolean.valueOf(enablePrefixSuffixMatchingKey != null ? enablePrefixSuffixMatchingKey : false),
         Boolean.valueOf(storeInSegmentFile != null ? storeInSegmentFile : false));
  }

  @JsonCreator
  public TextIndexConfig(@JsonProperty("disabled") Boolean disabled,
      @JsonProperty("rawValue") @Nullable Object rawValueForTextIndex,
      @JsonProperty("queryCache") Boolean enableQueryCache,
      @JsonProperty("useANDForMultiTermQueries") Boolean useANDForMultiTermQueries,
      @JsonProperty("stopWordsInclude") List<String> stopWordsInclude,
      @JsonProperty("stopWordsExclude") List<String> stopWordsExclude,
      @JsonProperty("luceneUseCompoundFile") Boolean luceneUseCompoundFile,
      @JsonProperty("luceneMaxBufferSizeMB") Integer luceneMaxBufferSizeMB,
      @JsonProperty("luceneAnalyzerClass") String luceneAnalyzerClass,
      @JsonProperty("luceneAnalyzerClassArgs") Object luceneAnalyzerClassArgs,
      @JsonProperty("luceneAnalyzerClassArgTypes") Object luceneAnalyzerClassArgTypes,
      @JsonProperty("luceneQueryParserClass") String luceneQueryParserClass,
      @JsonProperty("enablePrefixSuffixMatchingInPhraseQueries") Boolean enablePrefixSuffixMatchingInPhraseQueries,
      @JsonProperty("reuseMutableIndex") Boolean reuseMutableIndex,
      @JsonProperty("luceneNRTCachingDirectoryMaxBufferSizeMB") Integer luceneNRTCachingDirectoryMaxBufferSizeMB,
      @JsonProperty("useLogByteSizeMergePolicy") Boolean useLogByteSizeMergePolicy,
      @JsonProperty("docIdTranslatorMode") DocIdTranslatorMode docIdTranslatorMode,
      @JsonProperty("caseSensitive") Boolean caseSensitive,
      @JsonProperty("storeInSegmentFile") Boolean storeInSegmentFile) {
    super(disabled);
    _rawValueForTextIndex = rawValueForTextIndex;
    _enableQueryCache = enableQueryCache;
    _useANDForMultiTermQueries = useANDForMultiTermQueries;
    _stopWordsInclude = stopWordsInclude;
    _stopWordsExclude = stopWordsExclude;
    _luceneUseCompoundFile = luceneUseCompoundFile;
    _luceneMaxBufferSizeMB = luceneMaxBufferSizeMB;
    // Empty string is normalized to null so the getter applies the static default and the slim form omits the key.
    _luceneAnalyzerClass = (luceneAnalyzerClass == null || luceneAnalyzerClass.isEmpty()) ? null : luceneAnalyzerClass;

    // Note that we cannot depend on jackson's default behavior to automatically coerce the comma delimited args to
    // List<String>. This is because the args may contain comma and other special characters such as space. Therefore,
    // we use our own csv parser to parse the values directly when the input is a String.
    // However, when round-tripping (serializing then deserializing), Jackson may produce a List<String> directly,
    // so we also handle that case.
    _luceneAnalyzerClassArgs = parseToList(luceneAnalyzerClassArgs, true, false);
    _luceneAnalyzerClassArgTypes = parseToList(luceneAnalyzerClassArgTypes, false, true);
    _luceneQueryParserClass = luceneQueryParserClass;
    _enablePrefixSuffixMatchingInPhraseQueries = enablePrefixSuffixMatchingInPhraseQueries;
    _reuseMutableIndex = reuseMutableIndex;
    _luceneNRTCachingDirectoryMaxBufferSizeMB = luceneNRTCachingDirectoryMaxBufferSizeMB;
    _useLogByteSizeMergePolicy = useLogByteSizeMergePolicy;
    _docIdTranslatorMode = docIdTranslatorMode;
    _caseSensitive = caseSensitive;
    _storeInSegmentFile = storeInSegmentFile;
  }

  /// Parses the input value to a `List<String>`. Handles both `String` (CSV format) and `List<String>` (from JSON
  /// array) inputs. Round-trip support: the getter returns `List<String>` which Jackson serializes as a JSON array,
  /// but the original input format was CSV string.
  ///
  /// @param value the input value (can be `String`, `List`, or `null`)
  /// @param escapeComma if `true`, don't split on escaped commas when parsing `String`
  /// @param trim whether to trim each value
  /// @return parsed list of strings, empty list if input is `null` or empty
  @SuppressWarnings("unchecked")
  private static List<String> parseToList(final @Nullable Object value, final boolean escapeComma, final boolean trim) {
    if (value == null) {
      return Collections.emptyList();
    }
    if (value instanceof List) {
      final List<?> list = (List<?>) value;
      if (list.isEmpty()) {
        return Collections.emptyList();
      }
      // Convert each element to String and optionally trim
      final List<String> result = new ArrayList<>();
      for (final Object item : list) {
        final String strItem = item == null ? "" : item.toString();
        result.add(trim ? strItem.trim() : strItem);
      }
      return result;
    }
    // String or other types - use CSV parser
    return CsvParser.parse(value.toString(), escapeComma, trim);
  }

  @Nullable
  public Object getRawValueForTextIndex() {
    return _rawValueForTextIndex;
  }

  /// Whether Lucene query result cache should be enabled.
  ///
  /// While it helps a lot with performance for repeated queries, on the downside it causes heap issues.
  public boolean isEnableQueryCache() {
    return Boolean.TRUE.equals(_enableQueryCache);
  }

  public boolean isUseANDForMultiTermQueries() {
    return _useANDForMultiTermQueries != null ? _useANDForMultiTermQueries
        : LUCENE_INDEX_DEFAULT_USE_AND_FOR_MULTI_TERM_QUERIES;
  }

  public List<String> getStopWordsInclude() {
    return _stopWordsInclude;
  }

  public List<String> getStopWordsExclude() {
    return _stopWordsExclude;
  }

  /// Whether Lucene IndexWriter uses compound file format. Improves indexing speed but may cause file descriptor
  /// issues.
  public boolean isLuceneUseCompoundFile() {
    return _luceneUseCompoundFile != null ? _luceneUseCompoundFile : LUCENE_INDEX_DEFAULT_USE_COMPOUND_FILE;
  }

  /// Lucene buffer size. Helps with indexing speed but may cause heap issues.
  public int getLuceneMaxBufferSizeMB() {
    return _luceneMaxBufferSizeMB != null ? _luceneMaxBufferSizeMB : LUCENE_INDEX_DEFAULT_MAX_BUFFER_SIZE_MB;
  }

  /// Lucene analyzer fully qualified class name specifying which analyzer class to use for indexing.
  public String getLuceneAnalyzerClass() {
    return _luceneAnalyzerClass != null ? _luceneAnalyzerClass : FieldConfig.TEXT_INDEX_DEFAULT_LUCENE_ANALYZER_CLASS;
  }

  /// Lucene analyzer arguments in String type. At runtime, the string representation are best-effort coerced into the
  /// proper type with the fully-qualified value type specified in luceneAnalyzerClassArgTypes.
  public List<String> getLuceneAnalyzerClassArgs() {
    return _luceneAnalyzerClassArgs;
  }

  /// Lucene analyzer fully qualified argument value types for each argument. At runtime, the values specified in the
  /// luceneAnalyserClassArgs (string representation) are best-effort coerced into the specified value type.
  public List<String> getLuceneAnalyzerClassArgTypes() {
    return _luceneAnalyzerClassArgTypes;
  }

  /// Lucene query parser fully qualified class name specifying which lucene query parser class to use for query
  /// parsing.
  public String getLuceneQueryParserClass() {
    return _luceneQueryParserClass != null ? _luceneQueryParserClass
        : FieldConfig.TEXT_INDEX_DEFAULT_LUCENE_QUERY_PARSER_CLASS;
  }

  /// Whether to enable prefix and suffix wildcard term matching (i.e., `.*value` for prefix and `value.*` for suffix
  /// term matching) in a phrase query. By default, Pinot today treats `.*` in a phrase query like
  /// `".*value str1 value.*"` as literal. If this flag is enabled, `.*value` will be treated as suffix matching and
  /// `value.*` will be treated as prefix matching.
  public boolean isEnablePrefixSuffixMatchingInPhraseQueries() {
    return _enablePrefixSuffixMatchingInPhraseQueries != null ? _enablePrefixSuffixMatchingInPhraseQueries
        : LUCENE_INDEX_ENABLE_PREFIX_SUFFIX_MATCH_IN_PHRASE_SEARCH;
  }

  public boolean isReuseMutableIndex() {
    return _reuseMutableIndex != null ? _reuseMutableIndex : LUCENE_INDEX_REUSE_MUTABLE_INDEX;
  }

  public boolean isUseLogByteSizeMergePolicy() {
    return _useLogByteSizeMergePolicy != null ? _useLogByteSizeMergePolicy : LUCENE_USE_LOG_BYTE_SIZE_MERGE_POLICY;
  }

  public DocIdTranslatorMode getDocIdTranslatorMode() {
    return _docIdTranslatorMode != null ? _docIdTranslatorMode : LUCENE_TRANSLATOR_MODE;
  }

  public int getLuceneNRTCachingDirectoryMaxBufferSizeMB() {
    return _luceneNRTCachingDirectoryMaxBufferSizeMB != null ? _luceneNRTCachingDirectoryMaxBufferSizeMB
        : LUCENE_INDEX_NRT_CACHING_DIRECTORY_MAX_BUFFER_SIZE_MB;
  }

  public boolean isCaseSensitive() {
    return _caseSensitive != null ? _caseSensitive : LUCENE_INDEX_DEFAULT_CASE_SENSITIVE_INDEX;
  }

  /// Whether to store text index in segment file and cleanup the directory structure.
  /// @return true if text index should be stored in segment file and directory cleaned up,
  ///         false to keep directory structure.
  public boolean isStoreInSegmentFile() {
    return _storeInSegmentFile != null ? _storeInSegmentFile : LUCENE_INDEX_DEFAULT_STORE_IN_SEGMENT_FILE;
  }

  /// Curated slim serializer. See [IndexConfig#toJsonObject()] for the rationale.
  ///
  /// Each field is emitted only when it was explicitly configured (non-null). Empty lists for `stopWords*` and
  /// `luceneAnalyzerClassArgs*` are also treated as not configured and omitted.
  @Override
  @JsonValue
  public ObjectNode toJsonObject() {
    ObjectNode node = super.toJsonObject();
    if (_rawValueForTextIndex != null) {
      node.set("rawValue", JsonUtils.objectToJsonNode(_rawValueForTextIndex));
    }
    if (_enableQueryCache != null) {
      node.put("queryCache", _enableQueryCache);
    }
    if (_useANDForMultiTermQueries != null) {
      node.put("useANDForMultiTermQueries", _useANDForMultiTermQueries);
    }
    if (CollectionUtils.isNotEmpty(_stopWordsInclude)) {
      node.set("stopWordsInclude", JsonUtils.objectToJsonNode(_stopWordsInclude));
    }
    if (CollectionUtils.isNotEmpty(_stopWordsExclude)) {
      node.set("stopWordsExclude", JsonUtils.objectToJsonNode(_stopWordsExclude));
    }
    if (_luceneUseCompoundFile != null) {
      node.put("luceneUseCompoundFile", _luceneUseCompoundFile);
    }
    if (_luceneMaxBufferSizeMB != null) {
      node.put("luceneMaxBufferSizeMB", _luceneMaxBufferSizeMB);
    }
    if (_luceneAnalyzerClass != null) {
      node.put("luceneAnalyzerClass", _luceneAnalyzerClass);
    }
    if (CollectionUtils.isNotEmpty(_luceneAnalyzerClassArgs)) {
      node.set("luceneAnalyzerClassArgs", JsonUtils.objectToJsonNode(_luceneAnalyzerClassArgs));
    }
    if (CollectionUtils.isNotEmpty(_luceneAnalyzerClassArgTypes)) {
      node.set("luceneAnalyzerClassArgTypes", JsonUtils.objectToJsonNode(_luceneAnalyzerClassArgTypes));
    }
    if (_luceneQueryParserClass != null) {
      node.put("luceneQueryParserClass", _luceneQueryParserClass);
    }
    if (_enablePrefixSuffixMatchingInPhraseQueries != null) {
      node.put("enablePrefixSuffixMatchingInPhraseQueries", _enablePrefixSuffixMatchingInPhraseQueries);
    }
    if (_reuseMutableIndex != null) {
      node.put("reuseMutableIndex", _reuseMutableIndex);
    }
    if (_luceneNRTCachingDirectoryMaxBufferSizeMB != null) {
      node.put("luceneNRTCachingDirectoryMaxBufferSizeMB", _luceneNRTCachingDirectoryMaxBufferSizeMB);
    }
    if (_useLogByteSizeMergePolicy != null) {
      node.put("useLogByteSizeMergePolicy", _useLogByteSizeMergePolicy);
    }
    if (_docIdTranslatorMode != null) {
      node.put("docIdTranslatorMode", _docIdTranslatorMode.name());
    }
    if (_caseSensitive != null) {
      node.put("caseSensitive", _caseSensitive);
    }
    if (_storeInSegmentFile != null) {
      node.put("storeInSegmentFile", _storeInSegmentFile);
    }
    return node;
  }

  /// Builder fields are kept as primitives for binary compatibility — subclasses (e.g.
  /// `pinot-segment-local`'s `TextIndexConfigBuilder`) write to them directly via the protected
  /// access path. The wire-level explicit-vs-default distinction lives in [TextIndexConfig]'s
  /// wrapper-typed fields and the `@JsonCreator` ctor; programmatic builder users that want to
  /// pin an explicit-default value can construct the config directly via the @JsonCreator path.
  public static abstract class AbstractBuilder {
    @Nullable
    protected Object _rawValueForTextIndex;
    protected boolean _enableQueryCache = false;
    protected boolean _useANDForMultiTermQueries = LUCENE_INDEX_DEFAULT_USE_AND_FOR_MULTI_TERM_QUERIES;
    protected List<String> _stopWordsInclude = new ArrayList<>();
    protected List<String> _stopWordsExclude = new ArrayList<>();
    protected boolean _luceneUseCompoundFile = LUCENE_INDEX_DEFAULT_USE_COMPOUND_FILE;
    protected int _luceneMaxBufferSizeMB = LUCENE_INDEX_DEFAULT_MAX_BUFFER_SIZE_MB;
    protected String _luceneAnalyzerClass = FieldConfig.TEXT_INDEX_DEFAULT_LUCENE_ANALYZER_CLASS;
    protected List<String> _luceneAnalyzerClassArgs = new ArrayList<>();
    protected List<String> _luceneAnalyzerClassArgTypes = new ArrayList<>();
    protected String _luceneQueryParserClass = FieldConfig.TEXT_INDEX_DEFAULT_LUCENE_QUERY_PARSER_CLASS;
    protected boolean _enablePrefixSuffixMatchingInPhraseQueries =
        LUCENE_INDEX_ENABLE_PREFIX_SUFFIX_MATCH_IN_PHRASE_SEARCH;
    protected boolean _reuseMutableIndex = LUCENE_INDEX_REUSE_MUTABLE_INDEX;
    protected int _luceneNRTCachingDirectoryMaxBufferSizeMB = LUCENE_INDEX_NRT_CACHING_DIRECTORY_MAX_BUFFER_SIZE_MB;
    protected boolean _useLogByteSizeMergePolicy = LUCENE_USE_LOG_BYTE_SIZE_MERGE_POLICY;
    @Nullable
    protected DocIdTranslatorMode _docIdTranslatorMode = LUCENE_TRANSLATOR_MODE;
    protected boolean _caseSensitive = LUCENE_INDEX_DEFAULT_CASE_SENSITIVE_INDEX;
    protected boolean _storeInSegmentFile = LUCENE_INDEX_DEFAULT_STORE_IN_SEGMENT_FILE;

    public AbstractBuilder() {
    }

    public AbstractBuilder(TextIndexConfig other) {
      _enableQueryCache = other.isEnableQueryCache();
      _useANDForMultiTermQueries = other.isUseANDForMultiTermQueries();
      _stopWordsInclude =
          other._stopWordsInclude == null ? new ArrayList<>() : new ArrayList<>(other._stopWordsInclude);
      _stopWordsExclude =
          other._stopWordsExclude == null ? new ArrayList<>() : new ArrayList<>(other._stopWordsExclude);
      _luceneUseCompoundFile = other.isLuceneUseCompoundFile();
      _luceneMaxBufferSizeMB = other.getLuceneMaxBufferSizeMB();
      _luceneAnalyzerClass = other.getLuceneAnalyzerClass();
      _luceneAnalyzerClassArgs = other._luceneAnalyzerClassArgs;
      _luceneAnalyzerClassArgTypes = other._luceneAnalyzerClassArgTypes;
      _luceneQueryParserClass = other.getLuceneQueryParserClass();
      _enablePrefixSuffixMatchingInPhraseQueries = other.isEnablePrefixSuffixMatchingInPhraseQueries();
      _reuseMutableIndex = other.isReuseMutableIndex();
      _luceneNRTCachingDirectoryMaxBufferSizeMB = other.getLuceneNRTCachingDirectoryMaxBufferSizeMB();
      _useLogByteSizeMergePolicy = other.isUseLogByteSizeMergePolicy();
      _docIdTranslatorMode = other._docIdTranslatorMode;
      _caseSensitive = other.isCaseSensitive();
      _storeInSegmentFile = other.isStoreInSegmentFile();
    }

    /// Build the config. Builder fields are primitives (binary-compat constraint), so the slim form's
    /// "explicit-vs-default" distinction in JSON is approximated here by passing `null` to the wrapper
    /// `@JsonCreator` whenever a builder field equals its static default. Programmatic users that
    /// want to pin "explicit-default" should construct the config directly via the wrapper ctor.
    public TextIndexConfig build() {
      return new TextIndexConfig(Boolean.FALSE, _rawValueForTextIndex,
          _enableQueryCache ? Boolean.TRUE : null,
          _useANDForMultiTermQueries == LUCENE_INDEX_DEFAULT_USE_AND_FOR_MULTI_TERM_QUERIES ? null
              : Boolean.valueOf(_useANDForMultiTermQueries),
          _stopWordsInclude, _stopWordsExclude,
          _luceneUseCompoundFile == LUCENE_INDEX_DEFAULT_USE_COMPOUND_FILE ? null
              : Boolean.valueOf(_luceneUseCompoundFile),
          _luceneMaxBufferSizeMB == LUCENE_INDEX_DEFAULT_MAX_BUFFER_SIZE_MB ? null
              : Integer.valueOf(_luceneMaxBufferSizeMB),
          Objects.equals(_luceneAnalyzerClass, FieldConfig.TEXT_INDEX_DEFAULT_LUCENE_ANALYZER_CLASS) ? null
              : _luceneAnalyzerClass,
          (Object) CsvParser.serialize(_luceneAnalyzerClassArgs, true, false),
          (Object) CsvParser.serialize(_luceneAnalyzerClassArgTypes, true, false),
          Objects.equals(_luceneQueryParserClass, FieldConfig.TEXT_INDEX_DEFAULT_LUCENE_QUERY_PARSER_CLASS) ? null
              : _luceneQueryParserClass,
          _enablePrefixSuffixMatchingInPhraseQueries == LUCENE_INDEX_ENABLE_PREFIX_SUFFIX_MATCH_IN_PHRASE_SEARCH ? null
              : Boolean.valueOf(_enablePrefixSuffixMatchingInPhraseQueries),
          _reuseMutableIndex == LUCENE_INDEX_REUSE_MUTABLE_INDEX ? null : Boolean.valueOf(_reuseMutableIndex),
          _luceneNRTCachingDirectoryMaxBufferSizeMB == LUCENE_INDEX_NRT_CACHING_DIRECTORY_MAX_BUFFER_SIZE_MB ? null
              : Integer.valueOf(_luceneNRTCachingDirectoryMaxBufferSizeMB),
          _useLogByteSizeMergePolicy == LUCENE_USE_LOG_BYTE_SIZE_MERGE_POLICY ? null
              : Boolean.valueOf(_useLogByteSizeMergePolicy),
          _docIdTranslatorMode,
          _caseSensitive == LUCENE_INDEX_DEFAULT_CASE_SENSITIVE_INDEX ? null : Boolean.valueOf(_caseSensitive),
          _storeInSegmentFile == LUCENE_INDEX_DEFAULT_STORE_IN_SEGMENT_FILE ? null
              : Boolean.valueOf(_storeInSegmentFile));
    }

    public abstract AbstractBuilder withProperties(@Nullable Map<String, String> textIndexProperties);

    public AbstractBuilder withRawValueForTextIndex(@Nullable Object rawValueForTextIndex) {
      _rawValueForTextIndex = rawValueForTextIndex;
      return this;
    }

    public AbstractBuilder withUseANDForMultiTermQueries(boolean useANDForMultiTermQueries) {
      _useANDForMultiTermQueries = useANDForMultiTermQueries;
      return this;
    }

    public AbstractBuilder withStopWordsInclude(List<String> stopWordsInclude) {
      _stopWordsInclude = stopWordsInclude;
      return this;
    }

    public AbstractBuilder withStopWordsExclude(List<String> stopWordsExclude) {
      _stopWordsExclude = stopWordsExclude;
      return this;
    }

    public AbstractBuilder withLuceneUseCompoundFile(boolean useCompoundFile) {
      _luceneUseCompoundFile = useCompoundFile;
      return this;
    }

    public AbstractBuilder withLuceneMaxBufferSizeMB(int maxBufferSizeMB) {
      _luceneMaxBufferSizeMB = maxBufferSizeMB;
      return this;
    }

    public AbstractBuilder withLuceneAnalyzerClass(String luceneAnalyzerClass) {
      _luceneAnalyzerClass = luceneAnalyzerClass;
      return this;
    }

    public AbstractBuilder withLuceneAnalyzerClassArgs(String luceneAnalyzerClassArgs) {
      _luceneAnalyzerClassArgs = CsvParser.parse(luceneAnalyzerClassArgs, true, false);
      return this;
    }

    public AbstractBuilder withLuceneAnalyzerClassArgs(List<String> luceneAnalyzerClassArgs) {
      _luceneAnalyzerClassArgs = luceneAnalyzerClassArgs;
      return this;
    }

    public AbstractBuilder withLuceneAnalyzerClassArgTypes(String luceneAnalyzerClassArgTypes) {
      _luceneAnalyzerClassArgTypes = CsvParser.parse(luceneAnalyzerClassArgTypes, false, true);
      return this;
    }

    public AbstractBuilder withLuceneAnalyzerClassArgTypes(List<String> luceneAnalyzerClassArgTypes) {
      _luceneAnalyzerClassArgTypes = luceneAnalyzerClassArgTypes;
      return this;
    }

    public AbstractBuilder withLuceneQueryParserClass(String luceneQueryParserClass) {
      _luceneQueryParserClass = luceneQueryParserClass;
      return this;
    }

    public AbstractBuilder withEnablePrefixSuffixMatchingInPhraseQueries(
        boolean enablePrefixSuffixMatchingInPhraseQueries) {
      _enablePrefixSuffixMatchingInPhraseQueries = enablePrefixSuffixMatchingInPhraseQueries;
      return this;
    }

    public AbstractBuilder withReuseMutableIndex(boolean reuseMutableIndex) {
      _reuseMutableIndex = reuseMutableIndex;
      return this;
    }

    public AbstractBuilder withLuceneNRTCachingDirectoryMaxBufferSizeMB(int luceneNRTCachingDirectoryMaxBufferSizeMB) {
      _luceneNRTCachingDirectoryMaxBufferSizeMB = luceneNRTCachingDirectoryMaxBufferSizeMB;
      return this;
    }

    public AbstractBuilder withUseLogByteSizeMergePolicy(boolean useLogByteSizeMergePolicy) {
      _useLogByteSizeMergePolicy = useLogByteSizeMergePolicy;
      return this;
    }

    public AbstractBuilder withDocIdTranslatorMode(String mode) {
      _docIdTranslatorMode = DocIdTranslatorMode.of(mode);
      return this;
    }

    public AbstractBuilder withCaseSensitive(boolean caseSensitive) {
      _caseSensitive = caseSensitive;
      return this;
    }

    public AbstractBuilder withStoreInSegmentFile(boolean storeInSegmentFile) {
      _storeInSegmentFile = storeInSegmentFile;
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    TextIndexConfig that = (TextIndexConfig) o;
    return Objects.equals(_enableQueryCache, that._enableQueryCache)
        && Objects.equals(_useANDForMultiTermQueries, that._useANDForMultiTermQueries)
        && Objects.equals(_luceneUseCompoundFile, that._luceneUseCompoundFile)
        && Objects.equals(_luceneMaxBufferSizeMB, that._luceneMaxBufferSizeMB)
        && Objects.equals(_enablePrefixSuffixMatchingInPhraseQueries, that._enablePrefixSuffixMatchingInPhraseQueries)
        && Objects.equals(_reuseMutableIndex, that._reuseMutableIndex)
        && Objects.equals(_useLogByteSizeMergePolicy, that._useLogByteSizeMergePolicy)
        && _docIdTranslatorMode == that._docIdTranslatorMode
        && Objects.equals(_luceneNRTCachingDirectoryMaxBufferSizeMB, that._luceneNRTCachingDirectoryMaxBufferSizeMB)
        && Objects.equals(_rawValueForTextIndex, that._rawValueForTextIndex)
        && Objects.equals(_stopWordsInclude, that._stopWordsInclude)
        && Objects.equals(_stopWordsExclude, that._stopWordsExclude)
        && Objects.equals(_luceneAnalyzerClass, that._luceneAnalyzerClass)
        && Objects.equals(_luceneAnalyzerClassArgs, that._luceneAnalyzerClassArgs)
        && Objects.equals(_luceneAnalyzerClassArgTypes, that._luceneAnalyzerClassArgTypes)
        && Objects.equals(_luceneQueryParserClass, that._luceneQueryParserClass)
        && Objects.equals(_caseSensitive, that._caseSensitive)
        && Objects.equals(_storeInSegmentFile, that._storeInSegmentFile);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _rawValueForTextIndex, _enableQueryCache, _useANDForMultiTermQueries,
        _stopWordsInclude, _stopWordsExclude, _luceneUseCompoundFile,
        _luceneMaxBufferSizeMB, _luceneAnalyzerClass, _luceneAnalyzerClassArgs, _luceneAnalyzerClassArgTypes,
        _luceneQueryParserClass, _enablePrefixSuffixMatchingInPhraseQueries, _reuseMutableIndex,
        _luceneNRTCachingDirectoryMaxBufferSizeMB, _useLogByteSizeMergePolicy, _docIdTranslatorMode, _caseSensitive,
        _storeInSegmentFile);
  }

  public static boolean isProperty(String prop) {
    return PROPERTY_NAMES.contains(prop);
  }
}
