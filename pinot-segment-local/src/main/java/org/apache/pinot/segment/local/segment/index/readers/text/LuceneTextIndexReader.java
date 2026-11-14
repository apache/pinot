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
package org.apache.pinot.segment.local.segment.index.readers.text;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.local.segment.store.TextIndexUtils;
import org.apache.pinot.segment.local.utils.LuceneTextIndexUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.TextIndexConfig.DocIdTranslatorMode;
import org.apache.pinot.segment.spi.index.reader.QueryScopedCache;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.exception.QueryException;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.LoggerFactory;


/// This is used to read/search the Lucene text index.
/// When [org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader] loads the segment,
/// it also loads (mmaps) the Lucene text index if the segment has TEXT column(s).
public class LuceneTextIndexReader implements TextIndexReader {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(LuceneTextIndexReader.class);

  private final IndexReader _indexReader;
  private final Directory _indexDirectory;
  private final IndexSearcher _indexSearcher;
  /// Number of documents in the Pinot segment. A Lucene index holding more than this means a corrupted or
  /// partially converted segment; see [#getNumMatchingDocs].
  private final int _numDocs;
  private final String _column;
  private final DocIdTranslator _docIdTranslator;
  private final Analyzer _analyzer;
  private boolean _useANDForMultiTermQueries = false;
  private final String _queryParserClass;
  private Constructor<QueryParserBase> _queryParserClassConstructor;
  private boolean _enablePrefixSuffixMatchingInPhraseQueries = false;
  /// Everything about this column that changes how a search string parses. Two readers may share a
  /// parsed query only when these are equal. Computed once; immutable, with structural equality.
  private final ParseConfig _parseConfig;

  /// Retains support for immutable Lucene 8 text indexes through Lucene's backward codecs.
  /// The default reader only accepts indexes created with the current or previous major version.
  static DirectoryReader openIndexReader(Directory directory)
      throws IOException {
    try {
      return DirectoryReader.open(directory);
    } catch (IndexFormatTooOldException e) {
      // This lower-level reader accepts the Lucene 8 commit rejected by the default version check.
      return StandardDirectoryReader.open(directory, SegmentInfos.readLatestCommit(directory, 8), null, null);
    }
  }

  public LuceneTextIndexReader(String column, File indexDir, int numDocs, TextIndexConfig config) {
    _column = column;
    try {
      File indexFile = getTextIndexFile(indexDir);
      _indexDirectory = FSDirectory.open(indexFile.toPath());
      _indexReader = openIndexReader(_indexDirectory);
      _indexSearcher = new IndexSearcher(_indexReader);
      _numDocs = numDocs;
      if (!config.isEnableQueryCache()) {
        // Disable Lucene query result cache. While it helps a lot with performance for
        // repeated queries, on the downside it can cause heap issues.
        _indexSearcher.setQueryCache(null);
      }
      if (config.isUseANDForMultiTermQueries()) {
        _useANDForMultiTermQueries = true;
      }
      if (config.isEnablePrefixSuffixMatchingInPhraseQueries()) {
        _enablePrefixSuffixMatchingInPhraseQueries = true;
      }
      // TODO: consider using a threshold of num docs per segment to decide between building
      // mapping file upfront on segment load v/s on-the-fly during query processing
      // If the properties file exists, use the analyzer properties and query parser class from the properties file
      File propertiesFile = new File(indexFile, V1Constants.Indexes.LUCENE_TEXT_INDEX_PROPERTIES_FILE);
      if (propertiesFile.exists()) {
        config = TextIndexUtils.getUpdatedConfigFromPropertiesFile(propertiesFile, config);
      }

      _docIdTranslator = prepareDocIdTranslator(indexDir, _column, numDocs, _indexSearcher, config, indexFile);
      _analyzer = TextIndexUtils.getAnalyzer(config);
      _queryParserClass = config.getLuceneQueryParserClass();
      _queryParserClassConstructor =
          TextIndexUtils.getQueryParserWithStringAndAnalyzerTypeConstructor(_queryParserClass);
      // after the properties-file override above, so it reflects the configuration actually in force
      _parseConfig = buildParseConfig(_column, config);
      LOGGER.info("Successfully read lucene index for {} from {}", _column, indexDir);
    } catch (Exception e) {
      LOGGER.error("Failed to instantiate Lucene text index reader for column {}, exception {}", column,
          e.getMessage());
      throw new RuntimeException(e);
    }
  }

  /// As part of loading the segment in ImmutableSegmentLoader,
  /// we load the text index (per column if it exists) and store
  /// the reference in [org.apache.pinot.segment.local.segment.index.column.PhysicalColumnIndexContainer]
  /// similar to how it is done for other types of indexes.
  ///
  /// @param column   column name
  /// @param indexDir segment index directory
  /// @param numDocs  number of documents in the segment
  public LuceneTextIndexReader(String column, File indexDir, int numDocs,
      @Nullable Map<String, String> textIndexProperties) {
    this(column, indexDir, numDocs, new TextIndexConfigBuilder().withProperties(textIndexProperties).build());
  }

  /// Buffer-based constructor for reading Lucene text index from a combined buffer.
  /// This constructor creates a Lucene Directory from a PinotDataBuffer containing
  /// all the text index files in a combined format.
  ///
  /// @param column the column name
  /// @param indexBuffer the buffer containing combined text index data
  /// @param numDocs number of documents in the segment
  /// @param config text index configuration
  public LuceneTextIndexReader(String column, PinotDataBuffer indexBuffer, int numDocs, TextIndexConfig config) {
    _column = column;
    try {
      // Create Lucene Directory from buffer
      Directory indexDirectory = LuceneTextIndexBufferReader.createLuceneDirectory(indexBuffer, column);

      // Extract properties from buffer
      Properties properties = LuceneTextIndexBufferReader.extractProperties(indexBuffer, column);
      if (!properties.isEmpty()) {
        config = updateConfigFromProperties(properties, config);
      }

      // Initialize from components
      _indexDirectory = indexDirectory;
      _indexReader = openIndexReader(_indexDirectory);
      _indexSearcher = new IndexSearcher(_indexReader);
      _numDocs = numDocs;

      if (!config.isEnableQueryCache()) {
        // Disable Lucene query result cache. While it helps a lot with performance for
        // repeated queries, on the downside it can cause heap issues.
        _indexSearcher.setQueryCache(null);
      }

      if (config.isUseANDForMultiTermQueries()) {
        _useANDForMultiTermQueries = true;
      }

      if (config.isEnablePrefixSuffixMatchingInPhraseQueries()) {
        _enablePrefixSuffixMatchingInPhraseQueries = true;
      }
      PinotDataBuffer docIdMappingBuffer = LuceneTextIndexBufferReader.extractDocIdMappingBuffer(indexBuffer, column);
      // Initialize docId translator
      long startTime = System.currentTimeMillis();
      _docIdTranslator = createDocIdTranslator(docIdMappingBuffer, config, numDocs);
      LOGGER.info("Time taken to create docIdTranslator for column {}: {} ms", column,
          System.currentTimeMillis() - startTime);
      // Initialize analyzer and query parser
      _analyzer = TextIndexUtils.getAnalyzer(config);
      _queryParserClass = config.getLuceneQueryParserClass();
      _queryParserClassConstructor =
          TextIndexUtils.getQueryParserWithStringAndAnalyzerTypeConstructor(_queryParserClass);
      // after the properties-file override above, so it reflects the configuration actually in force
      _parseConfig = buildParseConfig(_column, config);

      LOGGER.info("Successfully read lucene index for {} from buffer", _column);
    } catch (Exception e) {
      LOGGER.error("Failed to instantiate Lucene text index reader for column {} from buffer", column, e);
      throw new RuntimeException(e);
    }
  }

  /// CASE 1: If IndexLoadingConfig specifies a segment version to load and if it is different then the on-disk
  /// version of the segment, then [org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader]
  /// will take care of up-converting the on-disk segment to v3 before load. The converter already has support for
  /// converting v1 text index to v3. So the text index can be loaded from segmentIndexDir/v3/ since v3 sub-directory
  /// would have already been created
  ///
  /// CASE 2: However, if IndexLoadingConfig doesn't specify the segment version to load or if the specified
  /// version is same as the on-disk version of the segment, then ImmutableSegmentLoader will load
  /// whatever the version of segment is on disk.
  ///
  /// @param segmentIndexDir top-level segment index directory
  /// @return text index file
  private File getTextIndexFile(File segmentIndexDir) {
    // will return null if file does not exist
    File file = SegmentDirectoryPaths.findTextIndexIndexFile(segmentIndexDir, _column);
    if (file == null) {
      throw new IllegalStateException("Failed to find text index file for column: " + _column);
    }
    return file;
  }

  @Override
  public ImmutableRoaringBitmap getDictIds(String searchQuery) {
    throw new UnsupportedOperationException("");
  }

  @Deprecated
  @Override
  public MutableRoaringBitmap getDocIds(String searchQuery) {
    return getDocIds(searchQuery, null);
  }

  @Override
  public MutableRoaringBitmap getDocIds(String searchQuery, @Nullable String optionsString) {
    return getDocIds(searchQuery, optionsString, null);
  }

  @Override
  public MutableRoaringBitmap getDocIds(String searchQuery, @Nullable String optionsString,
      @Nullable QueryScopedCache queryScopedCache) {
    MutableRoaringBitmap docIds = new MutableRoaringBitmap();
    Collector docIDCollector = new LuceneDocIdCollector(docIds, _docIdTranslator);
    try {
      validateDocCount();
      _indexSearcher.search(buildQuery(searchQuery, optionsString, queryScopedCache), docIDCollector);
      return docIds;
    } catch (QueryException e) {
      throw e;
    } catch (Exception e) {
      // Both wordings are preserved verbatim from before the two paths were merged: the options path's text is
      // asserted on in TextSearchQueriesTest#testTextFilterOptimizerWithWildcardsDifferentOptions, and the
      // no-options path is the common one whose text operators may already alert on.
      throw new RuntimeException(hasOptions(optionsString)
          ? "Failed while searching the text index for column " + _column + " with search query: " + searchQuery
          : "Caught exception while searching the text index for column:" + _column + " search query:" + searchQuery,
          e);
    }
  }

  private void validateDocCount() {
    int maxDoc = _indexReader.maxDoc();
    // Collectors address maxDoc() space, including deleted documents. Skip mode has no mapping-buffer bounds check.
    if (maxDoc > _numDocs) {
      throw new IllegalStateException(
          "Lucene text index contains " + maxDoc + " documents, exceeding segment document count: " + _numDocs);
    }
  }

  /// Identifies a parsing configuration. A record so that equality is structural: comparing the
  /// rendered text of these lists instead would conflate `["failed", "order"]` with
  /// `["failed, order"]`, which are different stop-word sets that parse the same string differently.
  private record ParseConfig(String column, String analyzerClass, List<String> analyzerArgs,
                             List<String> analyzerArgTypes, List<String> stopWordsInclude,
                             List<String> stopWordsExclude, boolean caseSensitive, String queryParserClass,
                             boolean useANDForMultiTermQueries, boolean prefixSuffixMatchingInPhraseQueries) {
    private ParseConfig {
      analyzerArgs = copy(analyzerArgs);
      analyzerArgTypes = copy(analyzerArgTypes);
      stopWordsInclude = copy(stopWordsInclude);
      stopWordsExclude = copy(stopWordsExclude);
    }

    private static List<String> copy(@Nullable List<String> values) {
      return values == null ? List.of() : List.copyOf(values);
    }
  }

  /// Cache key for one parsed query: the configuration plus the request-supplied text.
  private record ParsedQueryKey(ParseConfig parseConfig, String searchQuery, @Nullable String optionsString) {
  }

  /// Carries a checked parse failure out of the cache's mapper so the original exception, and in
  /// particular a [QueryException]'s error code, survives instead of being wrapped.
  private static final class ParseFailure extends RuntimeException {
    ParseFailure(Exception cause) {
      super(cause);
    }
  }

  private static ParseConfig buildParseConfig(String column, TextIndexConfig config) {
    return new ParseConfig(column, config.getLuceneAnalyzerClass(), config.getLuceneAnalyzerClassArgs(),
        config.getLuceneAnalyzerClassArgTypes(), config.getStopWordsInclude(), config.getStopWordsExclude(),
        config.isCaseSensitive(), config.getLuceneQueryParserClass(), config.isUseANDForMultiTermQueries(),
        config.isEnablePrefixSuffixMatchingInPhraseQueries());
  }

  /// Parses a search string into a Lucene query, applying the configured parser and options.
  ///
  /// Shared by the doc-id and the count paths so that a `count(*)` and the filter it counts can never
  /// interpret the same query differently.
  private Query parseQueryWithoutOptions(String searchQuery)
      throws Exception {
    // Lucene query parsers are generally stateful and a new instance must be created per query.
    QueryParserBase parser = _queryParserClassConstructor.newInstance(_column, _analyzer);
    // Phrase search with prefix/suffix matching may have leading *. E.g., `*pache pinot` which can be stripped by
    // the query parser. To support the feature, we need to explicitly set the config to be true.
    if (_enablePrefixSuffixMatchingInPhraseQueries) {
      parser.setAllowLeadingWildcard(true);
    }
    if (_useANDForMultiTermQueries) {
      parser.setDefaultOperator(QueryParser.Operator.AND);
    }
    Query query = parser.parse(searchQuery);
    if (_queryParserClass.equals("org.apache.lucene.queryparser.classic.QueryParser")
            && _enablePrefixSuffixMatchingInPhraseQueries) {
      query = LuceneTextIndexUtils.convertToMultiTermSpanQuery(query);
    }
    return query;
  }

  @Override
  public int getNumMatchingDocs(String searchQuery, @Nullable String optionsString) {
    return getNumMatchingDocs(searchQuery, optionsString, null);
  }

  @Override
  public int getNumMatchingDocs(String searchQuery, @Nullable String optionsString,
      @Nullable QueryScopedCache queryScopedCache) {
    try {
      validateDocCount();
      return LuceneTextIndexUtils.countWithoutMaterializing(_indexSearcher,
          buildQuery(searchQuery, optionsString, queryScopedCache));
    } catch (QueryException e) {
      // A timeout or accountant kill arrives as a QueryException; wrapping it would erase its error code,
      // since only QueryException keeps its code through BaseSingleBlockCombineOperator.
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while counting matches in the text index for column:" + _column + " search query:"
              + searchQuery, e);
    }
  }

  /// Parses a search string into a Lucene query, honouring the options string when one is given, and
  /// reusing the query already parsed for this request when a cache is given.
  ///
  /// Both [#getDocIds(String, String)] and [#getNumMatchingDocs] go through this method, so a `count(*)` and
  /// the filter it counts cannot interpret the same query differently. Keep it that way: any new entry point
  /// that parses a search string itself reintroduces that divergence.
  ///
  /// Every segment of a request parses the same string against the same configuration, so without a
  /// cache the work is repeated once per segment. With a cache the string is parsed once per effective
  /// parsing configuration rather than once per segment: one request can span segments whose text
  /// indexes are configured differently, and those keep their own entries. A parse that throws caches
  /// nothing, so the next segment retries and fails the same way.
  ///
  /// The returned query may be shared: Lucene queries are immutable and [Query#rewrite] does not
  /// mutate them, so a multi-term query is still expanded per segment against that segment's own
  /// term dictionary.
  @VisibleForTesting
  Query buildQuery(String searchQuery, @Nullable String optionsString, @Nullable QueryScopedCache queryScopedCache)
      throws Exception {
    if (queryScopedCache == null) {
      return parseQuery(searchQuery, optionsString);
    }
    ParsedQueryKey key = new ParsedQueryKey(_parseConfig, searchQuery, optionsString);
    try {
      return queryScopedCache.getOrComputeSharedValue(Query.class, key, k -> {
        try {
          return parseQuery(k.searchQuery(), k.optionsString());
        } catch (Exception e) {
          throw new ParseFailure(e);
        }
      });
    } catch (ParseFailure e) {
      throw (Exception) e.getCause();
    }
  }

  /// Parses unconditionally, bypassing any cache.
  @VisibleForTesting
  Query parseQuery(String searchQuery, @Nullable String optionsString)
      throws Exception {
    if (hasOptions(optionsString)) {
      LuceneTextIndexUtils.LuceneTextIndexOptions options = LuceneTextIndexUtils.createOptions(optionsString);
      return LuceneTextIndexUtils.createQueryParserWithOptions(searchQuery, options, _column, _analyzer);
    }
    return parseQueryWithoutOptions(searchQuery);
  }

  /// Whether `optionsString` selects the options-driven parser, i.e. it is non-blank and parses to at least
  /// one option. Shared so the query built and the error reported cannot disagree about which path ran.
  private static boolean hasOptions(@Nullable String optionsString) {
    return optionsString != null && !optionsString.trim().isEmpty()
        && !LuceneTextIndexUtils.createOptions(optionsString).getOptions().isEmpty();
  }


  /// When we destroy the loaded ImmutableSegment, all the indexes
  /// (for each column) are destroyed and as part of that
  /// we release the text index
  ///
  /// @throws IOException
  @Override
  public void close()
      throws IOException {
    _indexReader.close();
    _indexDirectory.close();
    _docIdTranslator.close();
    _analyzer.close();
  }

  /// Updates TextIndexConfig from Properties object
  private TextIndexConfig updateConfigFromProperties(Properties properties, TextIndexConfig config) {
    TextIndexConfigBuilder builder = new TextIndexConfigBuilder(config);

    String analyzerClass = properties.getProperty(FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS);
    if (analyzerClass != null) {
      builder.withLuceneAnalyzerClass(analyzerClass);
    }

    String analyzerArgs = properties.getProperty(FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS_ARGS);
    if (analyzerArgs != null) {
      List<String> args = Arrays.asList(analyzerArgs.split(","));
      builder.withLuceneAnalyzerClassArgs(args);
    }

    String analyzerArgTypes = properties.getProperty(FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS_ARG_TYPES);
    if (analyzerArgTypes != null) {
      List<String> argTypes = Arrays.asList(analyzerArgTypes.split(","));
      builder.withLuceneAnalyzerClassArgTypes(argTypes);
    }

    String queryParserClass = properties.getProperty(FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS);
    if (queryParserClass != null) {
      builder.withLuceneQueryParserClass(queryParserClass);
    }

    String docIdTranslatorMode = properties.getProperty(FieldConfig.TEXT_INDEX_LUCENE_DOC_ID_TRANSLATOR_MODE);
    if (docIdTranslatorMode != null) {
      builder.withDocIdTranslatorMode(docIdTranslatorMode);
    }

    return builder.build();
  }

  DocIdTranslator prepareDocIdTranslator(File segmentIndexDir, String column, int numDocs, IndexSearcher indexSearcher,
      TextIndexConfig config, File indexDir)
      throws IOException {
    if (config.getDocIdTranslatorMode() == DocIdTranslatorMode.Skip) {
      LOGGER.debug("Using no-op doc id translator");
      return NoOpDocIdTranslator.INSTANCE;
    }

    int length = Integer.BYTES * numDocs;
    File docIdMappingFile = new File(SegmentDirectoryPaths.findSegmentDirectory(segmentIndexDir),
        column + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    // The mapping is local to a segment. It is created on the server during segment load.
    // Unless we are running Pinot on Solaris/SPARC, the underlying architecture is
    // LITTLE_ENDIAN (Linux/x86). So use that as byte order.
    String desc = "Text index docId mapping buffer: " + column;
    PinotDataBuffer buffer = null;

    try {
      if (docIdMappingFile.exists()) {
        // we will be here for segment reload and server restart
        // for refresh, we will not be here since segment is deleted/replaced
        // TODO: see if we can prefetch the pages
        buffer =
            PinotDataBuffer.mapFile(docIdMappingFile, /* readOnly */ true, 0, length, ByteOrder.LITTLE_ENDIAN, desc);

        return new DefaultDocIdTranslator(buffer);
      }
      buffer =
          PinotDataBuffer.mapFile(docIdMappingFile, /* readOnly */ false, 0, length, ByteOrder.LITTLE_ENDIAN, desc);
      LOGGER.debug("Creating lucene to pinot doc id mapping.");
      boolean isIdentityMapping =
          TextIndexUtils.fillDocIdMapping(indexSearcher.getIndexReader(), buffer, numDocs, null);
      if (config.getDocIdTranslatorMode() == DocIdTranslatorMode.TryOptimize) {
        if (isIdentityMapping) {
          LOGGER.debug("Lucene doc ids are equal to Pinot's. Deleting mapping and updating index settings.");
          // TODO: it'd be better to unmap without flushing the buffer. Only some buffer types support it, though.
          buffer.close();
          buffer = null;
          // get rid of mapping and use lucene ids
          docIdMappingFile.delete();

          // mapping is unnecessary so store flag in config file to skip checking on next load
          TextIndexConfig newConfig =
              new TextIndexConfigBuilder(config).withDocIdTranslatorMode(DocIdTranslatorMode.Skip.name()).build();
          TextIndexUtils.writeConfigToPropertiesFile(indexDir, newConfig);

          return NoOpDocIdTranslator.INSTANCE;
        }
        LOGGER.debug("Lucene doc ids are not equal to Pinot's. Keeping the mapping.");
        // mapping is required so switch to default mode
        TextIndexConfig newConfig =
            new TextIndexConfigBuilder(config).withDocIdTranslatorMode(DocIdTranslatorMode.Default.name()).build();
        TextIndexUtils.writeConfigToPropertiesFile(indexDir, newConfig);
      }
      return new DefaultDocIdTranslator(buffer);
    } catch (Exception e) {
      if (buffer != null) {
        buffer.close();
      }

      throw new RuntimeException("Caught exception while building doc id mapping for text index column: " + column, e);
    }
  }

  /// Creates docId translator from buffer
  private DocIdTranslator createDocIdTranslator(PinotDataBuffer docIdMappingBuffer, TextIndexConfig config, int numDocs)
      throws IOException {
    if (config.getDocIdTranslatorMode() == DocIdTranslatorMode.Skip) {
      LOGGER.debug("Using no-op doc id translator");
      return NoOpDocIdTranslator.INSTANCE;
    }

    if (docIdMappingBuffer != null) {
      // Ensure the buffer is in little endian format as expected by DefaultDocIdTranslator
      // Create a view with little endian byte order if the buffer is not already in little endian
      PinotDataBuffer littleEndianBuffer = docIdMappingBuffer.order() == ByteOrder.LITTLE_ENDIAN
          ? docIdMappingBuffer
          : docIdMappingBuffer.view(0, docIdMappingBuffer.size(), ByteOrder.LITTLE_ENDIAN);
      return new DefaultDocIdTranslator(littleEndianBuffer);
    }

    LOGGER.info("building doc id mapping for text index column: {}", _column);
    // Create a new buffer and populate it
    int length = Integer.BYTES * numDocs;
    String desc = "Text index docId mapping buffer: " + _column;
    PinotDataBuffer buffer = PinotDataBuffer.allocateDirect(length, ByteOrder.LITTLE_ENDIAN, desc);

    try {
      LOGGER.debug("Creating lucene to pinot doc id mapping.");
      boolean isIdentityMapping = TextIndexUtils.fillDocIdMapping(_indexReader, buffer, numDocs, null);
      if (config.getDocIdTranslatorMode() == DocIdTranslatorMode.TryOptimize) {
        if (isIdentityMapping) {
          LOGGER.debug("Lucene doc ids are equal to Pinot's. Using no-op translator.");
          buffer.close();
          return NoOpDocIdTranslator.INSTANCE;
        }
        LOGGER.debug("Lucene doc ids are not equal to Pinot's. Using default translator.");
      }
      return new DefaultDocIdTranslator(buffer);
    } catch (Exception e) {
      buffer.close();
      throw new RuntimeException("Caught exception while creating doc id translator for text index column: " + _column,
          e);
    }
  }
}
