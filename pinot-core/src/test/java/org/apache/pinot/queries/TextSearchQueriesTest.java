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
package org.apache.pinot.queries;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.ControlledRealTimeReopenThread;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.response.broker.SelectionResults;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.SelectionOnlyOperator;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Functional tests for text search feature.
 * The tests use two kinds of input data
 * (1) Skills file
 * (2) PQL query log file
 * The test table has a SKILLS column and QUERY_LOG column. Text index is created
 * on each of these columns.
 */
public class TextSearchQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "TextSearchQueriesTest");
  private static final String TABLE_NAME = "MyTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String QUERY_LOG_TEXT_COL_NAME = "QUERY_LOG_TEXT_COL";
  private static final String SKILLS_TEXT_COL_NAME = "SKILLS_TEXT_COL";
  private static final String SKILLS_TEXT_COL_DICT_NAME = "SKILLS_TEXT_COL_DICT";
  private static final String SKILLS_TEXT_COL_MULTI_TERM_NAME = "SKILLS_TEXT_COL_1";
  private static final String SKILLS_TEXT_NO_RAW_NAME = "SKILLS_TEXT_COL_2";
  private static final String INT_COL_NAME = "INT_COL";
  private static final List<String> RAW_TEXT_INDEX_COLUMNS = Arrays
      .asList(QUERY_LOG_TEXT_COL_NAME, SKILLS_TEXT_COL_NAME, SKILLS_TEXT_COL_MULTI_TERM_NAME, SKILLS_TEXT_NO_RAW_NAME);
  private static final List<String> DICT_TEXT_INDEX_COLUMNS = Arrays.asList(SKILLS_TEXT_COL_DICT_NAME);
  private static final int INT_BASE_VALUE = 1000;

  private final List<GenericRow> _rows = new ArrayList<>();

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    buildSegment();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    Set<String> textIndexColumns = new HashSet<>();
    textIndexColumns.addAll(RAW_TEXT_INDEX_COLUMNS);
    textIndexColumns.addAll(DICT_TEXT_INDEX_COLUMNS);
    indexLoadingConfig.setTextIndexColumns(textIndexColumns);
    indexLoadingConfig.setInvertedIndexColumns(new HashSet<>(DICT_TEXT_INDEX_COLUMNS));
    Map<String, Map<String, String>> columnProperties = new HashMap<>();
    Map<String, String> props = new HashMap<>();
    props.put(FieldConfig.TEXT_INDEX_USE_AND_FOR_MULTI_TERM_QUERIES, "true");
    columnProperties.put(SKILLS_TEXT_COL_MULTI_TERM_NAME, props);
    indexLoadingConfig.setColumnProperties(columnProperties);
    ImmutableSegment immutableSegment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  @AfterClass
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private void buildSegment()
      throws Exception {
    List<GenericRow> rows = createTestData();

    List<FieldConfig> fieldConfigs = new ArrayList<>(RAW_TEXT_INDEX_COLUMNS.size() + DICT_TEXT_INDEX_COLUMNS.size());
    for (String textIndexColumn : RAW_TEXT_INDEX_COLUMNS) {
      fieldConfigs
          .add(new FieldConfig(textIndexColumn, FieldConfig.EncodingType.RAW, FieldConfig.IndexType.TEXT, null, null));
    }
    for (String textIndexColumn : DICT_TEXT_INDEX_COLUMNS) {
      fieldConfigs
          .add(new FieldConfig(textIndexColumn, FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.TEXT, null, null));
    }
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(RAW_TEXT_INDEX_COLUMNS).setInvertedIndexColumns(DICT_TEXT_INDEX_COLUMNS)
        .setFieldConfigList(fieldConfigs).build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(QUERY_LOG_TEXT_COL_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(SKILLS_TEXT_COL_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(SKILLS_TEXT_COL_DICT_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(SKILLS_TEXT_COL_MULTI_TERM_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(SKILLS_TEXT_NO_RAW_NAME, FieldSpec.DataType.STRING)
        .addMetric(INT_COL_NAME, FieldSpec.DataType.INT).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);
    Map<String, Map<String, String>> columnProperties = new HashMap<>();
    Map<String, String> props = new HashMap<>();
    props.put(FieldConfig.TEXT_INDEX_NO_RAW_DATA, "true");
    props.put(FieldConfig.TEXT_INDEX_RAW_VALUE, "ILoveCoding");
    columnProperties.put(SKILLS_TEXT_NO_RAW_NAME, props);
    config.setColumnProperties(columnProperties);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  private List<GenericRow> createTestData()
      throws Exception {
    List<GenericRow> rows = new ArrayList<>();

    // read the skills file
    URL resourceUrl = getClass().getClassLoader().getResource("data/text_search_data/skills.txt");
    File skillFile = new File(resourceUrl.getFile());
    String[] skills = new String[100];
    int skillCount = 0;
    try (InputStream inputStream = new FileInputStream(skillFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      String line;
      while ((line = reader.readLine()) != null) {
        skills[skillCount++] = line;
      }
    }

    // read the pql query log file (24k queries) and build dataset
    resourceUrl = getClass().getClassLoader().getResource("data/text_search_data/pql_query1.txt");
    File logFile = new File(resourceUrl.getFile());
    int counter = 0;
    try (InputStream inputStream = new FileInputStream(logFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      String line;
      while ((line = reader.readLine()) != null) {
        GenericRow row = new GenericRow();
        row.putValue(INT_COL_NAME, INT_BASE_VALUE + counter);
        row.putValue(QUERY_LOG_TEXT_COL_NAME, line);
        if (counter >= skillCount) {
          row.putValue(SKILLS_TEXT_COL_NAME, "software engineering");
          row.putValue(SKILLS_TEXT_COL_DICT_NAME, "software engineering");
          row.putValue(SKILLS_TEXT_COL_MULTI_TERM_NAME, "software engineering");
          row.putValue(SKILLS_TEXT_COL_MULTI_TERM_NAME, "software engineering");
        } else {
          row.putValue(SKILLS_TEXT_COL_NAME, skills[counter]);
          row.putValue(SKILLS_TEXT_COL_DICT_NAME, skills[counter]);
          row.putValue(SKILLS_TEXT_COL_MULTI_TERM_NAME, skills[counter]);
          row.putValue(SKILLS_TEXT_NO_RAW_NAME, skills[counter]);
        }
        rows.add(row);
        counter++;
      }
    }

    return rows;
  }

  /**
   * Tests for phrase, term, regex, composite (using AND/OR) text search queries.
   * Both selection and aggregation queries are used.
   */
  @Test
  public void testTextSearch()
      throws Exception {
    // TEST 1: phrase query
    // Search in QUERY_TEXT_COL to look for documents where each document MUST contain phrase "SELECT dimensionCol2"
    // as is. In other words, we are trying to find all "SELECT dimensionCol2" style queries. The expected result size
    // of 11787 comes from manually doing a grep on the test file.
    String query =
        "SELECT INT_COL, QUERY_LOG_TEXT_COL FROM MyTable WHERE TEXT_MATCH(QUERY_LOG_TEXT_COL, '\"SELECT dimensionCol2\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, 11787, false, null);

    query = "SELECT INT_COL, QUERY_LOG_TEXT_COL FROM MyTable WHERE QUERY_LOG_TEXT_COL LIKE '\"SELECT dimensionCol2\"' LIMIT 50000";
    testTextSearchSelectQueryHelper(query, 11787, false, null);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(QUERY_LOG_TEXT_COL, '\"SELECT dimensionCol2\"')";
    testTextSearchAggregationQueryHelper(query, 11787);

    // TEST 2: phrase query
    // Search in QUERY_TEXT_LOG column to look for documents where each document MUST contain phrase "SELECT count"
    // as is. In other words, we are trying to find all "SELECT count" style queries from log. The expected result size
    // of 12363 comes from manually doing a grep on the test file.
    query =
        "SELECT INT_COL, QUERY_LOG_TEXT_COL FROM MyTable WHERE TEXT_MATCH(QUERY_LOG_TEXT_COL, '\"SELECT count\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, 12363, false, null);

    query =
        "SELECT INT_COL, QUERY_LOG_TEXT_COL FROM MyTable WHERE QUERY_LOG_TEXT_COL LIKE '\"SELECT count\"' LIMIT 50000";
    testTextSearchSelectQueryHelper(query, 12363, false, null);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(QUERY_LOG_TEXT_COL, '\"SELECT count\"')";
    testTextSearchAggregationQueryHelper(query, 12363);

    query = "SELECT COUNT(*) FROM MyTable WHERE QUERY_LOG_TEXT_COL LIKE '\"SELECT count\"'";
    testTextSearchAggregationQueryHelper(query, 12363);

    // TEST 3: phrase query
    // Search in QUERY_LOG_TEXT_COL to look for documents where each document MUST contain phrase "GROUP BY" as is.
    // In other words, we are trying to find all GROUP BY queries from log. The actual resultset is then also compared
    // to the output of grep since the resultset size is small.
    query =
        "SELECT INT_COL, QUERY_LOG_TEXT_COL FROM MyTable WHERE TEXT_MATCH(QUERY_LOG_TEXT_COL, '\"GROUP BY\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, 26, true, null);

    query =
        "SELECT INT_COL, QUERY_LOG_TEXT_COL FROM MyTable WHERE QUERY_LOG_TEXT_COL LIKE '\"GROUP BY\"' LIMIT 50000";
    testTextSearchSelectQueryHelper(query, 26, true, null);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(QUERY_LOG_TEXT_COL, '\"GROUP BY\"')";
    testTextSearchAggregationQueryHelper(query, 26);

    // TEST 4: phrase query
    // Search in SKILL_TEXT_COL column to look for documents where each document MUST contain phrase "distributed systems"
    // as is. The expected result table is built by doing grep -n -i 'distributed systems' skills.txt
    List<Serializable[]> expected = new ArrayList<>();
    expected.add(
        new Serializable[]{1005, "Distributed systems, Java, C++, Go, distributed query engines for analytics and data warehouses, Machine learning, spark, Kubernetes, transaction processing"});
    expected.add(
        new Serializable[]{1009, "Distributed systems, database development, columnar query engine, database kernel, storage, indexing and transaction processing, building large scale systems"});
    expected.add(
        new Serializable[]{1010, "Distributed systems, Java, realtime streaming systems, Machine learning, spark, Kubernetes, distributed storage, concurrency, multi-threading"});
    expected.add(
        new Serializable[]{1012, "Distributed systems, Java, database engine, cluster management, docker image building and distribution"});
    expected.add(
        new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});
    expected.add(
        new Serializable[]{1020, "Databases, columnar query processing, Apache Arrow, distributed systems, Machine learning, cluster management, docker image building and distribution"});

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Distributed systems\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 5: phrase query
    // Search in SKILLS_TEXT_COL column to look for documents where each document MUST contain phrase
    // "query processing" as is. The expected result table is built by doing grep -n -i 'query processing' skills.txt
    expected = new ArrayList<>();
    expected.add(
        new Serializable[]{1014, "Apache spark, Java, C++, query processing, transaction processing, distributed storage, concurrency, multi-threading, apache airflow"});
    expected.add(
        new Serializable[]{1020, "Databases, columnar query processing, Apache Arrow, distributed systems, Machine learning, cluster management, docker image building and distribution"});

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"query processing\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"query processing\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 6: phrase query
    // Search in SKILLS_TEXT_COL column to look for documents where each document MUST contain the phrase "machine learning"
    // as is. The expected result table is built by doing grep -n -i 'machine learning' skills.txt
    expected = new ArrayList<>();
    expected.add(new Serializable[]{1003, "Java, C++, worked on open source projects, coursera machine learning"});
    expected.add(new Serializable[]{1004, "Machine learning, Tensor flow, Java, Stanford university,"});
    expected.add(
        new Serializable[]{1005, "Distributed systems, Java, C++, Go, distributed query engines for analytics and data warehouses, Machine learning, spark, Kubernetes, transaction processing"});
    expected.add(
        new Serializable[]{1006, "Java, Python, C++, Machine learning, building and deploying large scale production systems, concurrency, multi-threading, CPU processing"});
    expected.add(
        new Serializable[]{1007, "C++, Python, Tensor flow, database kernel, storage, indexing and transaction processing, building large scale systems, Machine learning"});
    expected.add(
        new Serializable[]{1010, "Distributed systems, Java, realtime streaming systems, Machine learning, spark, Kubernetes, distributed storage, concurrency, multi-threading"});
    expected.add(
        new Serializable[]{1011, "CUDA, GPU, Python, Machine learning, database kernel, storage, indexing and transaction processing, building large scale systems"});
    expected.add(
        new Serializable[]{1016, "CUDA, GPU processing, Tensor flow, Pandas, Python, Jupyter notebook, spark, Machine learning, building high performance scalable systems"});
    expected.add(
        new Serializable[]{1019, "C++, Java, Python, realtime streaming systems, Machine learning, spark, Kubernetes, transaction processing, distributed storage, concurrency, multi-threading, apache airflow"});
    expected.add(
        new Serializable[]{1020, "Databases, columnar query processing, Apache Arrow, distributed systems, Machine learning, cluster management, docker image building and distribution"});

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Machine learning\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Machine learning\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 7: composite phrase query using boolean operator AND
    // Search in SKILLS_TEXT_COL column to look for documents where each document MUST contain two independent phrases
    // "machine learning" and "tensor flow" as is. The expected result table is built by doing
    // grep -n -i -E 'machine learning.*tensor flow|tensor flow.*machine learning' skills.txt
    expected = new ArrayList<>();
    expected.add(new Serializable[]{1004, "Machine learning, Tensor flow, Java, Stanford university,"});
    expected.add(
        new Serializable[]{1007, "C++, Python, Tensor flow, database kernel, storage, indexing and transaction processing, building large scale systems, Machine learning"});
    expected.add(
        new Serializable[]{1016, "CUDA, GPU processing, Tensor flow, Pandas, Python, Jupyter notebook, spark, Machine learning, building high performance scalable systems"});

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Machine learning\" AND \"Tensor flow\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query =
        "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Machine learning\" AND \"Tensor flow\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 8: term query
    // Search in SKILLS_TEXT_COL column to look for documents where each document MUST contain term 'Java'.
    // The expected result table is built by doing grep -n -i 'Java' skills.txt
    expected = new ArrayList<>();
    expected.add(new Serializable[]{1000, "Accounts, Banking, Insurance, worked in NGO, Java"});
    expected.add(new Serializable[]{1003, "Java, C++, worked on open source projects, coursera machine learning"});
    expected.add(new Serializable[]{1004, "Machine learning, Tensor flow, Java, Stanford university,"});
    expected.add(
        new Serializable[]{1005, "Distributed systems, Java, C++, Go, distributed query engines for analytics and data warehouses, Machine learning, spark, Kubernetes, transaction processing"});
    expected.add(
        new Serializable[]{1006, "Java, Python, C++, Machine learning, building and deploying large scale production systems, concurrency, multi-threading, CPU processing"});
    expected.add(
        new Serializable[]{1008, "Amazon EC2, AWS, hadoop, big data, spark, building high performance scalable systems, building and deploying large scale production systems, concurrency, multi-threading, Java, C++, CPU processing"});
    expected.add(
        new Serializable[]{1010, "Distributed systems, Java, realtime streaming systems, Machine learning, spark, Kubernetes, distributed storage, concurrency, multi-threading"});
    expected.add(
        new Serializable[]{1012, "Distributed systems, Java, database engine, cluster management, docker image building and distribution"});
    expected.add(
        new Serializable[]{1014, "Apache spark, Java, C++, query processing, transaction processing, distributed storage, concurrency, multi-threading, apache airflow"});
    expected.add(
        new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});
    expected.add(
        new Serializable[]{1018, "Realtime stream processing, publish subscribe, columnar processing for data warehouses, concurrency, Java, multi-threading, C++,"});
    expected.add(
        new Serializable[]{1019, "C++, Java, Python, realtime streaming systems, Machine learning, spark, Kubernetes, transaction processing, distributed storage, concurrency, multi-threading, apache airflow"});

    query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, 'Java') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, 'Java') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 9: composite term query using BOOLEAN operator AND
    // Search in SKILLS_TEXT_COL column to look for documents where each document MUST contain two independent
    // terms 'Java' and 'C++'. The expected result table is built by doing
    // grep -E -n -i 'c\+\+.*java|java.*c\+\+' skills.txt
    expected = new ArrayList<>();
    expected.add(new Serializable[]{1003, "Java, C++, worked on open source projects, coursera machine learning"});
    expected.add(
        new Serializable[]{1005, "Distributed systems, Java, C++, Go, distributed query engines for analytics and data warehouses, Machine learning, spark, Kubernetes, transaction processing"});
    expected.add(
        new Serializable[]{1006, "Java, Python, C++, Machine learning, building and deploying large scale production systems, concurrency, multi-threading, CPU processing"});
    expected.add(
        new Serializable[]{1008, "Amazon EC2, AWS, hadoop, big data, spark, building high performance scalable systems, building and deploying large scale production systems, concurrency, multi-threading, Java, C++, CPU processing"});
    expected.add(
        new Serializable[]{1014, "Apache spark, Java, C++, query processing, transaction processing, distributed storage, concurrency, multi-threading, apache airflow"});
    expected.add(
        new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});
    expected.add(
        new Serializable[]{1018, "Realtime stream processing, publish subscribe, columnar processing for data warehouses, concurrency, Java, multi-threading, C++,"});
    expected.add(
        new Serializable[]{1019, "C++, Java, Python, realtime streaming systems, Machine learning, spark, Kubernetes, transaction processing, distributed storage, concurrency, multi-threading, apache airflow"});

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, 'Java AND C++') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, 'Java AND C++') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 10: phrase query
    // Search in SKILLS_TEXT_COL column to look for documents where each document MUST contain phrase "Java C++" as is.
    // Notice the difference in results with previous term query. in these cases, phrase query does not help a lot since
    // we are essentially relying on user's way of specifying skills in the text document. A user that has both Java and
    // C++ but the latter is not immediately followed after former will be missed with this phrase query. This is where
    // term query using boolean operator seems to be more appropriate as shown in the previous test since all we care
    // is that users should have both skills. Whether Java is written before C++ or vice-versa doesn't matter.
    // Hence term queries are very useful for such cases. The expected result table is built by doing
    // grep -n -i 'Java, C++' skills.txt
    expected = new ArrayList<>();
    expected.add(new Serializable[]{1003, "Java, C++, worked on open source projects, coursera machine learning"});
    expected.add(
        new Serializable[]{1005, "Distributed systems, Java, C++, Go, distributed query engines for analytics and data warehouses, Machine learning, spark, Kubernetes, transaction processing"});
    expected.add(
        new Serializable[]{1008, "Amazon EC2, AWS, hadoop, big data, spark, building high performance scalable systems, building and deploying large scale production systems, concurrency, multi-threading, Java, C++, CPU processing"});
    expected.add(
        new Serializable[]{1014, "Apache spark, Java, C++, query processing, transaction processing, distributed storage, concurrency, multi-threading, apache airflow"});

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Java C++\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Java C++\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 11: composite phrase query using boolean operator AND.
    // Search in SKILLS_TEXT_COL column to look for documents where each document MUST contain two independent phrases
    // "machine learning" and "gpu processing" as is. The expected result table is built by doing
    // grep -n -i -E 'machine learning.*gpu processing|gpu processing.*machine learning' skills.txt
    expected = new ArrayList<>();
    expected.add(
        new Serializable[]{1016, "CUDA, GPU processing, Tensor flow, Pandas, Python, Jupyter notebook, spark, Machine learning, building high performance scalable systems"});

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Machine learning\" AND \"gpu processing\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query =
        "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Machine learning\" AND \"gpu processing\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 12: composite phrase and term query using boolean operator AND.
    // Search in SKILLS_TEXT_COL column to look for documents where each document MUST contain phrase "machine learning"
    // as is and term 'gpu'. Note the difference in result with previous query where 'gpu' was used in the context of
    // phrase "gpu processing" but that resulted in missing out on one row. The expected result table is built by doing
    // grep -n -i -E 'machine learning.*gpu|gpu.*machine learning' skills.txt
    expected = new ArrayList<>();
    expected.add(
        new Serializable[]{1011, "CUDA, GPU, Python, Machine learning, database kernel, storage, indexing and transaction processing, building large scale systems"});
    expected.add(
        new Serializable[]{1016, "CUDA, GPU processing, Tensor flow, Pandas, Python, Jupyter notebook, spark, Machine learning, building high performance scalable systems"});

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Machine learning\" AND gpu') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query =
        "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Machine learning\" AND gpu') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 13: composite phrase and term query using boolean operator AND
    // Search in SKILLS_TEXT_COL column to look for documents where each document MUST contain phrase "machine learning"
    // as is and terms 'gpu' and 'python'. This example shows the usefulness of combining phrases and terms
    // in a single WHERE clause to build a strong search query. The expected result table is built by doing
    // grep -n -i -E 'machine learning.*gpu.*python|gpu.*machine learning.*python|gpu.*python.*machine learning' skills.txt
    expected = new ArrayList<>();
    expected.add(
        new Serializable[]{1011, "CUDA, GPU, Python, Machine learning, database kernel, storage, indexing and transaction processing, building large scale systems"});
    expected.add(
        new Serializable[]{1016, "CUDA, GPU processing, Tensor flow, Pandas, Python, Jupyter notebook, spark, Machine learning, building high performance scalable systems"});

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Machine learning\" AND gpu AND python') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query =
        "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Machine learning\" AND gpu AND python') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 14: term query
    // Search in SKILLS_TEXT_COL column to look for documents that MUST contain term 'apache'. The expected result
    // table is built by doing grep -n -i 'apache' skills.txt
    expected = new ArrayList<>();
    expected.add(
        new Serializable[]{1013, "Kubernetes, cluster management, operating systems, concurrency, multi-threading, apache airflow, Apache Spark,"});
    expected.add(
        new Serializable[]{1014, "Apache spark, Java, C++, query processing, transaction processing, distributed storage, concurrency, multi-threading, apache airflow"});
    expected.add(
        new Serializable[]{1015, "Big data stream processing, Apache Flink, Apache Beam, database kernel, distributed query engines for analytics and data warehouses"});
    expected.add(
        new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});
    expected.add(
        new Serializable[]{1019, "C++, Java, Python, realtime streaming systems, Machine learning, spark, Kubernetes, transaction processing, distributed storage, concurrency, multi-threading, apache airflow"});
    expected.add(
        new Serializable[]{1020, "Databases, columnar query processing, Apache Arrow, distributed systems, Machine learning, cluster management, docker image building and distribution"});
    query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, 'apache') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, 'apache') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 15: composite phrase and term query using boolean operator AND.
    // search in SKILLS_TEXT_COL column to look for documents where each document MUST contain phrase "distributed systems"
    // as is and term 'apache'. The expected result table was built by doing
    // grep -n -i -E 'distributed systems.*apache|apache.*distributed systems' skills.txt
    expected = new ArrayList<>();
    expected.add(
        new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});
    expected.add(
        new Serializable[]{1020, "Databases, columnar query processing, Apache Arrow, distributed systems, Machine learning, cluster management, docker image building and distribution"});

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\" AND apache') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query =
        "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\" AND apache') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 16: term query
    // Search in SKILLS_TEXT_COL column to look for documents where each document MUST contain term 'database'.
    // The expected result table is built by doing grep -n -i 'database' skills.txt
    expected = new ArrayList<>();
    expected.add(
        new Serializable[]{1007, "C++, Python, Tensor flow, database kernel, storage, indexing and transaction processing, building large scale systems, Machine learning"});
    expected.add(
        new Serializable[]{1009, "Distributed systems, database development, columnar query engine, database kernel, storage, indexing and transaction processing, building large scale systems"});
    expected.add(
        new Serializable[]{1011, "CUDA, GPU, Python, Machine learning, database kernel, storage, indexing and transaction processing, building large scale systems"});
    expected.add(
        new Serializable[]{1012, "Distributed systems, Java, database engine, cluster management, docker image building and distribution"});
    expected.add(
        new Serializable[]{1015, "Big data stream processing, Apache Flink, Apache Beam, database kernel, distributed query engines for analytics and data warehouses"});
    expected.add(
        new Serializable[]{1021, "Database engine, OLAP systems, OLTP transaction processing at large scale, concurrency, multi-threading, GO, building large scale systems"});

    query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, 'database') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, 'database') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 17: phrase query
    // Search in SKILLS_TEXT_COL column to look for documents where each document MUST contain phrase "database engine"
    // as is. The expected result table is built by doing grep -n -i 'database engine' skills.txt
    expected = new ArrayList<>();
    expected.add(
        new Serializable[]{1012, "Distributed systems, Java, database engine, cluster management, docker image building and distribution"});
    expected.add(
        new Serializable[]{1021, "Database engine, OLAP systems, OLTP transaction processing at large scale, concurrency, multi-threading, GO, building large scale systems"});

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"database engine\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"database engine\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 18: phrase query
    // Search in SKILLS_TEXT_COL column to look for documents where each document MUST contain phrase
    // "publish subscribe" as is. This is where we see the power of lucene's in-built text parser and tokenizer.
    // Ideally only second row should have been in the output but the parser is intelligent to break publish-subscribe
    // from the original text (even though not separated by white space delimiter) into two different indexable words.
    // The expected result table is built by doing grep -n -i 'publish-subscribe' skills.txt and
    // grep -n -i 'publish subscribe' skills.txt
    expected = new ArrayList<>();
    expected.add(
        new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});
    expected.add(
        new Serializable[]{1018, "Realtime stream processing, publish subscribe, columnar processing for data warehouses, concurrency, Java, multi-threading, C++,"});

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"publish subscribe\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"publish subscribe\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 19: phrase query
    // Search in SKILLS_TEXT_COL column to look for documents where each document MUST contain phrase
    // "accounts banking insurance" as is. The expected result table is built by doing
    // grep -n -i 'accounts, banking, insurance' skills.txt
    expected = new ArrayList<>();
    expected.add(new Serializable[]{1000, "Accounts, Banking, Insurance, worked in NGO, Java"});

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"accounts banking insurance\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query =
        "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"accounts banking insurance\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 20: composite term query with boolean operator AND
    // Search in SKILLS_TEXT_COL column to look for documents where each document MUST contain terms 'accounts' 'banking'
    // and 'insurance' this is another case where a phrase query will not be helpful since the user may have specified
    // the skills in any arbitrary order and so a boolean term query helps a lot in these cases. The expected result table
    // was built by doing grep -n -i 'accounts.*banking.*insurance' skills.txt
    expected = new ArrayList<>();
    expected.add(new Serializable[]{1000, "Accounts, Banking, Insurance, worked in NGO, Java"});
    expected.add(new Serializable[]{1001, "Accounts, Banking, Finance, Insurance"});
    expected.add(new Serializable[]{1002, "Accounts, Finance, Banking, Insurance"});

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, 'accounts AND banking AND insurance') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query =
        "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, 'accounts AND banking AND insurance') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 21: composite phrase and term query using boolean operator AND.
    // Search in SKILLS_TEXT_COL column to look for documents where each document MUST contain ALL the following skills:
    // phrase "distributed systems" as is, term 'Java', term 'C++'. The expected result table was built by doing
    // grep -n -i -E 'distributed systems.*java.*c\+\+|java.*distributed systems.*c\+\+|distributed systems.*c\+\+.*java' skills.txt
    expected = new ArrayList<>();
    expected.add(
        new Serializable[]{1005, "Distributed systems, Java, C++, Go, distributed query engines for analytics and data warehouses, Machine learning, spark, Kubernetes, transaction processing"});
    expected.add(
        new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\" AND Java AND C++') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);
    query =
        "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\" AND Java AND C++') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // test for the index configured to use AND as the default
    // conjunction operator
    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL_1, '\"distributed systems\" Java C++') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);
    query =
        "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL_1, '\"distributed systems\" Java C++') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());
    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL_1, '\"distributed systems\" AND Java AND C++') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);
    query =
        "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL_1, '\"distributed systems\" AND Java AND C++') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // test for the text index configured to not store the default value
    // full index is stored
    query =
        "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL_2, '\"distributed systems\" AND Java AND C++') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());
    // configurable default value is used
    query =
        "SELECT INT_COL, SKILLS_TEXT_COL_2 FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL_2, '\"distributed systems\" AND Java AND C++') LIMIT 50000";
    expected = new ArrayList<>();
    expected.add(new Serializable[]{1005, "ILoveCoding"});
    expected.add(new Serializable[]{1017, "ILoveCoding"});
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    // TEST 22: composite phrase and term query using boolean operator OR
    // Search in SKILLS_TEXT_COL column to look for documents where each document MUST contain ANY of the following skills:
    // phrase "distributed systems" as is, term 'Java', term 'C++'. Note: OR operator is implicit when we don't specify
    // any operator. The expected result table was built by doing grep -n -i -E 'distributed systems|java|c\+\+' skills.txt
    expected = new ArrayList<>();
    expected.add(new Serializable[]{1000, "Accounts, Banking, Insurance, worked in NGO, Java"});
    expected.add(new Serializable[]{1003, "Java, C++, worked on open source projects, coursera machine learning"});
    expected.add(new Serializable[]{1004, "Machine learning, Tensor flow, Java, Stanford university,"});
    expected.add(
        new Serializable[]{1005, "Distributed systems, Java, C++, Go, distributed query engines for analytics and data warehouses, Machine learning, spark, Kubernetes, transaction processing"});
    expected.add(
        new Serializable[]{1006, "Java, Python, C++, Machine learning, building and deploying large scale production systems, concurrency, multi-threading, CPU processing"});
    expected.add(
        new Serializable[]{1007, "C++, Python, Tensor flow, database kernel, storage, indexing and transaction processing, building large scale systems, Machine learning"});
    expected.add(
        new Serializable[]{1008, "Amazon EC2, AWS, hadoop, big data, spark, building high performance scalable systems, building and deploying large scale production systems, concurrency, multi-threading, Java, C++, CPU processing"});
    expected.add(
        new Serializable[]{1009, "Distributed systems, database development, columnar query engine, database kernel, storage, indexing and transaction processing, building large scale systems"});
    expected.add(
        new Serializable[]{1010, "Distributed systems, Java, realtime streaming systems, Machine learning, spark, Kubernetes, distributed storage, concurrency, multi-threading"});
    expected.add(
        new Serializable[]{1012, "Distributed systems, Java, database engine, cluster management, docker image building and distribution"});
    expected.add(
        new Serializable[]{1014, "Apache spark, Java, C++, query processing, transaction processing, distributed storage, concurrency, multi-threading, apache airflow"});
    expected.add(
        new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});
    expected.add(
        new Serializable[]{1018, "Realtime stream processing, publish subscribe, columnar processing for data warehouses, concurrency, Java, multi-threading, C++,"});
    expected.add(
        new Serializable[]{1019, "C++, Java, Python, realtime streaming systems, Machine learning, spark, Kubernetes, transaction processing, distributed storage, concurrency, multi-threading, apache airflow"});
    expected.add(
        new Serializable[]{1020, "Databases, columnar query processing, Apache Arrow, distributed systems, Machine learning, cluster management, docker image building and distribution"});

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\" Java C++') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);
    query =
        "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\" Java C++') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // test for the index configured to use AND as the default
    // conjunction operator
    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL_1, '\"distributed systems\" OR Java OR C++') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);
    query =
        "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL_1, '\"distributed systems\" OR Java OR C++') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 23: composite phrase and term query using both AND and OR
    // Search in SKILLS_TEXT_COL column to look for documents where each document MUST contain phrase "distributed systems"
    // as is and any of the following terms 'Java' or 'C++'. The expected result table was built by doing
    // grep -n -i -E 'distributed systems.*(java|c\+\+)' skills.txt
    expected = new ArrayList<>();
    expected.add(
        new Serializable[]{1005, "Distributed systems, Java, C++, Go, distributed query engines for analytics and data warehouses, Machine learning, spark, Kubernetes, transaction processing"});
    expected.add(
        new Serializable[]{1010, "Distributed systems, Java, realtime streaming systems, Machine learning, spark, Kubernetes, distributed storage, concurrency, multi-threading"});
    expected.add(
        new Serializable[]{1012, "Distributed systems, Java, database engine, cluster management, docker image building and distribution"});
    expected.add(
        new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\" AND (Java C++)') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);
    query =
        "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\" AND (Java C++)') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // test for the index configured to use AND as the default
    // conjunction operator
    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL_1, '\"distributed systems\" AND (Java OR C++)') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);
    query =
        "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL_1, '\"distributed systems\" AND (Java OR C++)') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());
    expected = new ArrayList<>();
    expected.add(
        new Serializable[]{1005, "Distributed systems, Java, C++, Go, distributed query engines for analytics and data warehouses, Machine learning, spark, Kubernetes, transaction processing"});
    expected.add(
        new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});
    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL_1, '\"distributed systems\" AND (Java C++)') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);
    query =
        "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL_1, '\"distributed systems\" AND (Java C++)') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 24: prefix query
    // Search in SKILLS_TEXT_COL column to look for documents that have stream* -- stream, streaming, streams etc.
    // The expected result table was built by doing grep -n -i -E 'stream' skills.txt
    expected = new ArrayList<>();
    expected.add(
        new Serializable[]{1010, "Distributed systems, Java, realtime streaming systems, Machine learning, spark, Kubernetes, distributed storage, concurrency, multi-threading"});
    expected.add(
        new Serializable[]{1015, "Big data stream processing, Apache Flink, Apache Beam, database kernel, distributed query engines for analytics and data warehouses"});
    expected.add(
        new Serializable[]{1018, "Realtime stream processing, publish subscribe, columnar processing for data warehouses, concurrency, Java, multi-threading, C++,"});
    expected.add(
        new Serializable[]{1019, "C++, Java, Python, realtime streaming systems, Machine learning, spark, Kubernetes, transaction processing, distributed storage, concurrency, multi-threading, apache airflow"});

    query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, 'stream*') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, 'stream*') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 25: regex query
    // Search in SKILLS_TEXT_COL column to look for documents that have 'exception'.
    // Based on how 'exception' is present in the original text, it won't be tokenized into an individual
    // indexable token/term. Hence it won't be available in the in the index on its own. It will be  present as part
    // of larger token depending on where the word boundary is. So we need to use Lucene regex query.
    expected = new ArrayList<>();
    expected.add(
        new Serializable[]{1022, "GET /administrator/ HTTP/1.1 200 4263 - Mozilla/5.0 (Windows NT 6.0; rv:34.0) Gecko/20100101 Firefox/34.0 - NullPointerException"});

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '/.*exception/') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '/.*exception/') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());
  }

  /**
   * Tests for combining (using AND/OR)
   * the execution of text match filters with other filters.
   */
  @Test
  public void testTextSearchWithAdditionalFilter()
      throws Exception {
    // TEST 1: combine an index based doc id iterator (text_match) with scan based doc id iterator (range >= ) using AND
    List<Serializable[]> expected = new ArrayList<>();
    expected.add(
        new Serializable[]{1010, "Distributed systems, Java, realtime streaming systems, Machine learning, spark, Kubernetes, distributed storage, concurrency, multi-threading"});
    expected.add(
        new Serializable[]{1012, "Distributed systems, Java, database engine, cluster management, docker image building and distribution"});
    expected.add(
        new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});
    expected.add(
        new Serializable[]{1020, "Databases, columnar query processing, Apache Arrow, distributed systems, Machine learning, cluster management, docker image building and distribution"});

    String query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE INT_COL >= 1010 AND TEXT_MATCH(SKILLS_TEXT_COL, '\"Distributed systems\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);
    query =
        "SELECT COUNT(*) FROM MyTable WHERE INT_COL >= 1010 AND TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE INT_COL >= 1010 AND TEXT_MATCH(SKILLS_TEXT_COL_DICT, '\"Distributed systems\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);
    query =
        "SELECT COUNT(*) FROM MyTable WHERE INT_COL >= 1010 AND TEXT_MATCH(SKILLS_TEXT_COL_DICT, '\"distributed systems\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 2: combine an index based doc id iterator (text_match) with scan based doc id iterator (range <= ) using AND
    expected = new ArrayList<>();
    expected.add(
        new Serializable[]{1005, "Distributed systems, Java, C++, Go, distributed query engines for analytics and data warehouses, Machine learning, spark, Kubernetes, transaction processing"});
    expected.add(
        new Serializable[]{1009, "Distributed systems, database development, columnar query engine, database kernel, storage, indexing and transaction processing, building large scale systems"});
    expected.add(
        new Serializable[]{1010, "Distributed systems, Java, realtime streaming systems, Machine learning, spark, Kubernetes, distributed storage, concurrency, multi-threading"});

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE INT_COL <= 1010 AND TEXT_MATCH(SKILLS_TEXT_COL, '\"Distributed systems\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);
    query =
        "SELECT COUNT(*) FROM MyTable WHERE INT_COL <= 1010 AND TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE INT_COL <= 1010 AND TEXT_MATCH(SKILLS_TEXT_COL_DICT, '\"Distributed systems\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);
    query =
        "SELECT COUNT(*) FROM MyTable WHERE INT_COL <= 1010 AND TEXT_MATCH(SKILLS_TEXT_COL_DICT, '\"distributed systems\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 3: combine an index based doc id iterator (text_match) with scan based doc id iterator (range >= ) using OR
    query =
        "SELECT COUNT(*) FROM MyTable WHERE INT_COL >= 1010 OR TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, 24142);

    query =
        "SELECT COUNT(*) FROM MyTable WHERE INT_COL >= 1010 OR TEXT_MATCH(SKILLS_TEXT_COL_DICT, '\"distributed systems\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, 24142);

    // TEST 4: combine an index based doc id iterator (text_match) with scan based doc id iterator (range <= ) using OR
    expected = new ArrayList<>();
    expected.add(new Serializable[]{1000, "Accounts, Banking, Insurance, worked in NGO, Java"});
    expected.add(new Serializable[]{1001, "Accounts, Banking, Finance, Insurance"});
    expected.add(new Serializable[]{1002, "Accounts, Finance, Banking, Insurance"});
    expected.add(new Serializable[]{1003, "Java, C++, worked on open source projects, coursera machine learning"});
    expected.add(new Serializable[]{1004, "Machine learning, Tensor flow, Java, Stanford university,"});
    expected.add(
        new Serializable[]{1005, "Distributed systems, Java, C++, Go, distributed query engines for analytics and data warehouses, Machine learning, spark, Kubernetes, transaction processing"});
    expected.add(
        new Serializable[]{1006, "Java, Python, C++, Machine learning, building and deploying large scale production systems, concurrency, multi-threading, CPU processing"});
    expected.add(
        new Serializable[]{1007, "C++, Python, Tensor flow, database kernel, storage, indexing and transaction processing, building large scale systems, Machine learning"});
    expected.add(
        new Serializable[]{1008, "Amazon EC2, AWS, hadoop, big data, spark, building high performance scalable systems, building and deploying large scale production systems, concurrency, multi-threading, Java, C++, CPU processing"});
    expected.add(
        new Serializable[]{1009, "Distributed systems, database development, columnar query engine, database kernel, storage, indexing and transaction processing, building large scale systems"});
    expected.add(
        new Serializable[]{1010, "Distributed systems, Java, realtime streaming systems, Machine learning, spark, Kubernetes, distributed storage, concurrency, multi-threading"});
    expected.add(
        new Serializable[]{1012, "Distributed systems, Java, database engine, cluster management, docker image building and distribution"});
    expected.add(
        new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});
    expected.add(
        new Serializable[]{1020, "Databases, columnar query processing, Apache Arrow, distributed systems, Machine learning, cluster management, docker image building and distribution"});

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE INT_COL <= 1010 OR TEXT_MATCH(SKILLS_TEXT_COL, '\"Distributed systems\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);
    query =
        "SELECT COUNT(*) FROM MyTable WHERE INT_COL <= 1010 OR TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE INT_COL <= 1010 OR TEXT_MATCH(SKILLS_TEXT_COL_DICT, '\"Distributed systems\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);
    query =
        "SELECT COUNT(*) FROM MyTable WHERE INT_COL <= 1010 OR TEXT_MATCH(SKILLS_TEXT_COL_DICT, '\"distributed systems\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 5: combine an index based doc id iterator (text_match) with sorted inverted index doc id iterator (equality) using AND
    expected = new ArrayList<>();
    expected.add(
        new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE INT_COL = 1017 AND TEXT_MATCH(SKILLS_TEXT_COL, '\"Distributed systems\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);
    query =
        "SELECT COUNT(*) FROM MyTable WHERE INT_COL = 1017 AND TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE INT_COL = 1017 AND TEXT_MATCH(SKILLS_TEXT_COL_DICT, '\"Distributed systems\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);
    query =
        "SELECT COUNT(*) FROM MyTable WHERE INT_COL = 1017 AND TEXT_MATCH(SKILLS_TEXT_COL_DICT, '\"distributed systems\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 6: combine an index based doc id iterator (text_match) with sorted inverted index doc id iterator (equality) using OR
    expected = new ArrayList<>();
    expected.add(new Serializable[]{1005});
    expected.add(new Serializable[]{1009});
    expected.add(new Serializable[]{1010});
    expected.add(new Serializable[]{1012});
    expected.add(new Serializable[]{1017});
    expected.add(new Serializable[]{1020});

    query =
        "SELECT INT_COL FROM MyTable WHERE INT_COL = 1017 OR TEXT_MATCH(SKILLS_TEXT_COL, '\"Distributed systems\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);
    query =
        "SELECT COUNT(*) FROM MyTable WHERE INT_COL = 1017 OR TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    query =
        "SELECT INT_COL FROM MyTable WHERE INT_COL = 1017 OR TEXT_MATCH(SKILLS_TEXT_COL_DICT, '\"Distributed systems\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);
    query =
        "SELECT COUNT(*) FROM MyTable WHERE INT_COL = 1017 OR TEXT_MATCH(SKILLS_TEXT_COL_DICT, '\"distributed systems\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 7: combine an index based doc id iterator (text_match) with another index based doc id iterator (text_match) using AND
    expected = new ArrayList<>();
    expected.add(new Serializable[]{1005});
    expected.add(new Serializable[]{1009});
    expected.add(new Serializable[]{1010});
    expected.add(new Serializable[]{1012});
    expected.add(new Serializable[]{1017});
    expected.add(new Serializable[]{1020});

    query =
        "SELECT INT_COL FROM MyTable WHERE TEXT_MATCH(QUERY_LOG_TEXT_COL, '\"SELECT count\"') AND TEXT_MATCH(SKILLS_TEXT_COL, '\"Distributed systems\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);
    query =
        "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(QUERY_LOG_TEXT_COL, '\"SELECT count\"') AND TEXT_MATCH(SKILLS_TEXT_COL, '\"Distributed systems\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST 8: combine an index based doc id iterator (text_match) with another index based doc id iterator (text_match) using OR
    expected = new ArrayList<>();
    expected.add(new Serializable[]{1005});
    expected.add(new Serializable[]{1009});
    expected.add(new Serializable[]{1010});
    expected.add(new Serializable[]{1012});
    expected.add(new Serializable[]{1013});
    expected.add(new Serializable[]{1014});
    expected.add(new Serializable[]{1015});
    expected.add(new Serializable[]{1017});
    expected.add(new Serializable[]{1019});
    expected.add(new Serializable[]{1020});

    query =
        "SELECT INT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, 'apache') OR TEXT_MATCH(SKILLS_TEXT_COL, '\"Distributed systems\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);
    query =
        "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, 'apache') OR TEXT_MATCH(SKILLS_TEXT_COL, '\"Distributed systems\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // since we support text index on dictionary encoded columns, the column might
    // also have native pinot inverted index in addition to text index.
    // so this query tests the exact match on the text column which will use the
    // native inverted index.
    expected = new ArrayList<>();
    expected.add(new Serializable[]{1004});
    query =
        "SELECT INT_COL FROM MyTable WHERE SKILLS_TEXT_COL_DICT = 'Machine learning, Tensor flow, Java, Stanford university,' LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);
    query =
        "SELECT COUNT(*) FROM MyTable WHERE SKILLS_TEXT_COL_DICT = 'Machine learning, Tensor flow, Java, Stanford university,' LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // since we support text index on dictionary encoded columns, the column might
    // also have native pinot inverted index in addition to text index.
    // so this query tests the exact match on the text column which will use the
    // native inverted index along with text match on the text column which will
    // use the text index
    expected = new ArrayList<>();
    expected.add(new Serializable[]{1003});
    expected.add(new Serializable[]{1004});
    expected.add(new Serializable[]{1005});
    expected.add(new Serializable[]{1006});
    expected.add(new Serializable[]{1007});
    expected.add(new Serializable[]{1010});
    expected.add(new Serializable[]{1011});
    expected.add(new Serializable[]{1016});
    expected.add(new Serializable[]{1019});
    expected.add(new Serializable[]{1020});

    query =
        "SELECT INT_COL FROM MyTable WHERE SKILLS_TEXT_COL_DICT = 'Machine learning, Tensor flow, Java, Stanford university,' OR TEXT_MATCH(SKILLS_TEXT_COL_DICT, '\"machine learning\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);
    query =
        "SELECT COUNT(*) FROM MyTable WHERE SKILLS_TEXT_COL_DICT = 'Machine learning, Tensor flow, Java, Stanford university,' OR TEXT_MATCH(SKILLS_TEXT_COL_DICT, '\"machine learning\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());
  }

  /**
   * Test the reference counting mechanism of {@link SearcherManager}
   * used by {@link org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneTextIndexReader}
   * for near realtime text search.
   */
  @Test
  public void testLuceneRealtimeWithSearcherManager()
      throws Exception {
    // create and open an index writer
    File indexFile = new File(INDEX_DIR.getPath() + "/realtime-test1.index");
    Directory indexDirectory = FSDirectory.open(indexFile.toPath());
    StandardAnalyzer standardAnalyzer = new StandardAnalyzer();
    IndexWriterConfig indexWriterConfig = new IndexWriterConfig(standardAnalyzer);
    indexWriterConfig.setRAMBufferSizeMB(500);
    IndexWriter indexWriter = new IndexWriter(indexDirectory, indexWriterConfig);

    // create an NRT index reader
    SearcherManager searcherManager = new SearcherManager(indexWriter, false, false, null);

    QueryParser queryParser = new QueryParser("skill", standardAnalyzer);
    Query query = queryParser.parse("\"machine learning\"");

    // acquire a searcher
    // the creation of SearcherManager would have created an IndexReader with a
    // refcount of 1. manager.acquire would have bumped up the refcount by 1
    // so the refcount should be 2 now
    // search() should not see any hits since nothing has been added to the index
    IndexSearcher searcher1 = searcherManager.acquire();
    Assert.assertEquals(2, searcher1.getIndexReader().getRefCount());
    Assert.assertEquals(0, searcher1.search(query, 100).scoreDocs.length);

    // acquire a searcher
    // since refresh hasn't happenend yet, this should be the same searcher as searcher1
    // but with refcount incremented. so refcount should be 3 now
    // search() should not see any hits since nothing has been added to the index
    IndexSearcher searcher2 = searcherManager.acquire();
    Assert.assertEquals(3, searcher2.getIndexReader().getRefCount());
    Assert.assertEquals(0, searcher2.search(query, 100).scoreDocs.length);
    Assert.assertEquals(searcher1, searcher2);
    Assert.assertEquals(3, searcher1.getIndexReader().getRefCount());
    Assert.assertEquals(0, searcher1.search(query, 100).scoreDocs.length);

    // add something to the index but don't commit
    Document docToIndex = new Document();
    docToIndex.add(new TextField("skill", "machine learning", Field.Store.NO));
    indexWriter.addDocument(docToIndex);

    // refresh the searcher inside SearcherManager
    // this
    searcherManager.maybeRefresh();

    // acquire a searcher
    // this should be the refreshed one
    // the refcount of the reader associated with this searcher
    // should be 2 (1 as the initial refcount of the reader and +1
    // due to acquire)
    IndexSearcher searcher3 = searcherManager.acquire();
    Assert.assertEquals(2, searcher3.getIndexReader().getRefCount());
    // this searcher should see the uncommitted document in the index
    Assert.assertEquals(1, searcher3.search(query, 100).scoreDocs.length);
    Assert.assertNotEquals(searcher2, searcher3);

    // searcher1 and searcher2 ref count should have gone down by 1 due to refresh
    // since they were the current searcher before refresh happened and after SearcherManager
    // got new searcher after refresh, it decrements the ref count for old one.
    Assert.assertEquals(2, searcher1.getIndexReader().getRefCount());
    Assert.assertEquals(0, searcher1.search(query, 100).scoreDocs.length);
    Assert.assertEquals(2, searcher2.getIndexReader().getRefCount());
    Assert.assertEquals(0, searcher2.search(query, 100).scoreDocs.length);

    // done searching with searcher1
    // release it -- this should decrement the refcount by 1 for both searcher1
    // and searcher2 since they are same
    searcherManager.release(searcher1);
    Assert.assertEquals(1, searcher1.getIndexReader().getRefCount());
    Assert.assertEquals(1, searcher2.getIndexReader().getRefCount());
    // the above release should not have impacted searcher3
    Assert.assertEquals(2, searcher3.getIndexReader().getRefCount());
    Assert.assertEquals(1, searcher3.search(query, 100).scoreDocs.length);

    // done searching with searcher2
    // release it -- this should decrement the refcount by 1 for both searcher1
    // and searcher2 since they are same
    // this gets the refcount to 0 and the associated reader is closed
    searcherManager.release(searcher2);
    Assert.assertEquals(0, searcher1.getIndexReader().getRefCount());
    Assert.assertEquals(0, searcher2.getIndexReader().getRefCount());
    // the above release should not have impacted searcher3
    Assert.assertEquals(2, searcher3.getIndexReader().getRefCount());
    Assert.assertEquals(1, searcher3.search(query, 100).scoreDocs.length);

    // add another document to the index but don't commit
    docToIndex = new Document();
    docToIndex.add(new TextField("skill", "java, machine learning", Field.Store.NO));
    indexWriter.addDocument(docToIndex);

    // searcher3 should not see the second document
    Assert.assertEquals(2, searcher3.getIndexReader().getRefCount());
    Assert.assertEquals(1, searcher3.search(query, 100).scoreDocs.length);

    // refresh
    searcherManager.maybeRefresh();

    // refresh would have resulted in a new current searcher inside searcher
    // manager and decremented the ref count of previous current searcher
    // (searcher3)
    Assert.assertEquals(1, searcher3.getIndexReader().getRefCount());
    Assert.assertEquals(1, searcher3.search(query, 100).scoreDocs.length);

    // acquire a searcher
    // this should be the refreshed one
    // the refcount of the reader associated with this searcher
    // should be 2 (1 as the initial refcount of the reader and +1
    // due to acquire)
    IndexSearcher searcher4 = searcherManager.acquire();
    Assert.assertEquals(2, searcher4.getIndexReader().getRefCount());
    // we should see both the uncommitted documents with the refreshed searcher
    Assert.assertEquals(2, searcher4.search(query, 100).scoreDocs.length);
    // searcher3 should not have been affected by the new acquire
    Assert.assertEquals(1, searcher3.getIndexReader().getRefCount());
    Assert.assertEquals(1, searcher3.search(query, 100).scoreDocs.length);

    // done searching with searcher3
    // release it -- this should decrement its refcount to 0
    // and close the associated reader
    searcherManager.release(searcher3);
    Assert.assertEquals(0, searcher1.getIndexReader().getRefCount());
    Assert.assertEquals(0, searcher2.getIndexReader().getRefCount());
    Assert.assertEquals(0, searcher3.getIndexReader().getRefCount());
    // searcher4 should not have been impacted by above release
    Assert.assertEquals(2, searcher4.getIndexReader().getRefCount());
    Assert.assertEquals(2, searcher4.search(query, 100).scoreDocs.length);

    // refresh
    searcherManager.maybeRefresh();

    // the above refresh is essentially a NOOP since nothing has changed
    // in the index. so any acquire after the above refresh will return
    // the same searcher as searcher4 but with 1 more refcount
    IndexSearcher searcher5 = searcherManager.acquire();
    Assert.assertEquals(3, searcher4.getIndexReader().getRefCount());
    Assert.assertEquals(2, searcher4.search(query, 100).scoreDocs.length);
    Assert.assertEquals(3, searcher5.getIndexReader().getRefCount());
    Assert.assertEquals(2, searcher5.search(query, 100).scoreDocs.length);
    Assert.assertEquals(searcher4, searcher5);

    searcherManager.release(searcher4);
    Assert.assertEquals(0, searcher1.getIndexReader().getRefCount());
    Assert.assertEquals(0, searcher2.getIndexReader().getRefCount());
    Assert.assertEquals(0, searcher3.getIndexReader().getRefCount());
    Assert.assertEquals(2, searcher4.getIndexReader().getRefCount());
    Assert.assertEquals(2, searcher4.search(query, 100).scoreDocs.length);
    Assert.assertEquals(2, searcher5.getIndexReader().getRefCount());
    Assert.assertEquals(2, searcher5.search(query, 100).scoreDocs.length);

    searcherManager.release(searcher5);
    Assert.assertEquals(0, searcher1.getIndexReader().getRefCount());
    Assert.assertEquals(0, searcher2.getIndexReader().getRefCount());
    Assert.assertEquals(0, searcher3.getIndexReader().getRefCount());
    Assert.assertEquals(1, searcher4.getIndexReader().getRefCount());
    Assert.assertEquals(2, searcher4.search(query, 100).scoreDocs.length);
    Assert.assertEquals(1, searcher5.getIndexReader().getRefCount());
    Assert.assertEquals(2, searcher5.search(query, 100).scoreDocs.length);

    searcherManager.close();
    Assert.assertEquals(0, searcher4.getIndexReader().getRefCount());
    Assert.assertEquals(0, searcher5.getIndexReader().getRefCount());
    indexWriter.close();
  }

  /**
   * Test the realtime search by verifying that realtime reader is able
   * to see monotonically increasing number of uncommitted documents
   * added to the index.
   */
  @Test
  public void testLuceneRealtimeWithoutSearcherManager()
      throws Exception {
    // create and open an index writer
    File indexFile = new File(INDEX_DIR.getPath() + "/realtime-test2.index");
    Directory indexDirectory = FSDirectory.open(indexFile.toPath());
    StandardAnalyzer standardAnalyzer = new StandardAnalyzer();
    IndexWriterConfig indexWriterConfig = new IndexWriterConfig(standardAnalyzer);
    indexWriterConfig.setRAMBufferSizeMB(50);
    IndexWriter indexWriter = new IndexWriter(indexDirectory, indexWriterConfig);

    // add a document but don't commit
    Document docToIndex = new Document();
    docToIndex.add(new TextField("skill", "distributed systems, machine learning, JAVA, C++", Field.Store.NO));
    indexWriter.addDocument(docToIndex);

    // create an NRT index reader from the writer -- should see one uncommitted document
    QueryParser queryParser = new QueryParser("skill", standardAnalyzer);
    Query query = queryParser.parse("\"distributed systems\" AND (Java C++)");
    IndexReader indexReader1 = DirectoryReader.open(indexWriter);
    IndexSearcher searcher1 = new IndexSearcher(indexReader1);
    Assert.assertEquals(1, searcher1.search(query, 50).scoreDocs.length);

    // add another document but don't commit
    docToIndex = new Document();
    docToIndex.add(new TextField("skill", "distributed systems, python, JAVA, C++", Field.Store.NO));
    indexWriter.addDocument(docToIndex);

    // reopen NRT reader and search -- should see two uncommitted documents
    IndexReader indexReader2 = DirectoryReader.openIfChanged((DirectoryReader) indexReader1);
    Assert.assertNotNull(indexReader2);
    IndexSearcher searcher2 = new IndexSearcher(indexReader2);
    Assert.assertEquals(2, searcher2.search(query, 50).scoreDocs.length);

    // add another document
    docToIndex = new Document();
    docToIndex.add(new TextField("skill", "distributed systems, GPU, JAVA, C++", Field.Store.NO));
    indexWriter.addDocument(docToIndex);

    // reopen NRT reader and search -- should see three uncommitted documents
    IndexReader indexReader3 = DirectoryReader.openIfChanged((DirectoryReader) indexReader2);
    Assert.assertNotNull(indexReader3);
    IndexSearcher searcher3 = new IndexSearcher(indexReader3);
    Assert.assertEquals(3, searcher3.search(query, 50).scoreDocs.length);

    indexWriter.close();
    indexReader1.close();
    indexReader2.close();
    indexReader3.close();
  }

  @Test
  public void testMultiThreadedLuceneRealtime()
      throws Exception {
    File indexFile = new File(INDEX_DIR.getPath() + "/realtime-test3.index");
    Directory indexDirectory = FSDirectory.open(indexFile.toPath());
    StandardAnalyzer standardAnalyzer = new StandardAnalyzer();
    // create and open a writer
    IndexWriterConfig indexWriterConfig = new IndexWriterConfig(standardAnalyzer);
    indexWriterConfig.setRAMBufferSizeMB(500);
    IndexWriter indexWriter = new IndexWriter(indexDirectory, indexWriterConfig);

    // create an NRT index reader
    SearcherManager searcherManager = new SearcherManager(indexWriter, false, false, null);

    // background thread to refresh NRT reader
    ControlledRealTimeReopenThread controlledRealTimeReopenThread =
        new ControlledRealTimeReopenThread(indexWriter, searcherManager, 0.01, 0.01);
    controlledRealTimeReopenThread.start();

    // start writer and reader
    Thread writer = new Thread(new RealtimeWriter(indexWriter));
    Thread realtimeReader = new Thread(new RealtimeReader(searcherManager, standardAnalyzer));

    writer.start();
    realtimeReader.start();

    writer.join();
    realtimeReader.join();
    controlledRealTimeReopenThread.join();
  }

  private static class RealtimeWriter implements Runnable {
    private final IndexWriter indexWriter;

    RealtimeWriter(IndexWriter indexWriter) {
      this.indexWriter = indexWriter;
    }

    @Override
    public void run() {
      URL resourceUrl = getClass().getClassLoader().getResource("data/text_search_data/skills.txt");
      File skillFile = new File(resourceUrl.getFile());
      String[] skills = new String[100];

      int skillCount = 0;
      try (InputStream inputStream = new FileInputStream(skillFile);
          BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
        String line;
        while ((line = reader.readLine()) != null) {
          skills[skillCount++] = line;
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while reading skills file");
      }

      try {
        int counter = 0;
        Random random = new Random();
        // ingest 500k documents
        while (counter < 500000) {
          Document docToIndex = new Document();
          if (counter >= skillCount) {
            int index = random.nextInt(skillCount);
            docToIndex.add(new TextField("skill", skills[index], Field.Store.NO));
          } else {
            docToIndex.add(new TextField("skill", skills[counter], Field.Store.NO));
          }
          counter++;
          indexWriter.addDocument(docToIndex);
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while adding a document to index");
      } finally {
        try {
          indexWriter.commit();
          indexWriter.close();
        } catch (Exception e) {
          throw new RuntimeException("Failed to commit/close the index writer");
        }
      }
    }
  }

  private static class RealtimeReader implements Runnable {
    private final QueryParser queryParser;
    private final SearcherManager searchManager;

    RealtimeReader(SearcherManager searcherManager, StandardAnalyzer standardAnalyzer) {
      this.queryParser = new QueryParser("skill", standardAnalyzer);
      this.searchManager = searcherManager;
    }

    @Override
    public void run() {
      try {
        Query query = queryParser.parse("\"machine learning\" AND spark");
        int count = 0;
        int prevHits = 0;
        // run the same query 1000 times and see in increasing number of hits
        // in the index
        while (count < 1000) {
          IndexSearcher indexSearcher = searchManager.acquire();
          int hits = indexSearcher.search(query, Integer.MAX_VALUE).scoreDocs.length;
          // TODO: see how we can make this more deterministic
          if (count > 200) {
            // we should see an increasing number of hits
            Assert.assertTrue(hits > 0);
            Assert.assertTrue(hits >= prevHits);
          }
          count++;
          prevHits = hits;
          searchManager.release(indexSearcher);
          Thread.sleep(1);
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception in realtime reader");
      }
    }
  }

  /*
   * Helper methods for tests
   */

  private void testTextSearchSelectQueryHelper(String query, int expectedResultSize, boolean compareGrepOutput,
      List<Serializable[]> expectedResults)
      throws Exception {
    SelectionOnlyOperator operator = getOperatorForPqlQuery(query);
    IntermediateResultsBlock operatorResult = operator.nextBlock();
    List<Object[]> resultset = (List<Object[]>) operatorResult.getSelectionResult();
    Assert.assertNotNull(resultset);
    Assert.assertEquals(resultset.size(), expectedResultSize);
    if (compareGrepOutput) {
      // compare with grep output
      verifySearchOutputWithGrepResults(resultset);
    } else if (expectedResults != null) {
      // compare with expected result table
      for (int i = 0; i < expectedResultSize; i++) {
        Object[] actualRow = resultset.get(i);
        Object[] expectedRow = expectedResults.get(i);
        Assert.assertEquals(actualRow.length, expectedRow.length);
        for (int j = 0; j < actualRow.length; j++) {
          Object actualColValue = actualRow[j];
          Object expectedColValue = expectedRow[j];
          Assert.assertEquals(actualColValue, expectedColValue);
        }
      }
    }
  }

  private void verifySearchOutputWithGrepResults(List<Object[]> actualResultSet)
      throws Exception {
    URL resourceUrl = getClass().getClassLoader().getResource("data/text_search_data/group_by_grep_results.out");
    File file = new File(resourceUrl.getFile());
    InputStream inputStream = new FileInputStream(file);
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    String line;
    int counter = 0;
    while ((line = reader.readLine()) != null) {
      String[] expectedRow = line.split(":");
      Object[] actualRow = actualResultSet.get(counter);
      int expectedIntColValue = Integer.valueOf(expectedRow[0]) + INT_BASE_VALUE - 1;
      Assert.assertEquals(expectedIntColValue, actualRow[0]);
      Assert.assertEquals(expectedRow[1], actualRow[1]);
      counter++;
    }
  }

  private void testTextSearchAggregationQueryHelper(String query, int expectedCount) {
    AggregationOperator operator = getOperatorForPqlQuery(query);
    IntermediateResultsBlock operatorResult = operator.nextBlock();
    long count = (Long) operatorResult.getAggregationResult().get(0);
    Assert.assertEquals(expectedCount, count);
  }

  @Test
  public void testInterSegment() {
    String query =
        "SELECT count(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Machine learning\" AND \"Tensor flow\"') LIMIT 50000";
    testInterSegmentAggregationQueryHelper(query, 12);
    query =
        "SELECT count(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL_DICT, '\"Machine learning\" AND \"Tensor flow\"') LIMIT 50000";
    testInterSegmentAggregationQueryHelper(query, 12);

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Machine learning\" AND \"Tensor flow\"') LIMIT 50000";
    List<Serializable[]> expected = new ArrayList<>();
    expected.add(new Serializable[]{1004, "Machine learning, Tensor flow, Java, Stanford university,"});
    expected.add(
        new Serializable[]{1007, "C++, Python, Tensor flow, database kernel, storage, indexing and transaction processing, building large scale systems, Machine learning"});
    expected.add(
        new Serializable[]{1016, "CUDA, GPU processing, Tensor flow, Pandas, Python, Jupyter notebook, spark, Machine learning, building high performance scalable systems"});
    expected.add(new Serializable[]{1004, "Machine learning, Tensor flow, Java, Stanford university,"});
    expected.add(
        new Serializable[]{1007, "C++, Python, Tensor flow, database kernel, storage, indexing and transaction processing, building large scale systems, Machine learning"});
    expected.add(
        new Serializable[]{1016, "CUDA, GPU processing, Tensor flow, Pandas, Python, Jupyter notebook, spark, Machine learning, building high performance scalable systems"});
    expected.add(new Serializable[]{1004, "Machine learning, Tensor flow, Java, Stanford university,"});
    expected.add(
        new Serializable[]{1007, "C++, Python, Tensor flow, database kernel, storage, indexing and transaction processing, building large scale systems, Machine learning"});
    expected.add(
        new Serializable[]{1016, "CUDA, GPU processing, Tensor flow, Pandas, Python, Jupyter notebook, spark, Machine learning, building high performance scalable systems"});
    expected.add(new Serializable[]{1004, "Machine learning, Tensor flow, Java, Stanford university,"});
    expected.add(
        new Serializable[]{1007, "C++, Python, Tensor flow, database kernel, storage, indexing and transaction processing, building large scale systems, Machine learning"});
    expected.add(
        new Serializable[]{1016, "CUDA, GPU processing, Tensor flow, Pandas, Python, Jupyter notebook, spark, Machine learning, building high performance scalable systems"});
    testInterSegmentSelectionQueryHelper(query, expected);
    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL_DICT, '\"Machine learning\" AND \"Tensor flow\"') LIMIT 50000";
    testInterSegmentSelectionQueryHelper(query, expected);

    // try arbitrary filters in search expressions
    query =
        "SELECT count(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '(\"distributed systems\" AND apache) OR (Java AND C++)') LIMIT 50000";
    testInterSegmentAggregationQueryHelper(query, 36);
    query =
        "SELECT count(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL_DICT, '(\"distributed systems\" AND apache) OR (Java AND C++)') LIMIT 50000";
    testInterSegmentAggregationQueryHelper(query, 36);

    query =
        "SELECT count(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '(\"distributed systems\" AND apache) AND (Java AND C++)') LIMIT 50000";
    testInterSegmentAggregationQueryHelper(query, 4);
    query =
        "SELECT count(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL_DICT, '(\"distributed systems\" AND apache) AND (Java AND C++)') LIMIT 50000";
    testInterSegmentAggregationQueryHelper(query, 4);

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '(\"distributed systems\" AND apache) AND (Java AND C++)') LIMIT 50000";
    expected = new ArrayList<>();
    expected.add(
        new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});
    expected.add(
        new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});
    expected.add(
        new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});
    expected.add(
        new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});
    testInterSegmentSelectionQueryHelper(query, expected);

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL_DICT, '(\"distributed systems\" AND apache) AND (Java AND C++)') LIMIT 50000";
    testInterSegmentSelectionQueryHelper(query, expected);

    query =
        "SELECT count(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '(\"apache spark\" OR \"query processing\") AND \"machine learning\"') LIMIT 50000";
    testInterSegmentAggregationQueryHelper(query, 4);
    query =
        "SELECT count(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL_DICT, '(\"apache spark\" OR \"query processing\") AND \"machine learning\"') LIMIT 50000";
    testInterSegmentAggregationQueryHelper(query, 4);

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '(\"apache spark\" OR \"query processing\") AND \"machine learning\"') LIMIT 50000";
    expected = new ArrayList<>();
    expected.add(
        new Serializable[]{1020, "Databases, columnar query processing, Apache Arrow, distributed systems, Machine learning, cluster management, docker image building and distribution"});
    expected.add(
        new Serializable[]{1020, "Databases, columnar query processing, Apache Arrow, distributed systems, Machine learning, cluster management, docker image building and distribution"});
    expected.add(
        new Serializable[]{1020, "Databases, columnar query processing, Apache Arrow, distributed systems, Machine learning, cluster management, docker image building and distribution"});
    expected.add(
        new Serializable[]{1020, "Databases, columnar query processing, Apache Arrow, distributed systems, Machine learning, cluster management, docker image building and distribution"});
    testInterSegmentSelectionQueryHelper(query, expected);

    query =
        "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL_DICT, '(\"apache spark\" OR \"query processing\") AND \"machine learning\"') LIMIT 50000";
    testInterSegmentSelectionQueryHelper(query, expected);

    // query with only stop-words. they should not be indexed
    query = "SELECT count(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, 'a and or in the are')";
    testInterSegmentAggregationQueryHelper(query, 0);
    query = "SELECT count(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL_DICT, 'a and or in the are')";
    testInterSegmentAggregationQueryHelper(query, 0);
    // analyzer should prune/ignore the stop words from search expression and consider everything else for a match
    query = "SELECT count(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"learned a lot\"')";
    testInterSegmentAggregationQueryHelper(query, 4);
    query = "SELECT count(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL_DICT, '\"learned a lot\"')";
    testInterSegmentAggregationQueryHelper(query, 4);
    query = "SELECT count(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"indexing and transaction processing\"')";
    testInterSegmentAggregationQueryHelper(query, 12);
    query =
        "SELECT count(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL_DICT, '\"indexing and transaction processing\"')";
    testInterSegmentAggregationQueryHelper(query, 12);
    query =
        "SELECT count(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"docker image building and distribution\"')";
    testInterSegmentAggregationQueryHelper(query, 8);
    query =
        "SELECT count(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL_DICT, '\"docker image building and distribution\"')";
    testInterSegmentAggregationQueryHelper(query, 8);
    query =
        "SELECT count(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed query engines for analytics and data warehouses\"')";
    testInterSegmentAggregationQueryHelper(query, 8);
    query =
        "SELECT count(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL_DICT, '\"distributed query engines for analytics and data warehouses\"')";
    testInterSegmentAggregationQueryHelper(query, 8);
    query = "SELECT count(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"worked in NGO\"')";
    testInterSegmentAggregationQueryHelper(query, 4);
    query = "SELECT count(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL_DICT, '\"worked in NGO\"')";
    testInterSegmentAggregationQueryHelper(query, 4);
  }

  private void testInterSegmentAggregationQueryHelper(String query, long expectedCount) {
    // PQL
    BrokerResponseNative brokerResponseNative = getBrokerResponseForPqlQuery(query);
    List<AggregationResult> aggregationResults = brokerResponseNative.getAggregationResults();
    Assert.assertEquals(aggregationResults.size(), 1);
    Assert.assertEquals(aggregationResults.get(0).getValue().toString(), String.valueOf(expectedCount));
    // SQL
    brokerResponseNative = getBrokerResponseForSqlQuery(query);
    ResultTable resultTable = brokerResponseNative.getResultTable();
    DataSchema dataSchema = resultTable.getDataSchema();
    Assert.assertEquals(dataSchema.size(), 1);
    Assert.assertEquals(dataSchema.getColumnName(0), "count(*)");
    Assert.assertEquals(dataSchema.getColumnDataType(0), DataSchema.ColumnDataType.LONG);
    List<Object[]> rows = resultTable.getRows();
    Assert.assertEquals(rows.size(), 1);
    Object[] row = rows.get(0);
    Assert.assertEquals(row.length, 1);
    Assert.assertEquals(row[0], expectedCount);
  }

  private void testInterSegmentSelectionQueryHelper(String query, List<Serializable[]> expectedResults) {
    // PQL
    BrokerResponseNative brokerResponseNative = getBrokerResponseForPqlQuery(query);
    SelectionResults selectionResults = brokerResponseNative.getSelectionResults();
    List<String> columns = selectionResults.getColumns();
    Assert.assertEquals(columns.size(), 2);
    List<Serializable[]> rows = selectionResults.getRows();
    Assert.assertEquals(rows.size(), expectedResults.size());
    for (int i = 0; i < rows.size(); i++) {
      Serializable[] actualRow = rows.get(i);
      Serializable[] expectedRow = expectedResults.get(i);
      Assert.assertEquals(actualRow[0], String.valueOf(expectedRow[0]));
      Assert.assertEquals(actualRow[1], expectedRow[1]);
    }
    // SQL
    brokerResponseNative = getBrokerResponseForSqlQuery(query);
    ResultTable resultTable = brokerResponseNative.getResultTable();
    DataSchema dataSchema = resultTable.getDataSchema();
    Assert.assertEquals(dataSchema.size(), 2);
    Assert.assertEquals(dataSchema.getColumnName(0), "INT_COL");
    Assert.assertEquals(dataSchema.getColumnName(1), "SKILLS_TEXT_COL");
    Assert.assertEquals(dataSchema.getColumnDataType(0), DataSchema.ColumnDataType.INT);
    Assert.assertEquals(dataSchema.getColumnDataType(1), DataSchema.ColumnDataType.STRING);
    List<Object[]> results = resultTable.getRows();
    Assert.assertEquals(results.size(), expectedResults.size());
    for (int i = 0; i < results.size(); i++) {
      Object[] actualRow = results.get(i);
      Serializable[] expectedRow = expectedResults.get(i);
      Assert.assertEquals(actualRow, expectedRow);
    }
  }
}
