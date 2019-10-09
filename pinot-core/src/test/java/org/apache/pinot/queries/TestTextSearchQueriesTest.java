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

import com.google.common.base.Stopwatch;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.ControlledRealTimeReopenThread;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.data.manager.SegmentDataManager;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.data.readers.RecordReader;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.SelectionOnlyOperator;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestTextSearchQueriesTest extends BaseQueriesTest {

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "TextSearchQueryTest");
  private static final String TABLE_NAME = "MyTable";
  private static final String SEGMENT_NAME = TABLE_NAME + "_100000000_200000000";
  private static final String QUERY_LOG_TEXT_COL_NAME = "QUERY_LOG_TEXT_COL";
  private static final String QUERY_LOG_STRING_COL_NAME = "QUERY_LOG_STRING_COL";
  private static final String SKILLS_TEXT_COL_NAME = "SKILLS_TEXT_COL";
  private static final String SKILLS_STRING_COL_NAME = "SKILLS_STRING_COL";
  private static final String INT_COL_NAME = "INT_COL";
  private static final List<String> textSearchColumns = new ArrayList<>();
  private static final List<String> rawIndexCreationColumns = new ArrayList<>();
  private static final int INT_BASE_VALUE = 1000;
  private List<GenericRow> _rows = new ArrayList<>();
  private RecordReader _recordReader;
  Schema _schema;

  private List<IndexSegment> _indexSegments = new ArrayList<>(1);
  private List<SegmentDataManager> _segmentDataManagers = new ArrayList<>();

  @BeforeClass
  public void setUp()
      throws Exception {
    createPinotTableSchema();
    createTestData();
    _recordReader = new GenericRowRecordReader(_rows, _schema);
    createSegment();
    loadSegment();
  }

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegments.get(0);
  }

  @Override
  protected List<SegmentDataManager> getSegmentDataManagers() {
    return _segmentDataManagers;
  }

  @AfterClass
  public void tearDown() {
    for (IndexSegment indexSegment : _indexSegments) {
      if (indexSegment != null) {
        indexSegment.destroy();
      }
    }
    _indexSegments.clear();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private void createPinotTableSchema() {
    _schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(QUERY_LOG_TEXT_COL_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(QUERY_LOG_STRING_COL_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(SKILLS_TEXT_COL_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(SKILLS_STRING_COL_NAME, FieldSpec.DataType.STRING)
        .addMetric(INT_COL_NAME, FieldSpec.DataType.INT).build();
  }

  private void createSegment()
      throws Exception {
    textSearchColumns.add(QUERY_LOG_TEXT_COL_NAME);
    textSearchColumns.add(SKILLS_TEXT_COL_NAME);
    rawIndexCreationColumns.addAll(textSearchColumns);
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(_schema);
    segmentGeneratorConfig.setTableName(TABLE_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getAbsolutePath());
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setRawIndexCreationColumns(rawIndexCreationColumns);
    segmentGeneratorConfig.setTextSearchColumns(textSearchColumns);
    segmentGeneratorConfig.setCheckTimeColumnValidityDuringGeneration(false);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, _recordReader);
    driver.build();

    File segmentIndexDir = new File(INDEX_DIR.getAbsolutePath(), SEGMENT_NAME);
    if (!segmentIndexDir.exists()) {
      throw new IllegalStateException("Segment generation failed");
    }
  }

  private void loadSegment()
      throws Exception {
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    Set<String> textColumns = new HashSet<>();
    textColumns.addAll(textSearchColumns);
    indexLoadingConfig.setTextSearchColumns(textColumns);
    ImmutableSegment segment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig);
    _indexSegments.add(segment);
  }

  private void createTestData()
      throws Exception {
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

    // read the pql query log file and build dataset
    resourceUrl = getClass().getClassLoader().getResource("data/text_search_data/pql_query1.txt");
    File logFile = new File(resourceUrl.getFile());
    int counter = 0;
    try (InputStream inputStream = new FileInputStream(logFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      String line;
      while ((line = reader.readLine()) != null) {
        GenericRow row = new GenericRow();
        row.putField(INT_COL_NAME, INT_BASE_VALUE + counter);
        row.putField(QUERY_LOG_TEXT_COL_NAME, line);
        row.putField(QUERY_LOG_STRING_COL_NAME, line);
        if (counter >= skillCount) {
          row.putField(SKILLS_TEXT_COL_NAME, "software engineering");
          row.putField(SKILLS_STRING_COL_NAME, "software engineering");
        } else {
          row.putField(SKILLS_TEXT_COL_NAME, skills[counter]);
          row.putField(SKILLS_STRING_COL_NAME, skills[counter]);
        }
        _rows.add(row);
        counter++;
      }
    }
  }

  @Test
  public void testTextSearch() throws Exception {
    // TEST: phrase query
    // search in QUERY_TEXT_COL to
    // look for documents where each document MUST contain phrase
    // "SELECT activity_id" as is.
    // in other words, we are trying to find all "SELECT activity_id" style queries
    String query =
        "SELECT INT_COL, QUERY_LOG_TEXT_COL FROM MyTable WHERE TEXT_MATCH(QUERY_LOG_TEXT_COL, '\"SELECT dimensionCol2\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, 11787, false, null);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(QUERY_LOG_TEXT_COL, '\"SELECT dimensionCol2\"')";
    testTextSearchAggregationQueryHelper(query, 11787);

    // TEST: phrase query
    // search in QUERY_TEXT_LOG column to
    // look for documents where each document MUST
    // contain phrase "SELECT count" as is.
    // in other words, we are trying to find all "SELECT count" style queries from log

    query = "SELECT INT_COL, QUERY_LOG_TEXT_COL FROM MyTable WHERE TEXT_MATCH(QUERY_LOG_TEXT_COL, '\"SELECT count\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, 12363, false, null);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(QUERY_LOG_TEXT_COL, '\"SELECT count\"')";
    testTextSearchAggregationQueryHelper(query, 12363);

    // TEST: phrase query
    // search in QUERY_LOG_TEXT_COL to
    // look for documents where each document MUST contain phrase
    // "GROUP BY" as is
    // in other words, we are trying to find all GROUP BY queries from log

    query = "SELECT INT_COL, QUERY_LOG_TEXT_COL FROM MyTable WHERE TEXT_MATCH(QUERY_LOG_TEXT_COL, '\"GROUP BY\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, 26, true, null);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(QUERY_LOG_TEXT_COL, '\"GROUP BY\"')";
    testTextSearchAggregationQueryHelper(query, 26);

    // TEST: phrase query
    // search in SKILL_TEXT_COL column to
    // look for documents where each document MUST contain phrase
    // "distributed systems" as is

    List<Serializable[]> expected = new ArrayList<>();
    expected.add(new Serializable[]{1005, "Distributed systems, Java, C++, Go, distributed query engines for analytics and data warehouses, Machine learning, spark, Kubernetes, transaction processing"});
    expected.add(new Serializable[]{1009, "Distributed systems, database development, columnar query engine, database kernel, storage, indexing and transaction processing, building large scale systems"});
    expected.add(new Serializable[]{1010, "Distributed systems, Java, realtime streaming systems, Machine learning, spark, Kubernetes, distributed storage, concurrency, multi-threading"});
    expected.add(new Serializable[]{1012, "Distributed systems, Java, database engine, cluster management, docker image building and distribution"});
    expected.add(new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});
    expected.add(new Serializable[]{1020, "Databases, columnar query processing, Apache Arrow, distributed systems, Machine learning, cluster management, docker image building and distribution"});

    query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Distributed systems\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST: phrase query
    // search in SKILLS_TEXT_COL column to
    // look for documents where each document MUST contain phrase
    // "query processing" as is

    expected = new ArrayList<>();
    expected.add(new Serializable[]{1014, "Apache spark, Java, C++, query processing, transaction processing, distributed storage, concurrency, multi-threading, apache airflow"});
    expected.add(new Serializable[]{1020, "Databases, columnar query processing, Apache Arrow, distributed systems, Machine learning, cluster management, docker image building and distribution"});

    query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"query processing\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"query processing\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST: phrase query
    // search in SKILLS_TEXT_COL column to
    // look for documents where each document MUST contain phrase
    // "machine learning" as is

    expected = new ArrayList<>();
    expected.add(new Serializable[]{1003, "Java, C++, worked on open source projects, coursera machine learning"});
    expected.add(new Serializable[]{1004, "Machine learning, Tensor flow, Java, Stanford university,"});
    expected.add(new Serializable[]{1005, "Distributed systems, Java, C++, Go, distributed query engines for analytics and data warehouses, Machine learning, spark, Kubernetes, transaction processing"});
    expected.add(new Serializable[]{1006, "Java, Python, C++, Machine learning, building and deploying large scale production systems, concurrency, multi-threading, CPU processing"});
    expected.add(new Serializable[]{1007, "C++, Python, Tensor flow, database kernel, storage, indexing and transaction processing, building large scale systems, Machine learning"});
    expected.add(new Serializable[]{1010, "Distributed systems, Java, realtime streaming systems, Machine learning, spark, Kubernetes, distributed storage, concurrency, multi-threading"});
    expected.add(new Serializable[]{1011, "CUDA, GPU, Python, Machine learning, database kernel, storage, indexing and transaction processing, building large scale systems"});
    expected.add(new Serializable[]{1016, "CUDA, GPU processing, Tensor flow, Pandas, Python, Jupyter notebook, spark, Machine learning, building high performance scalable systems"});
    expected.add(new Serializable[]{1019, "C++, Java, Python, realtime streaming systems, Machine learning, spark, Kubernetes, transaction processing, distributed storage, concurrency, multi-threading, apache airflow"});
    expected.add(new Serializable[]{1020, "Databases, columnar query processing, Apache Arrow, distributed systems, Machine learning, cluster management, docker image building and distribution"});

    query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Machine learning\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Machine learning\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST: composite phrase query using boolean operator AND
    // search in SKILLS_TEXT_COL column to
    // look for documents where each document MUST contain
    // two independent phrases
    // "machine learning" and "tensor flow" as is

    expected = new ArrayList<>();
    expected.add(new Serializable[]{1004, "Machine learning, Tensor flow, Java, Stanford university,"});
    expected.add(new Serializable[]{1007, "C++, Python, Tensor flow, database kernel, storage, indexing and transaction processing, building large scale systems, Machine learning"});
    expected.add(new Serializable[]{1016, "CUDA, GPU processing, Tensor flow, Pandas, Python, Jupyter notebook, spark, Machine learning, building high performance scalable systems"});

    query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Machine learning\" AND \"Tensor flow\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Machine learning\" AND \"Tensor flow\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST: term query
    // search in SKILLS_TEXT_COL column to
    // look for documents where each document MUST contain term 'Java'

    expected = new ArrayList<>();
    expected.add(new Serializable[]{1000, "Accounts, Banking, Insurance, worked in NGO, Java"});
    expected.add(new Serializable[]{1003, "Java, C++, worked on open source projects, coursera machine learning"});
    expected.add(new Serializable[]{1004, "Machine learning, Tensor flow, Java, Stanford university,"});
    expected.add(new Serializable[]{1005, "Distributed systems, Java, C++, Go, distributed query engines for analytics and data warehouses, Machine learning, spark, Kubernetes, transaction processing"});
    expected.add(new Serializable[]{1006, "Java, Python, C++, Machine learning, building and deploying large scale production systems, concurrency, multi-threading, CPU processing"});
    expected.add(new Serializable[]{1008, "Amazon EC2, AWS, hadoop, big data, spark, building high performance scalable systems, building and deploying large scale production systems, concurrency, multi-threading, Java, C++, CPU processing"});
    expected.add(new Serializable[]{1010, "Distributed systems, Java, realtime streaming systems, Machine learning, spark, Kubernetes, distributed storage, concurrency, multi-threading"});
    expected.add(new Serializable[]{1012, "Distributed systems, Java, database engine, cluster management, docker image building and distribution"});
    expected.add(new Serializable[]{1014, "Apache spark, Java, C++, query processing, transaction processing, distributed storage, concurrency, multi-threading, apache airflow"});
    expected.add(new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});
    expected.add(new Serializable[]{1018, "Realtime stream processing, publish subscribe, columnar processing for data warehouses, concurrency, Java, multi-threading, C++,"});
    expected.add(new Serializable[]{1019, "C++, Java, Python, realtime streaming systems, Machine learning, spark, Kubernetes, transaction processing, distributed storage, concurrency, multi-threading, apache airflow"});

    query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, 'Java') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, 'Java') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST: composite term query using BOOLEAN operator AND
    // search in SKILLS_TEXT_COL column to
    // look for documents where each document MUST contain two independent
    // terms 'Java' and 'C++'

    expected = new ArrayList<>();
    expected.add(new Serializable[]{1003, "Java, C++, worked on open source projects, coursera machine learning"});
    expected.add(new Serializable[]{1005, "Distributed systems, Java, C++, Go, distributed query engines for analytics and data warehouses, Machine learning, spark, Kubernetes, transaction processing"});
    expected.add(new Serializable[]{1006, "Java, Python, C++, Machine learning, building and deploying large scale production systems, concurrency, multi-threading, CPU processing"});
    expected.add(new Serializable[]{1008, "Amazon EC2, AWS, hadoop, big data, spark, building high performance scalable systems, building and deploying large scale production systems, concurrency, multi-threading, Java, C++, CPU processing"});
    expected.add(new Serializable[]{1014, "Apache spark, Java, C++, query processing, transaction processing, distributed storage, concurrency, multi-threading, apache airflow"});
    expected.add(new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});
    expected.add(new Serializable[]{1018, "Realtime stream processing, publish subscribe, columnar processing for data warehouses, concurrency, Java, multi-threading, C++,"});
    expected.add(new Serializable[]{1019, "C++, Java, Python, realtime streaming systems, Machine learning, spark, Kubernetes, transaction processing, distributed storage, concurrency, multi-threading, apache airflow"});

    query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, 'Java AND C++') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, 'Java AND C++') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST: phrase query
    // search in SKILLS_TEXT_COL column to
    // look for documents where each document MUST contain phrase "Java C++" as is.
    // notice the difference in results with previous term query.
    // in these cases, phrase query does not help a lot since
    // we are essentially relying on user's way of specifying skills.
    // a user that has both Java and C++ but the latter is not immediately
    // followed after former will be missed with this phrase query. This is where
    // term query using boolean operator seems to be more appropriate
    // as shown in the previous test.

    expected = new ArrayList<>();
    expected.add(new Serializable[]{1003, "Java, C++, worked on open source projects, coursera machine learning"});
    expected.add(new Serializable[]{1005, "Distributed systems, Java, C++, Go, distributed query engines for analytics and data warehouses, Machine learning, spark, Kubernetes, transaction processing"});
    expected.add(new Serializable[]{1008, "Amazon EC2, AWS, hadoop, big data, spark, building high performance scalable systems, building and deploying large scale production systems, concurrency, multi-threading, Java, C++, CPU processing"});
    expected.add(new Serializable[]{1014, "Apache spark, Java, C++, query processing, transaction processing, distributed storage, concurrency, multi-threading, apache airflow"});

    query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Java C++\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Java C++\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST: composite phrase query using boolean operator AND.
    // search in SKILLS_TEXT_COL column to
    // look for documents where each document MUST contain two independent phrases
    // "machine learning" and "gpu processing" as is

    expected = new ArrayList<>();
    expected.add(new Serializable[]{1016, "CUDA, GPU processing, Tensor flow, Pandas, Python, Jupyter notebook, spark, Machine learning, building high performance scalable systems"});

    query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Machine learning\" AND \"gpu processing\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Machine learning\" AND \"gpu processing\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST: composite phrase and term query using boolean operator AND.
    // search in SKILLS_TEXT_COL column to
    // look for documents where each document MUST contain
    // phrase "machine learning" as is
    // and term 'gpu'
    // note the difference in result with previous query where 'gpu'
    // was used in the context of phrase "gpu processing" but that
    // resulted in missing out on one row

    expected = new ArrayList<>();
    expected.add(new Serializable[]{1011, "CUDA, GPU, Python, Machine learning, database kernel, storage, indexing and transaction processing, building large scale systems"});
    expected.add(new Serializable[]{1016, "CUDA, GPU processing, Tensor flow, Pandas, Python, Jupyter notebook, spark, Machine learning, building high performance scalable systems"});

    query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Machine learning\" AND gpu') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Machine learning\" AND gpu') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST: composite phrase and term query using boolean operator AND
    // search in SKILLS_TEXT_COL column to
    // look for documents where each document MUST contain
    // phrase "machine learning" as is
    // and terms 'gpu' and 'python'
    // this example shows the usefulness of combining phrases and terms
    // in a single WHERE clause to look for skills "machine learning" along
    // with 'gpu' and 'python'

    expected = new ArrayList<>();
    expected.add(new Serializable[]{1011, "CUDA, GPU, Python, Machine learning, database kernel, storage, indexing and transaction processing, building large scale systems"});
    expected.add(new Serializable[]{1016, "CUDA, GPU processing, Tensor flow, Pandas, Python, Jupyter notebook, spark, Machine learning, building high performance scalable systems"});

    query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Machine learning\" AND gpu AND python') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"Machine learning\" AND gpu AND python') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST: term query
    // search in SKILLS_TEXT_COL column to
    // look for documents that MUST contain term 'apache'

    expected = new ArrayList<>();
    expected.add(new Serializable[]{1013, "Kubernetes, cluster management, operating systems, concurrency, multi-threading, apache airflow, Apache Spark,"});
    expected.add(new Serializable[]{1014, "Apache spark, Java, C++, query processing, transaction processing, distributed storage, concurrency, multi-threading, apache airflow"});
    expected.add(new Serializable[]{1015, "Big data stream processing, Apache Flink, Apache Beam, database kernel, distributed query engines for analytics and data warehouses"});
    expected.add(new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});
    expected.add(new Serializable[]{1019, "C++, Java, Python, realtime streaming systems, Machine learning, spark, Kubernetes, transaction processing, distributed storage, concurrency, multi-threading, apache airflow"});
    expected.add(new Serializable[]{1020, "Databases, columnar query processing, Apache Arrow, distributed systems, Machine learning, cluster management, docker image building and distribution"});
    query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, 'apache') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, 'apache') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST: composite phrase and term query using boolean operator AND.
    // search in SKILLS_TEXT_COL column to
    // look for documents where each document MUST contain
    // phrase "distributed systems" as is
    // and term 'apache'
    // this is another example where a combination of phrase and term(s)
    // in a single expression seems to be very useful.
    // we want to use proper phrase like "distributed systems" to search
    // for the exact skill along with open source work by using the term
    // 'apache'

    expected = new ArrayList<>();
    expected.add(new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});
    expected.add(new Serializable[]{1020, "Databases, columnar query processing, Apache Arrow, distributed systems, Machine learning, cluster management, docker image building and distribution"});

    query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\" AND apache') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\" AND apache') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST: term query
    // search in SKILLS_TEXT_COL column to
    // look for documents where each document MUST contain term 'database'

    expected = new ArrayList<>();
    expected.add(new Serializable[]{1007, "C++, Python, Tensor flow, database kernel, storage, indexing and transaction processing, building large scale systems, Machine learning"});
    expected.add(new Serializable[]{1009, "Distributed systems, database development, columnar query engine, database kernel, storage, indexing and transaction processing, building large scale systems"});
    expected.add(new Serializable[]{1011, "CUDA, GPU, Python, Machine learning, database kernel, storage, indexing and transaction processing, building large scale systems"});
    expected.add(new Serializable[]{1012, "Distributed systems, Java, database engine, cluster management, docker image building and distribution"});
    expected.add(new Serializable[]{1015, "Big data stream processing, Apache Flink, Apache Beam, database kernel, distributed query engines for analytics and data warehouses"});
    expected.add(new Serializable[]{1021, "Database engine, OLAP systems, OLTP transaction processing at large scale, concurrency, multi-threading, GO, building large scale systems"});

    query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, 'database') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, 'database') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST: phrase query
    // search in SKILLS_TEXT_COL column to
    // look for documents where each document MUST contain phrase "database engine" as is

    expected = new ArrayList<>();
    expected.add(new Serializable[]{1012, "Distributed systems, Java, database engine, cluster management, docker image building and distribution"});
    expected.add(new Serializable[]{1021, "Database engine, OLAP systems, OLTP transaction processing at large scale, concurrency, multi-threading, GO, building large scale systems"});

    query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"database engine\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"database engine\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST: phrase query
    // search in SKILLS_TEXT_COL column to
    // look for documents where each document MUST contain phrase "publish subscribe" as is.
    // this is where we see the power of lucene's in-built parser.
    // ideally only second row should have been in the output but the parser
    // is intelligent to break publish-subscribe (even though not separated by
    // white space delimiter) into two different indexable words

    expected = new ArrayList<>();
    expected.add(new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});
    expected.add(new Serializable[]{1018, "Realtime stream processing, publish subscribe, columnar processing for data warehouses, concurrency, Java, multi-threading, C++,"});

    query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"publish subscribe\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"publish subscribe\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST: phrase query
    // search in SKILLS_TEXT_COL column to
    // look for documents where each document MUST contain phrase
    // "accounts banking insurance" as is.

    expected = new ArrayList<>();
    expected.add(new Serializable[]{1000, "Accounts, Banking, Insurance, worked in NGO, Java"});

    query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"accounts banking insurance\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"accounts banking insurance\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST: composite term query with boolean operator AND
    // search in SKILLS_TEXT_COL column to
    // look for documents where each document MUST contain terms
    // 'accounts' 'banking' and 'insurance'
    // this is another case where a phrase query will not be helpful
    // since the user may have specified the skills in any arbitrary
    // order and so a boolean term query helps a lot in these cases

    expected = new ArrayList<>();
    expected.add(new Serializable[]{1000, "Accounts, Banking, Insurance, worked in NGO, Java"});
    expected.add(new Serializable[]{1001, "Accounts, Banking, Finance, Insurance"});
    expected.add(new Serializable[]{1002, "Accounts, Finance, Banking, Insurance"});

    query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, 'accounts AND banking AND insurance') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, 'accounts AND banking AND insurance') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST: composite phrase and term query using boolean operator AND.
    // search in SKILLS_TEXT_COL column to
    // look for documents where each document MUST contain ALL the following skills:
    // phrase "distributed systems" as is
    // term 'Java'
    // term 'C++'

    expected = new ArrayList<>();
    expected.add(new Serializable[]{1005, "Distributed systems, Java, C++, Go, distributed query engines for analytics and data warehouses, Machine learning, spark, Kubernetes, transaction processing"});
    expected.add(new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});

    query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\" AND Java AND C++') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\" AND Java AND C++') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST: composite phrase and term query using boolean operator OR
    // search in SKILLS_TEXT_COL column to
    // look for documents where each document MUST contain ANY of the following skills:
    // phrase "distributed systems" as is
    // term 'Java'
    // term 'C++'
    // Note: OR operator is implicit when we don't specify any operator

    expected = new ArrayList<>();
    expected.add(new Serializable[]{1000, "Accounts, Banking, Insurance, worked in NGO, Java"});
    expected.add(new Serializable[]{1003, "Java, C++, worked on open source projects, coursera machine learning"});
    expected.add(new Serializable[]{1004, "Machine learning, Tensor flow, Java, Stanford university,"});
    expected.add(new Serializable[]{1005, "Distributed systems, Java, C++, Go, distributed query engines for analytics and data warehouses, Machine learning, spark, Kubernetes, transaction processing"});
    expected.add(new Serializable[]{1006, "Java, Python, C++, Machine learning, building and deploying large scale production systems, concurrency, multi-threading, CPU processing"});
    expected.add(new Serializable[]{1007, "C++, Python, Tensor flow, database kernel, storage, indexing and transaction processing, building large scale systems, Machine learning"});
    expected.add(new Serializable[]{1008, "Amazon EC2, AWS, hadoop, big data, spark, building high performance scalable systems, building and deploying large scale production systems, concurrency, multi-threading, Java, C++, CPU processing"});
    expected.add(new Serializable[]{1009, "Distributed systems, database development, columnar query engine, database kernel, storage, indexing and transaction processing, building large scale systems"});
    expected.add(new Serializable[]{1010, "Distributed systems, Java, realtime streaming systems, Machine learning, spark, Kubernetes, distributed storage, concurrency, multi-threading"});
    expected.add(new Serializable[]{1012, "Distributed systems, Java, database engine, cluster management, docker image building and distribution"});
    expected.add(new Serializable[]{1014, "Apache spark, Java, C++, query processing, transaction processing, distributed storage, concurrency, multi-threading, apache airflow"});
    expected.add(new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});
    expected.add(new Serializable[]{1018, "Realtime stream processing, publish subscribe, columnar processing for data warehouses, concurrency, Java, multi-threading, C++,"});
    expected.add(new Serializable[]{1019, "C++, Java, Python, realtime streaming systems, Machine learning, spark, Kubernetes, transaction processing, distributed storage, concurrency, multi-threading, apache airflow"});
    expected.add(new Serializable[]{1020, "Databases, columnar query processing, Apache Arrow, distributed systems, Machine learning, cluster management, docker image building and distribution"});

    query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\" Java C++') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\" Java C++') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    // TEST: composite phrase and term query using both AND and OR
    // search in SKILLS_TEXT_COL column to
    // look for documents where each document MUST contain phrase "distributed systems" as is
    // and any of the following terms 'Java' or 'C++'

    expected = new ArrayList<>();
    expected.add(new Serializable[]{1005, "Distributed systems, Java, C++, Go, distributed query engines for analytics and data warehouses, Machine learning, spark, Kubernetes, transaction processing"});
    expected.add(new Serializable[]{1010, "Distributed systems, Java, realtime streaming systems, Machine learning, spark, Kubernetes, distributed storage, concurrency, multi-threading"});
    expected.add(new Serializable[]{1012, "Distributed systems, Java, database engine, cluster management, docker image building and distribution"});
    expected.add(new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe, building and deploying large scale production systems, concurrency, multi-threading, C++, CPU processing, Java"});

    query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\" AND (Java C++)') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\" AND (Java C++)') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());
  }

  @Test
  public void testCorrectness() {
    String query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE text_match(SKILLS_TEXT_COL, '\"java c++\"') LIMIT 1000000";
    String query1 = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE regexp_like(SKILLS_TEXT_COL, 'java, c++') LIMIT 1000000";
    SelectionOnlyOperator operator1 = getOperatorForQuery(query);
    IntermediateResultsBlock block1 = operator1.nextBlock();
    SelectionOnlyOperator operator2 = getOperatorForQuery(query1);
    IntermediateResultsBlock block2 = operator2.nextBlock();
    System.out.println("dfef");
  }

  @Test
  public void testTextSearchWithAdditionalFilter() throws Exception {
    List<Serializable[]> expected = new ArrayList<>();
    expected.add(new Serializable[]{1010, "Distributed systems, Java"});
    expected.add(new Serializable[]{1012, "Distributed systems, Java, database engine"});
    expected.add(new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe"});
    expected.add(new Serializable[]{1020, "Databases, columnar query processing, Apache Arrow, distributed systems"});

    // Notes on multi col filter processing:
    // int_col >= 1010 will create a dictionary based range predicate evaluator.
    // the range predicate evaluator would have already binary searched the dictionary
    // to figure out the start dict id based on the lower bound.
    // the first leaf filter operator will be a scan based filter operator since we don't use inverted index
    // for range predicates.
    // text_match() will create a text filter leaf operator (that uses the text inverted index)
    // and gives a index based doc id set and iterator
    // the root operator and id set will be AndFilterOperator and AndBlockDocIdSet containing children:
    // -- luceneDocIdSet
    // -- scanBasedDocIdSet
    // The AND operator takes care of intersecting these in optimized manner --
    // only the luceneDocIdSet is iterated to get the next docID and then
    // the docID is used in the scan doc id set to get the corresponding dictionary ID
    // and if the dictionary ID >= start dict ID, we have a match
    String query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE INT_COL >= 1010 AND TEXT_MATCH(SKILLS_TEXT_COL, '\"Distributed systems\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE INT_COL >= 1010 AND TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());

    expected = new ArrayList<>();
    expected.add(new Serializable[]{1017, "Distributed systems, Apache Kafka, publish-subscribe"});

    // notes on multi col filter processing:
    // int_col = 1017 is equality predicate and
    // will create a dictionary based equality predicate evaluator
    query = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE INT_COL = 1017 AND TEXT_MATCH(SKILLS_TEXT_COL, '\"Distributed systems\"') LIMIT 50000";
    testTextSearchSelectQueryHelper(query, expected.size(), false, expected);

    query = "SELECT COUNT(*) FROM MyTable WHERE INT_COL = 1017 AND TEXT_MATCH(SKILLS_TEXT_COL, '\"distributed systems\"') LIMIT 50000";
    testTextSearchAggregationQueryHelper(query, expected.size());
  }

  private void testTextSearchSelectQueryHelper(String query, int expectedResultSize, boolean compareGrepOutput,
      List<Serializable[]> expectedResults)
      throws Exception {
    System.out.println("Query: " + query);
    SelectionOnlyOperator operator = getOperatorForQuery(query);
    IntermediateResultsBlock operatorResult = operator.nextBlock();
    List<Serializable[]> resultset = (List<Serializable[]>) operatorResult.getSelectionResult();
    Assert.assertNotNull(resultset);
    if (resultset.size() < 50) {
      // print for debugging/examining
      // TODO: remove this when checking-in
      for (Serializable[] row : resultset) {
        for (Serializable colValue : row) {
          System.out.print(" COLUMN: " + colValue);
        }
        System.out.println();
      }
    }
    // TODO: remove this when checking-in
    System.out.println("Number of Rows returned: " + resultset.size());
    Assert.assertEquals(resultset.size(), expectedResultSize);
    if (compareGrepOutput) {
      verifySearchOutputWithGrepResults(resultset);
    } else if (expectedResults != null) {
      for (int i = 0; i < expectedResultSize; i++) {
        Serializable[] actualRow = resultset.get(i);
        Serializable[] expectedRow = expectedResults.get(i);
        Assert.assertEquals(actualRow.length, expectedRow.length);
        for (int j = 0; j < actualRow.length; j++) {
          Object actualColValue = actualRow[j];
          Object expectedColValue = expectedRow[j];
          Assert.assertEquals(actualColValue, expectedColValue);
        }
      }
    }
  }

  private void verifySearchOutputWithGrepResults(List<Serializable[]> actualResultSet)
      throws Exception {
    URL resourceUrl = getClass().getClassLoader().getResource("data/text_search_data/group_by_grep_results.out");
    File file = new File(resourceUrl.getFile());
    InputStream inputStream = new FileInputStream(file);
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    String line;
    int counter = 0;
    while ((line = reader.readLine()) != null) {
      String[] expectedRow = line.split(":");
      Serializable[] actualRow = actualResultSet.get(counter);
      int expectedIntColValue = Integer.valueOf(expectedRow[0]) + INT_BASE_VALUE - 1;
      Assert.assertEquals(expectedIntColValue, actualRow[0]);
      Assert.assertEquals(expectedRow[1], actualRow[1]);
      counter++;
    }
  }

  private void testTextSearchAggregationQueryHelper(String query, int expectedCount) {
    System.out.println("Query: " + query);
    AggregationOperator operator = getOperatorForQuery(query);
    IntermediateResultsBlock operatorResult = operator.nextBlock();
    long count = (Long) operatorResult.getAggregationResult().get(0);
    // TODO: remove this when checking-in
    System.out.println("COUNT: " + count);
    Assert.assertEquals(expectedCount, count);
  }

  // Quick POC for NRT(Near real time) search with Lucene
  // TODO: move this out to separate test

  public static void main(String[] args) {
    try {
      File indexFile = new File("/Users/steotia/text_search/text.index");
      Directory indexDirectory = FSDirectory.open(indexFile.toPath());
      System.out.println("Lucene index file: " + indexFile.getAbsolutePath());
      StandardAnalyzer standardAnalyzer = new StandardAnalyzer();
      simpleRealtime(standardAnalyzer, indexDirectory);
      multithreadedRealtime(standardAnalyzer, indexDirectory);
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate Lucene text index creator. Error: " + e);
    }
  }

  private static void multithreadedRealtime(StandardAnalyzer standardAnalyzer, Directory indexDirectory) {
    try {
      // create and open a writer
      IndexWriterConfig indexWriterConfig = new IndexWriterConfig(standardAnalyzer);
      indexWriterConfig.setRAMBufferSizeMB(500*1024*1024);
      IndexWriter indexWriter = new IndexWriter(indexDirectory, indexWriterConfig);

      // create an NRT index reader
      SearcherManager searcherManager = new SearcherManager(indexWriter, false, false, null);
      ControlledRealTimeReopenThread controlledRealTimeReopenThread =
          new ControlledRealTimeReopenThread(indexWriter, searcherManager, 0.01, 0.01);
      controlledRealTimeReopenThread.start();

      // start writer and reader
      new Thread(new RealtimeWriter(indexWriter)).start();
      new Thread(new RealtimeReader(searcherManager, standardAnalyzer)).start();
    } catch (Exception e) {

    }
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
        System.out.println("Caught exception while reading skills file");
      }

      try {
        int counter = 0;
        Random random = new Random();
        while (counter < 1000000) {
          Document docToIndex = new Document();
          if (counter >= skillCount) {
            int index = random.nextInt(skillCount);
            docToIndex.add(new TextField("skill", skills[index], Field.Store.NO));
          } else {
            docToIndex.add(new TextField("skill", skills[counter], Field.Store.NO));
          }
          docToIndex.add(new StringField("docid", String.valueOf(counter), Field.Store.YES));
          counter++;
          indexWriter.addDocument(docToIndex);
        }
      } catch (Exception e) {
        System.out.println("Caught exception while adding a document to index");
      } finally {
        try {
          indexWriter.commit();
          System.out.println("index writer committed");
          indexWriter.close();
          System.out.println("index writer closed");
        } catch (Exception e) {
          System.out.println("Failed to commit/close the index writer");
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
        int count  = 0;
        while (count < 6500) {
          System.out.println("running query: " + count);
          IndexSearcher indexSearcher = searchManager.acquire();
          TopDocs topDocs = indexSearcher.search(query, Integer.MAX_VALUE);
          ScoreDoc[] scoreDocs = topDocs.scoreDocs;
          System.out.println("hits: " + scoreDocs.length);
          count++;
        }
      } catch (Exception e) {

      }
    }
  }

  // simple realtime test where each time reader is refreshed, we get one more hit
  // in the index of the live writer
  private static void simpleRealtime(StandardAnalyzer standardAnalyzer, Directory indexDirectory) {
    try {
      // create and open a writer
      IndexWriterConfig indexWriterConfig = new IndexWriterConfig(standardAnalyzer);
      indexWriterConfig.setRAMBufferSizeMB(500*1024*1024);
      IndexWriter indexWriter = new IndexWriter(indexDirectory, indexWriterConfig);

      //use the writer to add 1 document to the index
      Document docToIndex = new Document();
      docToIndex.add(new TextField("text", "distributed systems, machine learning, JAVA, C++", Field.Store.NO));
      docToIndex.add(new StringField("docid", String.valueOf(0), Field.Store.YES));
      try {
        indexWriter.addDocument(docToIndex);
      } catch (Exception e) {
        throw new RuntimeException("Failure while adding a new document to index. Error: " + e);
      }

      // create an NRT index reader and searcher
      QueryParser queryParser = new QueryParser("text", standardAnalyzer);
      Query query = queryParser.parse("\"distributed systems\" AND (Java C++)");
      IndexReader indexReader = DirectoryReader.open(indexWriter);
      IndexSearcher searcher = new IndexSearcher(indexReader);
      TopDocs topDocs = searcher.search(query, 1000);
      ScoreDoc[] scoreDocs = topDocs.scoreDocs;
      System.out.println("hits: " + scoreDocs.length);

      // use the writer to add another document
      docToIndex = new Document();
      docToIndex.add(new TextField("text", "distributed systems, python, JAVA, C++", Field.Store.NO));
      docToIndex.add(new StringField("docid", String.valueOf(1), Field.Store.YES));
      try {
        indexWriter.addDocument(docToIndex);
      } catch (Exception e) {
        throw new RuntimeException("Failure while adding a new document to index. Error: " + e);
      }

      // reopen NRT reader if needed and search
      IndexReader indexReader1 = DirectoryReader.openIfChanged((DirectoryReader)indexReader);
      if (indexReader1 != null) {
        searcher = new IndexSearcher(indexReader1);
      }
      topDocs = searcher.search(query, 1000);
      scoreDocs = topDocs.scoreDocs;
      System.out.println("hits: " + scoreDocs.length);

      // use the writer to add another document
      docToIndex = new Document();
      docToIndex.add(new TextField("text", "distributed systems, GPU, JAVA, C++", Field.Store.NO));
      docToIndex.add(new StringField("docid", String.valueOf(2), Field.Store.YES));
      try {
        indexWriter.addDocument(docToIndex);
      } catch (Exception e) {
        throw new RuntimeException("Failure while adding a new document to index. Error: " + e);
      }

      // reopen NRT reader if needed and search
      IndexReader indexReader2 = DirectoryReader.openIfChanged((DirectoryReader)indexReader1);
      if (indexReader2 != null) {
        searcher = new IndexSearcher(indexReader2);
      }
      query = queryParser.parse("\"distributed systems\" AND (gpu or java)");
      topDocs = searcher.search(query, 1000);
      scoreDocs = topDocs.scoreDocs;
      System.out.println("hits: " + scoreDocs.length);
    } catch (Exception e) {

    }
  }
}
