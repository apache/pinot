package com.linkedin.pinot.queries;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.client.request.RequestConverter;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.query.gen.AvroQueryGenerator;
import com.linkedin.pinot.common.query.gen.AvroQueryGenerator.TestGroupByAggreationQuery;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.data.manager.InstanceDataManager;
import com.linkedin.pinot.core.data.manager.config.InstanceDataManagerConfig;
import com.linkedin.pinot.core.data.readers.RecordReaderFactory;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.columnar.creator.ColumnarSegmentCreator;
import com.linkedin.pinot.core.indexsegment.creator.SegmentCreatorFactory;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfiguration;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import com.linkedin.pinot.core.time.SegmentTimeUnit;
import com.linkedin.pinot.pql.parsers.PQLCompiler;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Oct 14, 2014
 */

public class QueriesSentinelTest {
  private static final Logger logger = Logger.getLogger(QueriesSentinelTest.class);

  private static final PQLCompiler requestCompiler = new PQLCompiler(new HashMap<String, String[]>());

  private final String AVRO_DATA = "data/mirror-sv.avro";
  private static File INDEX_DIR = new File(QueriesSentinelTest.class.toString());
  AvroQueryGenerator gen;
  static InstanceDataManager instanceDM;
  static QueryExecutor queryExecutor;
  static TestingServerPropertiesBuilder configBuilder;

  @BeforeClass
  public void setup() throws Exception {
    configBuilder = new TestingServerPropertiesBuilder("mirror");

    setupSegmentFor("mirror");
    setUpTestQueries("mirror");

    final PropertiesConfiguration serverConf = configBuilder.build();
    serverConf.setDelimiterParsingDisabled(false);

    final InstanceDataManager instanceDataManager = InstanceDataManager.getInstanceDataManager();
    instanceDataManager.init(new InstanceDataManagerConfig(serverConf.subset("pinot.server.instance")));
    instanceDataManager.start();

    final IndexSegment indexSegment = ColumnarSegmentLoader.load(new File(INDEX_DIR, "segment"), ReadMode.heap);
    instanceDataManager.getResourceDataManager("mirror");
    instanceDataManager.getResourceDataManager("mirror").addSegment(indexSegment);

    queryExecutor = new ServerQueryExecutorV1Impl();
    queryExecutor.init(serverConf.subset("pinot.server.query.executor"), instanceDataManager);
  }

  @Test
  public void test1() throws Exception {
    final List<AvroQueryGenerator.TestGroupByAggreationQuery> groupByCalls = gen.giveMeNGroupByAggregationQueries(100);
    int counter = 0;
    for (final TestGroupByAggreationQuery groupBy : groupByCalls) {
      logger.info("running : " + groupBy.pql);
      queryExecutor.processQuery(new InstanceRequest(counter++, RequestConverter.fromJSON(requestCompiler.compile(groupBy.pql))));
    }
  }

  private void setUpTestQueries(String resource) throws FileNotFoundException, IOException {
    final String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();
    final List<String> dims = new ArrayList<String>();
    dims.add("viewerId");
    dims.add("viewerType");
    dims.add("vieweeId");
    dims.add("viewerCompany");
    dims.add("viewerCountry");
    dims.add("viewerRegionCode");
    dims.add("viewerIndustry");
    dims.add("viewerOccupation");
    dims.add("viewerSchool");
    dims.add("viewerSeniority");
    dims.add("viewerPrivacySetting");
    dims.add("viewerObfuscationType");
    dims.add("vieweePrivacySetting");
    dims.add("weeksSinceEpochSunday");
    dims.add("daysSinceEpoch");
    dims.add("hoursSinceEpoch");

    final List<String> mets = new ArrayList<String>();
    mets.add("count");

    final String time = "minutesSinceEpoch";
    gen = new AvroQueryGenerator(new File(filePath), dims, mets, time, resource);
    gen.init();
    gen.generateSimpleAggregationOnSingleColumnFilters();
  }

  private void setupSegmentFor(String resource) throws Exception {
    final String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();

    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }
    INDEX_DIR.mkdir();

    final SegmentGeneratorConfiguration config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), new File(INDEX_DIR, "segment"), "daysSinceEpoch",
            SegmentTimeUnit.days, resource, resource);

    final ColumnarSegmentCreator creator =
        (ColumnarSegmentCreator) SegmentCreatorFactory.get(SegmentVersion.v1, RecordReaderFactory.get(config));
    creator.init(config);
    creator.buildSegment();

    System.out.println("built at : " + INDEX_DIR.getAbsolutePath());
  }
}
