package com.linkedin.pinot.queries;

import java.io.File;
import java.util.HashMap;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.client.request.RequestConverter;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.request.BrokerRequest;
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
  private static final PQLCompiler requestCompiler = new PQLCompiler(new HashMap<String, String[]>());

  private final String AVRO_DATA = "data/sample_data.avro";
  private static File INDEX_DIR = new File(QueriesSentinelTest.class.toString());

  static InstanceDataManager instanceDM;
  static QueryExecutor queryExecutor;
  static TestingServerPropertiesBuilder configBuilder ;

  @BeforeClass
  public void setup() throws Exception {
    configBuilder = new TestingServerPropertiesBuilder("mirror");
    setupSegmentFor("mirror");

    final PropertiesConfiguration serverConf = configBuilder.build();
    serverConf.setDelimiterParsingDisabled(false);

    final InstanceDataManager instanceDataManager = InstanceDataManager.getInstanceDataManager();
    instanceDataManager.init(new InstanceDataManagerConfig(serverConf.subset("pinot.server.instance")));
    instanceDataManager.start();

    final IndexSegment indexSegment = ColumnarSegmentLoader.load(INDEX_DIR, ReadMode.heap);
    instanceDataManager.getResourceDataManager("mirror");
    instanceDataManager.getResourceDataManager("mirror").addSegment(indexSegment);

    queryExecutor = new ServerQueryExecutorV1Impl();
    queryExecutor.init(serverConf.subset("pinot.server.query.executor"), instanceDataManager);
  }

  @Test
  public void test1() throws Exception {
    System.out.println(queryExecutor.processQuery(new InstanceRequest(0, getCountQuery())));
  }


  private BrokerRequest getCountQuery() throws Exception {
    final JSONObject request = requestCompiler.compile("select count(*) from mirror limit 0");
    return RequestConverter.fromJSON(request);
  }

  private void setupSegmentFor(String resource) throws Exception {
    final String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();

    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    final SegmentGeneratorConfiguration config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch",
            SegmentTimeUnit.days, resource, resource);

    final ColumnarSegmentCreator creator =
        (ColumnarSegmentCreator) SegmentCreatorFactory.get(SegmentVersion.v1, RecordReaderFactory.get(config));
    creator.init(config);
    creator.buildSegment();

    System.out.println("built at : " + INDEX_DIR.getAbsolutePath());
  }
}
