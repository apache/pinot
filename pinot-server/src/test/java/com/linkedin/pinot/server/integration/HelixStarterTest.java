package com.linkedin.pinot.server.integration;

import java.io.File;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.core.data.readers.RecordReaderFactory;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentMetadataLoader;
import com.linkedin.pinot.core.indexsegment.columnar.creator.ColumnarSegmentCreator;
import com.linkedin.pinot.core.indexsegment.creator.SegmentCreatorFactory;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfiguration;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.time.SegmentTimeUnit;
import com.linkedin.pinot.server.conf.ServerConf;
import com.linkedin.pinot.server.starter.ServerInstance;
import com.linkedin.pinot.server.starter.helix.DefaultHelixStarterServerConfig;
import com.linkedin.pinot.server.util.SegmentTestUtils;


public class HelixStarterTest {

  private final String AVRO_DATA = "data/sample_data.avro";
  private static File INDEX_DIR = new File(HelixStarterTest.class.toString());
  public static IndexSegment _indexSegment;
  private final ColumnarSegmentMetadataLoader _columnarSegmentMetadataLoader = new ColumnarSegmentMetadataLoader();

  @BeforeTest
  public void setup() throws Exception {

  }

  private void setupSegment(File segmentDir, String resourceName, String tableName) throws Exception {
    String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();

    if (segmentDir.exists()) {
      FileUtils.deleteQuietly(segmentDir);
    }

    SegmentGeneratorConfiguration config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), segmentDir,
            "daysSinceEpoch", SegmentTimeUnit.days, resourceName, tableName);

    ColumnarSegmentCreator creator =
        (ColumnarSegmentCreator) SegmentCreatorFactory.get(SegmentVersion.v1, RecordReaderFactory.get(config));
    creator.init(config);
    creator.buildSegment();

    System.out.println("built at : " + INDEX_DIR.getAbsolutePath());
  }

  @Test
  public void testHelixServerInstance() throws Exception {
    Configuration pinotHelixProperties = new PropertiesConfiguration();
    String instanceId = "localhost:0000";
    pinotHelixProperties.addProperty("pinot.server.instance.id", instanceId);
    ServerConf serverConf = DefaultHelixStarterServerConfig.getDefaultHelixServerConfig(pinotHelixProperties);
    ServerInstance serverInstance = new ServerInstance();

    serverInstance.init(serverConf);
    serverInstance.start();
    DataManager instanceDataManager = serverInstance.getInstanceDataManager();
    File segmentDir0 = new File(INDEX_DIR.getAbsolutePath() + "/segment0");
    setupSegment(segmentDir0, "mirror", "testTable");
    File segmentDir1 = new File(INDEX_DIR.getAbsolutePath() + "/segment1");
    setupSegment(segmentDir1, "resource0", "testTable");
    File segmentDir2 = new File(INDEX_DIR.getAbsolutePath() + "/segment2");
    setupSegment(segmentDir2, "resource1", "testTable");

    instanceDataManager.addSegment(_columnarSegmentMetadataLoader.loadIndexSegmentMetadataFromDir(segmentDir0
        .getAbsolutePath()));
    instanceDataManager.addSegment(_columnarSegmentMetadataLoader.loadIndexSegmentMetadataFromDir(segmentDir1
        .getAbsolutePath()));
    instanceDataManager.addSegment(_columnarSegmentMetadataLoader.loadIndexSegmentMetadataFromDir(segmentDir2
        .getAbsolutePath()));

  }

}
