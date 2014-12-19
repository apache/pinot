package com.linkedin.pinot.server.integration;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeTest;

import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentMetadataLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.server.conf.ServerConf;
import com.linkedin.pinot.server.starter.ServerInstance;
import com.linkedin.pinot.server.starter.helix.DefaultHelixStarterServerConfig;
import com.linkedin.pinot.server.util.SegmentTestUtils;


public class HelixStarterTest {

  private final String AVRO_DATA = "data/sample_data.avro";
  private static File INDEX_DIR = new File("HelixStarterTest");
  public static IndexSegment _indexSegment;
  private final ColumnarSegmentMetadataLoader _columnarSegmentMetadataLoader = new ColumnarSegmentMetadataLoader();

  @BeforeTest
  public void setup() throws Exception {
    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }
    INDEX_DIR.mkdirs();
  }

  private void setupSegment(File segmentDir, String resourceName, String tableName) throws Exception {
    System.out.println(getClass().getClassLoader().getResource(AVRO_DATA));
    final String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();

    if (segmentDir.exists()) {
      FileUtils.deleteQuietly(segmentDir);
    }

    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), segmentDir,
            "daysSinceEpoch", TimeUnit.DAYS, resourceName, tableName);

    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();

    System.out.println("built at : " + INDEX_DIR.getAbsolutePath());
  }

  public void testSingleHelixServerStartAndTakingSegment() throws Exception {
    final Configuration pinotHelixProperties = new PropertiesConfiguration();
    final String instanceId = "localhost:0000";
    pinotHelixProperties.addProperty("pinot.server.instance.id", instanceId);
    final ServerConf serverConf = DefaultHelixStarterServerConfig.getDefaultHelixServerConfig(pinotHelixProperties);
    final ServerInstance serverInstance = new ServerInstance();

    serverInstance.init(serverConf);
    serverInstance.start();
    final DataManager instanceDataManager = serverInstance.getInstanceDataManager();
    final File segmentDir0 = new File(INDEX_DIR.getAbsolutePath() + "/segment0");
    System.out.println(segmentDir0);
    setupSegment(segmentDir0, "mirror", "testTable");
    final File segmentDir1 = new File(INDEX_DIR.getAbsolutePath() + "/segment1");
    System.out.println(segmentDir1);
    setupSegment(segmentDir1, "resource0", "testTable");
    final File segmentDir2 = new File(INDEX_DIR.getAbsolutePath() + "/segment2");
    System.out.println(segmentDir2);
    setupSegment(segmentDir2, "resource1", "testTable");

    instanceDataManager.addSegment(_columnarSegmentMetadataLoader.loadIndexSegmentMetadataFromDir(segmentDir0
        .getAbsolutePath()));
    instanceDataManager.addSegment(_columnarSegmentMetadataLoader.loadIndexSegmentMetadataFromDir(segmentDir1
        .getAbsolutePath()));
    instanceDataManager.addSegment(_columnarSegmentMetadataLoader.loadIndexSegmentMetadataFromDir(segmentDir2
        .getAbsolutePath()));

  }

}
