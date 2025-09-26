package org.apache.pinot.integration.tests;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.server.starter.helix.HelixInstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class RetentionManagerIntegrationTest extends BaseClusterIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(RetentionManagerIntegrationTest.class);

  protected List<File> _avroFiles;

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, true);
    properties.put(ControllerConf.ControllerPeriodicTasksConf.ENABLE_DEEP_STORE_RETRY_UPLOAD_LLC_SEGMENT, true);
    properties.put(ControllerConf.ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_INITIAL_DELAY_IN_SECONDS,
        500);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    try {
      LOGGER.info("Set segment.store.uri: {} for server with scheme: {}", _controllerConfig.getDataDir(),
          new URI(_controllerConfig.getDataDir()).getScheme());
      serverConf.setProperty("pinot.server.instance.segment.store.uri",
          "file:" + _controllerConfig.getDataDir());
      serverConf.setProperty("pinot.server.instance." + HelixInstanceDataManagerConfig.UPLOAD_SEGMENT_TO_DEEP_STORE,
          "true");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    startZk();
    startController();
    startBroker();
    startServer();
    setupTable();
    waitForAllDocsLoaded(600_000L);
  }

  protected void setupTable()
      throws Exception {
    _avroFiles = unpackAvroData(_tempDir);
    startKafka();
    pushAvroIntoKafka(_avroFiles);

    Schema schema = createSchema();
    addSchema(schema);

    TableConfig tableConfig = createRealtimeTableConfig(_avroFiles.get(0));
    tableConfig.getValidationConfig().setRetentionTimeUnit("DAYS");
    tableConfig.getValidationConfig().setRetentionTimeValue("2");
    addTableConfig(tableConfig);

    waitForDocsLoaded(600_000L, true, tableConfig.getTableName());

    PinotFS pinotFS = PinotFSFactory.create(new URI(_controllerConfig.getDataDir()).getScheme());
    Thread.sleep(1000);
//    updateClusterConfig();
  }

  @Test
  public void testNothing() {

  }
}
