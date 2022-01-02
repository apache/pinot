package org.apache.pinot.query;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.query.dispatch.WorkerQueryRequest;
import org.apache.pinot.query.runtime.QueryRunner;
import org.apache.pinot.query.runtime.mailbox.GrpcMailboxService;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class QueryServerEnclosure {
  private static final int NUM_ROWS = 5;

  private final ExecutorService _testExecutor = Executors.newFixedThreadPool(1);
  private final int _grpcPort;
  private final Map<String, Object> _runnerConfig = new HashMap<>();
  private final Map<String, List<ImmutableSegment>> _segmentMap = new HashMap<>();
  private final InstanceDataManager _instanceDataManager;
  private final Map<String, TableDataManager> _tableDataManagers = new HashMap<>();
  private final Map<String, File> _indexDirs;

  private GrpcMailboxService _mailboxService;
  private QueryRunner _queryRunner;

  public QueryServerEnclosure(List<String> tables, Map<String, File> indexDirs, Map<String, List<String>> segments) {
    _indexDirs = indexDirs;
    try {
      for (int i = 0; i < tables.size(); i++) {
        String tableName = tables.get(i);
        File indexDir = indexDirs.get(tableName);
        FileUtils.deleteQuietly(indexDir);
        List<ImmutableSegment> segmentList = new ArrayList<>();
        for (String segmentName : segments.get(tableName)) {
          segmentList.add(buildSegment(indexDir, tableName, segmentName));
        }
        _segmentMap.put(tableName, segmentList);
      }
      _instanceDataManager = mockInstanceDataManager();
      _grpcPort = QueryEnvironmentTestUtils.getAvailablePort();
      _runnerConfig.put(CommonConstants.Server.CONFIG_OF_GRPC_PORT, _grpcPort);
      _runnerConfig.put(CommonConstants.Server.CONFIG_OF_INSTANCE_ID, String.format("Server_localhost_%d", _grpcPort));
      _queryRunner = new QueryRunner();
    } catch (Exception e) {
      throw new RuntimeException("Test Failed!", e);
    }
  }

  public ServerMetrics mockServiceMetrics() {
    return mock(ServerMetrics.class);
  }

  public InstanceDataManager mockInstanceDataManager() {
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    for (Map.Entry<String, List<ImmutableSegment>> e : _segmentMap.entrySet()) {
      TableDataManager tableDataManager = mockTableDataManager(e.getValue());
      _tableDataManagers.put(e.getKey(), tableDataManager);
    }
    for (Map.Entry<String, TableDataManager> e : _tableDataManagers.entrySet()) {
      when(instanceDataManager.getTableDataManager(matches(String.format("%s.*", e.getKey()))))
          .thenReturn(e.getValue());
    }
    return instanceDataManager;
  }

  public TableDataManager mockTableDataManager(List<ImmutableSegment> segmentList) {
    List<SegmentDataManager> tableSegmentDataManagers = segmentList.stream()
        .map(ImmutableSegmentDataManager::new).collect(Collectors.toList());
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.acquireSegments(any(), any())).thenReturn(tableSegmentDataManagers);
    return tableDataManager;
  }

  public ImmutableSegment buildSegment(File indexDir, String tableName, String segmentName) throws Exception {
    GenericRow row = new GenericRow();
    row.putValue("c1", "foo");
    row.putValue("c2", "bar");
    row.putValue("c3", 1);
    row.putValue("t", System.currentTimeMillis());

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      rows.add(row);
    }

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).setTimeColumnName("t").build();
    Schema schema = QueryEnvironmentTestUtils.SCHEMA;
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(indexDir.getPath());
    config.setTableName(tableName);
    config.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
    }
    driver.build();
    return ImmutableSegmentLoader.load(new File(indexDir, segmentName), ReadMode.mmap);
  }

  public int getPort() {
    return _grpcPort;
  }

  public void start() throws Exception {
    PinotConfiguration configuration = new PinotConfiguration(_runnerConfig);
    _mailboxService = new GrpcMailboxService(configuration);
    _mailboxService.start();
    _queryRunner = new QueryRunner();
    _queryRunner.init(configuration, _instanceDataManager, _mailboxService, mockServiceMetrics());
  }

  public void shutDown() {
    _queryRunner.shutDown();
    _mailboxService.shutdown();
    for (Map.Entry<String, List<ImmutableSegment>> e : _segmentMap.entrySet()) {
      for (ImmutableSegment segment : e.getValue()) {
        segment.destroy();
      }
      FileUtils.deleteQuietly(_indexDirs.get(e.getKey()));
    }
  }

  public void processQuery(WorkerQueryRequest workerQueryRequest, Map<String, String> requestMetadataMap) {
    _queryRunner.processQuery(workerQueryRequest, _testExecutor, requestMetadataMap);
  }
}
