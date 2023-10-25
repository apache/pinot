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
package org.apache.pinot.query.testutils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class MockInstanceDataManagerFactory {
  private static final String DATA_DIR_PREFIX = "MockInstanceDataDir";

  // Key is table name with type
  private final Map<String, List<ImmutableSegment>> _tableSegmentMap;
  private final Map<String, List<String>> _tableSegmentNameMap;
  private final Map<String, File> _serverTableDataDirMap;

  // Key is raw table name
  private final Map<String, List<GenericRow>> _tableRowsMap;
  private final Map<String, Boolean> _nullHandlingMap;
  private final Map<String, Schema> _schemaMap;

  // Key is registered table (with or without type)
  private final Map<String, Schema> _registeredSchemaMap;

  private String _serverName;

  public MockInstanceDataManagerFactory(String serverName) {
    _serverName = serverName;
    _serverTableDataDirMap = new HashMap<>();
    _tableSegmentMap = new HashMap<>();
    _tableSegmentNameMap = new HashMap<>();
    _tableRowsMap = new HashMap<>();
    _schemaMap = new HashMap<>();
    _registeredSchemaMap = new HashMap<>();
    _nullHandlingMap = new HashMap<>();
  }

  public void registerTable(Schema schema, String tableName) {
    _registeredSchemaMap.put(tableName, schema);
    if (TableNameBuilder.isTableResource(tableName)) {
      _schemaMap.put(TableNameBuilder.extractRawTableName(tableName), schema);
      registerTableNameWithType(schema, tableName);
    } else {
      _schemaMap.put(tableName, schema);
      registerTableNameWithType(schema, TableNameBuilder.OFFLINE.tableNameWithType(tableName));
      registerTableNameWithType(schema, TableNameBuilder.REALTIME.tableNameWithType(tableName));
    }
  }

  public MockInstanceDataManagerFactory setNullHandlingForTable(String tableName) {
    _nullHandlingMap.put(tableName, true);
    return this;
  }

  private void registerTableNameWithType(Schema schema, String tableNameWithType) {
    File tableDataDir = new File(FileUtils.getTempDirectory(),
        String.format("%s_%s_%s", DATA_DIR_PREFIX, _serverName, tableNameWithType));
    FileUtils.deleteQuietly(tableDataDir);
    _serverTableDataDirMap.put(tableNameWithType, tableDataDir);
  }

  public String addSegment(String tableNameWithType, List<GenericRow> rows) {
    String segmentName = String.format("%s_%s", tableNameWithType, UUID.randomUUID());
    File tableDataDir = _serverTableDataDirMap.get(tableNameWithType);
    ImmutableSegment segment = buildSegment(tableNameWithType, tableDataDir, segmentName, rows);

    List<ImmutableSegment> segmentList = _tableSegmentMap.getOrDefault(tableNameWithType, new ArrayList<>());
    segmentList.add(segment);
    _tableSegmentMap.put(tableNameWithType, segmentList);

    List<String> segmentNameList = _tableSegmentNameMap.getOrDefault(tableNameWithType, new ArrayList<>());
    segmentNameList.add(segment.getSegmentName());
    _tableSegmentNameMap.put(tableNameWithType, segmentNameList);

    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    List<GenericRow> tableRows = _tableRowsMap.getOrDefault(rawTableName, new ArrayList<>());
    tableRows.addAll(rows);
    _tableRowsMap.put(rawTableName, tableRows);

    return segmentName;
  }

  public InstanceDataManager buildInstanceDataManager() {
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    Map<String, TableDataManager> tableDataManagers = new HashMap<>();
    for (Map.Entry<String, List<ImmutableSegment>> e : _tableSegmentMap.entrySet()) {
      TableDataManager tableDataManager = mockTableDataManager(e.getValue());
      tableDataManagers.put(e.getKey(), tableDataManager);
    }
    for (Map.Entry<String, TableDataManager> e : tableDataManagers.entrySet()) {
      when(instanceDataManager.getTableDataManager(e.getKey())).thenReturn(e.getValue());
    }
    return instanceDataManager;
  }

  public Map<String, Schema> getRegisteredSchemaMap() {
    return _registeredSchemaMap;
  }

  public Map<String, Schema> buildSchemaMap() {
    return _schemaMap;
  }

  public Map<String, Boolean> buildNullHandlingTableMap() {
    return _nullHandlingMap;
  }

  public Map<String, List<GenericRow>> buildTableRowsMap() {
    return _tableRowsMap;
  }

  public Map<String, List<String>> buildTableSegmentNameMap() {
    return _tableSegmentNameMap;
  }

  private TableDataManager mockTableDataManager(List<ImmutableSegment> segmentList) {
    Map<String, SegmentDataManager> segmentDataManagerMap =
        segmentList.stream().collect(Collectors.toMap(IndexSegment::getSegmentName, ImmutableSegmentDataManager::new));
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.acquireSegments(anyList(), anyList())).thenAnswer(invocation -> {
      List<String> segments = invocation.getArgument(0);
      return segments.stream().map(segmentDataManagerMap::get).collect(Collectors.toList());
    });
    return tableDataManager;
  }

  private ImmutableSegment buildSegment(String tableNameWithType, File indexDir, String segmentName,
      List<GenericRow> rows) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    // TODO: plugin table config constructor
    TableConfig tableConfig =
        new TableConfigBuilder(tableType).setTableName(rawTableName).setTimeColumnName("ts")
            .setNullHandlingEnabled(true)
            .build();
    Schema schema = _schemaMap.get(rawTableName);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(indexDir.getPath());
    config.setTableName(tableNameWithType);
    config.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
      return ImmutableSegmentLoader.load(new File(indexDir, segmentName), ReadMode.mmap);
    } catch (Exception e) {
      throw new RuntimeException("Unable to construct immutable segment from records", e);
    }
  }
}
