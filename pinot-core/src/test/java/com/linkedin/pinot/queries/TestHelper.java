/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.queries;

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.data.manager.config.FileBasedInstanceDataManagerConfig;
import com.linkedin.pinot.core.data.manager.offline.FileBasedInstanceDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManagerProvider;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import com.linkedin.pinot.core.startree.hll.HllConfig;
import com.linkedin.pinot.core.startree.hll.SegmentWithHllIndexCreateHelper;
import com.linkedin.pinot.util.TestUtils;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestHelper implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestHelper.class);

  static final String AVRO_DATA = "data/test_data-sv.avro";

  // package-visible since this is package-internal testing helper
  final String tableName;
  static final String TIME_COLUMN = "daysSinceEpoch";
  static final TimeUnit TIME_UNIT = TimeUnit.DAYS;
  final ServerMetrics serverMetrics;
  final FileBasedInstanceDataManager instanceDataManager;
  final PropertiesConfiguration serverConf;
  final QueryExecutor queryExecutor;

  TestHelper(@Nonnull String tableName, @Nullable PropertiesConfiguration serverConf)
      throws IOException {
    this.tableName = Objects.requireNonNull(tableName, "Table name should not be null");
    if (serverConf != null) {
      this.serverConf = serverConf;
    } else {
      this.serverConf = new TestingServerPropertiesBuilder(tableName).build();
    }
    this.serverConf.setDelimiterParsingDisabled(false);

    serverMetrics = new ServerMetrics(new MetricsRegistry());
    TableDataManagerProvider.setServerMetrics(serverMetrics);
    instanceDataManager = FileBasedInstanceDataManager.getInstanceDataManager();
    queryExecutor = new ServerQueryExecutorV1Impl(false);
  }

  void init()
      throws ConfigurationException, IllegalAccessException, ClassNotFoundException, InstantiationException {
    instanceDataManager.init(new FileBasedInstanceDataManagerConfig(serverConf.subset("pinot.server.instance")));
    instanceDataManager.start();
    queryExecutor.init(serverConf.subset("pinot.server.query.executor"), instanceDataManager, serverMetrics);
  }

  SegmentWithHllIndexCreateHelper buildLoadDefaultHllSegment(HllConfig hllConfig)
      throws Exception {
    SegmentWithHllIndexCreateHelper helper = buildDefaultHllSegment(hllConfig);
    loadSegment(helper.getSegmentDirectory());
    return helper;
  }

  IndexSegment loadSegment(File indexDir)
      throws Exception {
    IndexSegment indexSegment = ColumnarSegmentLoader.load(indexDir, ReadMode.mmap);
    instanceDataManager.getTableDataManager(tableName).addSegment(indexSegment);
    return indexSegment;
  }

  SegmentWithHllIndexCreateHelper buildDefaultHllSegment(HllConfig hllConfig)
      throws Exception {
    return buildHllSegment(this.tableName, TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(AVRO_DATA)),
        TIME_COLUMN, TIME_UNIT, "testSegment", hllConfig, true);
  }

  SegmentWithHllIndexCreateHelper buildHllSegment(String tableName, String avroData, String timeColumn, TimeUnit timeUnit,
      String testSegment, HllConfig hllConfig, boolean enableStarTree)
      throws Exception {
    SegmentWithHllIndexCreateHelper helper =
        new SegmentWithHllIndexCreateHelper(tableName, avroData, timeColumn, timeUnit, testSegment);
    helper.build(enableStarTree, hllConfig);
    return helper;
  }

  @Override
  public void close() {
    queryExecutor.shutDown();
    instanceDataManager.shutDown();
  }
}
