/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import com.linkedin.pinot.common.utils.StringUtil;


/**
 * Oct 14, 2014
 */
public class TestingServerPropertiesBuilder {
  private static final String PINOT_SERVER_PREFIX = "pinot.server";
  private static final String INSTANCE_PREFIC = "instance";
  private static final String EXECUTOR_PREFIX = "query.executor";

  private final String[] tableNames;

  public TestingServerPropertiesBuilder(String... tableNames) {
    this.tableNames = tableNames;
  }

  public PropertiesConfiguration build() throws IOException {
    final File file = new File("/tmp/" + TestingServerPropertiesBuilder.class.toString());

    if (file.exists()) {
      FileUtils.deleteDirectory(file);
    }

    file.mkdir();

    final File bootsDir = new File(file, "bootstrap");
    final File dataDir = new File(file, "data");

    bootsDir.mkdir();
    dataDir.mkdir();

    final PropertiesConfiguration config = new PropertiesConfiguration();
    config.addProperty(StringUtil.join(".", PINOT_SERVER_PREFIX, INSTANCE_PREFIC, "id"), "0");
    config.addProperty(StringUtil.join(".", PINOT_SERVER_PREFIX, INSTANCE_PREFIC, "bootstrap.segment.dir"), bootsDir.getAbsolutePath());
    config.addProperty(StringUtil.join(".", PINOT_SERVER_PREFIX, INSTANCE_PREFIC, "dataDir"), dataDir.getAbsolutePath());
    config.addProperty(StringUtil.join(".", PINOT_SERVER_PREFIX, INSTANCE_PREFIC, "bootstrap.segment.dir"), "0");
    config.addProperty(StringUtil.join(".", PINOT_SERVER_PREFIX, INSTANCE_PREFIC, "data.manager.class"),
        "com.linkedin.pinot.core.data.manager.InstanceDataManager");
    config.addProperty(StringUtil.join(".", PINOT_SERVER_PREFIX, INSTANCE_PREFIC, "segment.metadata.loader.class"),
        "com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentMetadataLoader");

    config.addProperty(StringUtil.join(".", PINOT_SERVER_PREFIX, INSTANCE_PREFIC, "tableName"), StringUtils.join(tableNames, ","));

    for (final String table : tableNames) {
      config.addProperty(StringUtil.join(".", PINOT_SERVER_PREFIX, INSTANCE_PREFIC, table, "dataManagerType"), "offline");
      config.addProperty(StringUtil.join(".", PINOT_SERVER_PREFIX, INSTANCE_PREFIC, table, "readMode"), "heap");
      config.addProperty(StringUtil.join(".", PINOT_SERVER_PREFIX, INSTANCE_PREFIC, table, "numQueryExecutorThreads"), "50");
    }

    config.addProperty(StringUtil.join(".", PINOT_SERVER_PREFIX, EXECUTOR_PREFIX, "class"),
        "com.linkedin.pinot.core.query.executor.ServerQueryExecutor");
    config.addProperty(StringUtil.join(".", PINOT_SERVER_PREFIX, EXECUTOR_PREFIX, "timeout"), "150000");
    config.addProperty(StringUtil.join(".", PINOT_SERVER_PREFIX, "requestHandlerFactory.class"),
        "com.linkedin.pinot.server.request.SimpleRequestHandlerFactory");
    config.addProperty(StringUtil.join(".", PINOT_SERVER_PREFIX, "netty.port"), "8882");
    config.setDelimiterParsingDisabled(true);

    final Iterator<String> keys = config.getKeys();

    while (keys.hasNext()) {
      final String key = keys.next();
      System.out.println(key + "  : " + config.getProperty(key));
    }
    return config;
  }

  public static void main(String[] args) throws IOException {
    final TestingServerPropertiesBuilder bld = new TestingServerPropertiesBuilder("testTable");
    bld.build();
  }
}
