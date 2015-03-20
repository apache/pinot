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
package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.controller.helix.ControllerTest;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.integration.tests.helpers.DataGenerator;
import com.linkedin.pinot.integration.tests.helpers.DataGeneratorSpec;
import com.linkedin.pinot.server.util.SegmentTestUtils;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 14, 2014
 */

public class FileBasedSentineTest extends ControllerTest {
  private static final Logger logger = Logger.getLogger(FileBasedSentineTest.class);
  private static URL url;
  private static final String AVRO_FILE_PATH = "/tmp/avroFiles";
  FileBasedServerBrokerStarters starter;
  DataGenerator generator;
  private static File avroDataDir = new File(AVRO_FILE_PATH);

  @BeforeClass
  public void setup() throws Exception {
    url = new URL("http://localhost:" + FileBasedServerBrokerStarters.BROKER_CLIENT_PORT + "/query");

    // lets generate data
    final String[] columns = { "dimention1", "dimention2", "dimention3", "dimention4", "metric1", "daysSinceEpoch" };
    final Map<String, DataType> dataTypes = new HashMap<String, FieldSpec.DataType>();
    final Map<String, Integer> cardinality = new HashMap<String, Integer>();
    for (final String col : columns) {
      if (col.equals("dimention1")) {
        dataTypes.put(col, DataType.STRING);
        cardinality.put(col, 1000);
      } else {
        dataTypes.put(col, DataType.INT);
        cardinality.put(col, 1000);
      }
    }

    if (avroDataDir.exists()) {
      FileUtils.deleteDirectory(avroDataDir);
    }

    final DataGeneratorSpec spec =
        new DataGeneratorSpec(Arrays.asList(columns), cardinality, dataTypes, FileFormat.AVRO, avroDataDir.getAbsolutePath(), true);
    generator = new DataGenerator();
    generator.init(spec);
    generator.generate(100000L, 2);
    // lets make segments now

    final File bootstrapDir = new File(FileBasedServerBrokerStarters.SERVER_BOOTSTRAP_DIR);

    if (bootstrapDir.exists()) {
      FileUtils.deleteDirectory(bootstrapDir);
    }

    bootstrapDir.mkdir();

    int counter = 0;
    for (final File avro : avroDataDir.listFiles()) {
      for (final String resource : FileBasedServerBrokerStarters.RESOURCE_NAMES) {
        final SegmentGeneratorConfig genConfig =
            SegmentTestUtils
                .getSegmentGenSpecWithSchemAndProjectedColumns(avro, new File(bootstrapDir, "segment-" + counter),
                    "daysSinceEpoch", TimeUnit.DAYS, resource, resource);

        final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
        driver.init(genConfig);
        driver.build();

        counter++;
      }
    }

    // lets start the server and the broker now

    starter = new FileBasedServerBrokerStarters();
    starter.startAll();

    // pick some values from here if you need to use it for running filter queries

    final JSONObject selectionRequestResponse = postQuery("select * from 'resource1' limit 100",
        "http://localhost:" + FileBasedServerBrokerStarters.BROKER_CLIENT_PORT);

    System.out.println(selectionRequestResponse.toString(1));
  }

  @AfterClass
  public void tearDown() throws IOException {
    starter.stopAll();

    FileUtils.deleteDirectory(new File(FileBasedServerBrokerStarters.SERVER_BOOTSTRAP_DIR));
    FileUtils.deleteDirectory(avroDataDir);
    FileUtils.deleteDirectory(new File(FileBasedServerBrokerStarters.SERVER_INDEX_DIR));
  }

  @Test
  public void test1() {

  }

  @Override
  protected String getHelixClusterName() {
    return "FileBasedSentineTest";
  }
}
