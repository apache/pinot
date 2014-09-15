package com.linkedin.pinot.integration.tests;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.data.readers.RecordReaderFactory;
import com.linkedin.pinot.core.indexsegment.creator.SegmentCreator;
import com.linkedin.pinot.core.indexsegment.creator.SegmentCreatorFactory;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfiguration;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.time.SegmentTimeUnit;
import com.linkedin.pinot.integration.tests.helpers.DataGenerator;
import com.linkedin.pinot.integration.tests.helpers.DataGeneratorSpec;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 14, 2014
 */

public class FileBasedSentineTest {
  private static final Logger logger = Logger.getLogger(FileBasedSentineTest.class);
  private static URL url;

  FileBasedServerBrokerStarters starter;
  DataGenerator generator;
  private static File avroDataDir = new File("/tmp/avroFiles");

  @BeforeClass
  public void setup() throws Exception {
    url = new URL("http://localhost:"+FileBasedServerBrokerStarters.BROKER_CLIENT_PORT+"/query");

    // lets generate data
    final String[] columns = { "dimention1", "dimention2", "dimention3", "dimention4", "metric1", "daysSinceEpoch" };
    final Map<String, DataType> dataTypes = new HashMap<String, FieldSpec.DataType>();
    final Map<String, Integer> cardinality = new HashMap<String, Integer>();
    for (final String col : columns) {
      dataTypes.put(col, DataType.INT);
      cardinality.put(col, 1000);
    }

    if (avroDataDir.exists()) {
      FileUtils.deleteDirectory(avroDataDir);
    }

    final DataGeneratorSpec spec =
        new DataGeneratorSpec(Arrays.asList(columns), cardinality, dataTypes, FileFormat.avro, "/tmp/avroFiles", true);
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
        final SegmentGeneratorConfiguration genConfig =
            SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(avro, new File(bootstrapDir, "segment-" + counter),
                "daysSinceEpoch", SegmentTimeUnit.days, resource, resource);

        final SegmentCreator cr = SegmentCreatorFactory.get(SegmentVersion.v1, RecordReaderFactory.get(genConfig));
        cr.init(genConfig);
        cr.buildSegment();

        counter++;
      }
    }

    // lets start the server and the broker now

    starter = new FileBasedServerBrokerStarters();
    starter.startAll();


    // pick some values from here if you need to use it for running filter queries

    final JSONObject selectionRequestResponse = postQuery("select * from resource1 limit 100");

  }

  @AfterClass
  public void tearDown() throws IOException {
    starter.stopAll();

    //  FileUtils.deleteDirectory(new File(FileBasedServerBrokerStarters.SERVER_BOOTSTRAP_DIR));
    //    FileUtils.deleteDirectory(avroDataDir);
    //    FileUtils.deleteDirectory(new File(FileBasedServerBrokerStarters.SERVER_INDEX_DIR));ss
  }

  @Test
  public void test1() {

  }

  public static JSONObject postQuery(String query) throws Exception {

    final JSONObject json = new JSONObject();
    json.put("pql", query);

    final long start = System.currentTimeMillis();
    final URLConnection conn = url.openConnection();
    conn.setDoOutput(true);
    final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream(), "UTF-8"));
    final String reqStr = json.toString();

    //System.out.println("req: " + reqStr);
    writer.write(reqStr, 0, reqStr.length());
    writer.flush();
    final BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));

    final StringBuilder sb = new StringBuilder();
    String line = null;
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }

    final long stop = System.currentTimeMillis();

    logger.info(" Time take for Request : " + query + " in ms:" + (stop - start));

    final String res = sb.toString();
    // System.out.println("res: " + res);
    final JSONObject ret = new JSONObject(res);
    if (ret.opt("totaldocs") != null) {
      // assertEquals(15000L, ret.getLong("totaldocs"));
    }
    ret.put("totalTime", (stop - start));

    return ret;
  }
}
