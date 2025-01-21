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
package org.apache.pinot.plugin.stream.push;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


public class PushBasedIngestionConsumerTest {
  private static final String STREAM_TYPE = "push";
  private static final String TABLE_NAME_WITH_TYPE = "pushTest_REALTIME";
  private static final String STREAM_NAME = "push-test";
  private static final int TIMEOUT = 1000;
  private static final int NUM_RECORDS = 10;
  private static final String DUMMY_RECORD_PREFIX = "DUMMY_RECORD-";

  private PushBasedIngestionConfig _config;
  private PushBasedIngestionBufferManager _bufferManager;
  private List<BufferedRecord> _records;
  private PushApiApplication _pushApiApplication;

  private PushBasedIngestionConfig getConfig() {
    Map<String, String> props = new HashMap<>();
    props.put(StreamConfigProperties.STREAM_TYPE, STREAM_TYPE);
    props.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_TOPIC_NAME),
        STREAM_NAME);
    props.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), PushBasedIngestionConsumerFactory.class.getName());
    props.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_DECODER_CLASS),
        "org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder");
    return new PushBasedIngestionConfig(new StreamConfig(TABLE_NAME_WITH_TYPE, props));
  }

  @BeforeClass
  public void setUp()
      throws InterruptedException, IOException {
    _bufferManager = new PushBasedIngestionBufferManager();
    _pushApiApplication = new PushApiApplication(_bufferManager);
    _pushApiApplication.start(8989);
    _config = getConfig();
    _records = new ArrayList<>(NUM_RECORDS);

    for (int i = 0; i < NUM_RECORDS; i++) {
      byte[] record = (DUMMY_RECORD_PREFIX + i).getBytes(StandardCharsets.UTF_8);
      postRecord(record);
    }
  }

  private void postRecord(byte[] record) throws IOException {
    URL url = new URL("http://localhost:8989/ingestion/records/" + TABLE_NAME_WITH_TYPE);
    URLConnection con = url.openConnection();
    HttpURLConnection http = (HttpURLConnection) con;
    http.setRequestMethod("POST"); // PUT is another valid option
    http.setDoOutput(true);
    int length = record.length;
    http.setFixedLengthStreamingMode(length);
    http.setRequestProperty("Content-Type", "application/octet-stream");
    http.setRequestProperty("Content-Length", Integer.toString(length));
    http.connect();
    try (OutputStream os = http.getOutputStream()) {
      os.write(record);
    }
    assertEquals(200, http.getResponseCode());
  }

  @AfterClass
  public void shutDown() {
    _pushApiApplication.stop();
  }

  @Test
  public void testBasicConsumer() {
    PushBasedIngestionConsumer consumer = new PushBasedIngestionConsumer(_config, _bufferManager);
    // Fetch first batch
    LongMsgOffset startOffset = new LongMsgOffset(0);
    PushBasedIngestionMessageBatch messageBatch = consumer.fetchMessages(startOffset, TIMEOUT);
    assertEquals(messageBatch.getMessageCount(), NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      assertEquals(new String(messageBatch.getStreamMessage(i).getValue(), StandardCharsets.UTF_8),
          DUMMY_RECORD_PREFIX + i);
    }
    assertFalse(messageBatch.isEndOfPartitionGroup());
  }
}
