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
package org.apache.pinot.tools.predownload;

import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;


public class TestUtil {
  public static final String SAMPLE_PROPERTIES_FILE_NAME = "test_data/sample_pinot_server.properties";
  public static final String SEGMENT_NAME = "segmentName";
  public static final String SECOND_SEGMENT_NAME = "secondSegmentName";
  public static final String THIRD_SEGMENT_NAME = "thirdSegmentName";
  public static final String TABLE_NAME = "tableName";
  public static final String DOWNLOAD_URL = "downloadUrl";
  public static final String CRYPTER_NAME = "crypterName";
  public static final long CRC = 123;
  public static final String TIER = "tier_test";
  public static final String ZK_ADDRESS = "localhost";
  public static final String CLUSTER_NAME = "clusterName";
  public static final String INSTANCE_ID = "instanceId";
  public static final String TAG = "tag";
  public static final String INSTANCE_DATA_DIR = "dataDir";
  public static final String READ_MODE = "heap";
  public static final long DISK_SIZE_BYTES = 1000;
  public static final String SESSION_ID = "sessionId";

  private TestUtil() {
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
  }

  public static PinotConfiguration getPinotConfiguration() {
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty(CommonConstants.Server.INSTANCE_ID, INSTANCE_ID);
    pinotConfiguration.setProperty(CommonConstants.Server.INSTANCE_DATA_DIR, INSTANCE_DATA_DIR);
    pinotConfiguration.setProperty(CommonConstants.Server.READ_MODE, READ_MODE);
    return pinotConfiguration;
  }

  public static SegmentZKMetadata createSegmentZKMetadata() {
    SegmentZKMetadata metadata = new SegmentZKMetadata(SEGMENT_NAME);
    metadata.setDownloadUrl(DOWNLOAD_URL);
    metadata.setCrc(CRC);
    metadata.setCrypterName(CRYPTER_NAME);
    metadata.setTier(TIER);
    return metadata;
  }
}
