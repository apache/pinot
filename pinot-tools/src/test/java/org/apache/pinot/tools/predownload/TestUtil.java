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
