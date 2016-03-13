package com.linkedin.thirdeye.bootstrap.segment.push;

import static com.linkedin.thirdeye.bootstrap.segment.push.SegmentPushPhaseConstants.SEGMENT_PUSH_INPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.segment.push.SegmentPushPhaseConstants.SEGMENT_PUSH_CONTROLLER_HOSTS;
import static com.linkedin.thirdeye.bootstrap.segment.push.SegmentPushPhaseConstants.SEGMENT_PUSH_CONTROLLER_PORT;
import static com.linkedin.thirdeye.bootstrap.segment.push.SegmentPushPhaseConstants.SEGMENT_PUSH_TABLENAME;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.utils.FileUploadUtils;

public class SegmentPushPhase  extends Configured {

  private static String SEGMENT_JOINER = "_";

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPushPhase.class);
  private final String name;
  private final Properties props;
  private String[] hosts;
  private String port;
  private String tablename;
  private boolean uploadSuccess = true;
  private String segmentName = null;

  SegmentPushControllerAPIs segmentPushControllerAPIs;


  public SegmentPushPhase(String jobName, Properties properties) throws Exception {
    super(new Configuration());
    name = jobName;
    props = properties;
  }

  public void run() throws Exception {
    Configuration configuration = new Configuration();
    FileSystem fs = FileSystem.get(configuration);

    String segmentPath = getAndSetConfiguration(configuration, SEGMENT_PUSH_INPUT_PATH);
    LOGGER.info("Segment path : {}", segmentPath);
    hosts = getAndSetConfiguration(configuration, SEGMENT_PUSH_CONTROLLER_HOSTS).split(",");
    port = getAndSetConfiguration(configuration, SEGMENT_PUSH_CONTROLLER_PORT);
    tablename = getAndSetConfiguration(configuration, SEGMENT_PUSH_TABLENAME);


    Path path = new Path(segmentPath);
    FileStatus[] fileStatusArr = fs.globStatus(path);
    for (FileStatus fileStatus : fileStatusArr) {
      if (fileStatus.isDirectory()) {
        pushDir(fs, fileStatus.getPath());
      } else {
        pushOneTarFile(fs, fileStatus.getPath());
      }
    }

    if (uploadSuccess && segmentName != null) {
      segmentPushControllerAPIs = new SegmentPushControllerAPIs(hosts, port);
      LOGGER.info("Deleting segments overlapping to {} from table {}  ", segmentName, tablename);
      segmentPushControllerAPIs.deleteOverlappingSegments(tablename, segmentName);
    }

  }

  public void pushDir(FileSystem fs, Path path) throws Exception {
    LOGGER.info("******** Now uploading segments tar from dir: {}", path);
    FileStatus[] fileStatusArr = fs.listStatus(new Path(path.toString() + "/"));
    for (FileStatus fileStatus : fileStatusArr) {
      if (fileStatus.isDirectory()) {
        pushDir(fs, fileStatus.getPath());
      } else {
        pushOneTarFile(fs, fileStatus.getPath());
      }
    }
  }

  public void pushOneTarFile(FileSystem fs, Path path) throws Exception {
    String fileName = path.getName();
    if (!fileName.endsWith(".tar.gz")) {
      return;
    }
    long length = fs.getFileStatus(path).getLen();
    for (String host : hosts) {
      InputStream inputStream = null;
      try {
        inputStream = fs.open(path);
        fileName = fileName.split(".tar")[0];
        if (fileName.lastIndexOf(SEGMENT_JOINER) != -1) {
          segmentName = fileName.substring(0, fileName.lastIndexOf(SEGMENT_JOINER));
        }
        LOGGER.info("******** Upoading file: {} to Host: {} and Port: {} *******", fileName, host, port);
        try {
          int responseCode = FileUploadUtils.sendSegmentFile(host, port, fileName, inputStream, length);
          LOGGER.info("Response code: {}", responseCode);

          if (uploadSuccess == true && responseCode != 200) {
            uploadSuccess = false;
          }

        } catch (Exception e) {
          LOGGER.error("******** Error Upoading file: {} to Host: {} and Port: {}  *******", fileName, host, port);
          LOGGER.error("Caught exception during upload", e);
          throw new RuntimeException("Got Error during send tar files to push hosts!");
        }
      } finally {
        inputStream.close();
      }
    }
  }


  private String getAndSetConfiguration(Configuration configuration,
      SegmentPushPhaseConstants constant) {
    String value = getAndCheck(constant.toString());
    configuration.set(constant.toString(), value);
    return value;
  }

  private String getAndCheck(String propName) {
    String propValue = props.getProperty(propName);
    if (propValue == null) {
      throw new IllegalArgumentException(propName + " required property");
    }
    return propValue;
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("usage: config.properties");
    }

    Properties props = new Properties();
    props.load(new FileInputStream(args[0]));

    SegmentPushPhase job = new SegmentPushPhase("segment_push_job", props);
    job.run();
  }


}
