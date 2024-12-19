package org.apache.pinot.tools.predownload;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


enum PredownloadCompleteReason {
  INSTANCE_NOT_ALIVE(false, false, "%s is not alive in cluster %s"),
  INSTANCE_NON_EXISTENT(false, false, "%s does not exist in cluster %s"),
  CANNOT_CONNECT_TO_DEEPSTORE(false, true, "cannot connect to deepstore for %s in cluster %s"),
  SOME_SEGMENTS_DOWNLOAD_FAILED(false, true, "some segments failed to predownload for %s in cluster %s"),
  NO_SEGMENT_TO_PREDOWNLOAD("no segment to predownload for %s in cluster %s"),
  ALL_SEGMENTS_DOWNLOADED("all segments are predownloaded for %s in cluster %s");

  private static final String FAIL_MESSAGE = "Failed to predownload segments for %s.%s, because ";
  private static final String SUCCESS_MESSAGE = "Successfully predownloaded segments for %s.%s, because ";
  private final boolean _retriable; // Whether the failure is retriable.
  private final boolean _isSucceed; // Whether the predownload is successful.
  private final String _message;
  private final String _messageTemplate;

  PredownloadCompleteReason(boolean isSucceed, boolean retriable, String message) {
    _isSucceed = isSucceed;
    _messageTemplate = isSucceed ? SUCCESS_MESSAGE : FAIL_MESSAGE;
    _retriable = retriable;
    _message = message;
  }

  PredownloadCompleteReason(String message) {
    this(true, false, message);
  }

  public boolean isRetriable() {
    return _retriable;
  }

  public boolean isSucceed() {
    return _isSucceed;
  }

  public String getMessage(String clusterName, String instanceName, String segmentName) {
    return String.format(_messageTemplate + _message, instanceName, segmentName, instanceName, clusterName);
  }
}

// Record the status of the pre-download to be consumed by odin-pinot-worker.
public class StatusRecorder {
  private static final Logger LOGGER = LoggerFactory.getLogger(StatusRecorder.class);
  private static final long STATUS_RECORD_EXPIRATION_SEC = 3600 * 6; // 6 hours
  // TODO: make those name configurable
  private static final String SUCCESS_STATUS = "SUCCESS_%s";
  private static final String FAILURE_STATUS = "FAILURE_%s";
  private static final String NON_RETRIABLE_FAILURE_STATUS = "NONRETRIABLEFAILURE_%s";
  private static String _statusRecordFolder = "/shared/predownload/status/";
  @Nullable
  private static PredownloadMetrics _predownloadMetrics;

  private StatusRecorder() {
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
  }

  public static void registerMetrics(PredownloadMetrics predownloadMetrics) {
    StatusRecorder._predownloadMetrics = predownloadMetrics;
  }

  @VisibleForTesting
  static void setStatusRecordFolder(String statusRecordFolder) {
    _statusRecordFolder = statusRecordFolder;
  }

  public static void predownloadComplete(PredownloadCompleteReason reason, String clusterName, String instanceName,
      String segmentName) {
    LOGGER.info(reason.getMessage(clusterName, instanceName, segmentName));
    if (_predownloadMetrics != null) {
      _predownloadMetrics.preDownloadComplete(reason);
    }
    if (reason.isSucceed()) {
      predownloadSucceeded(reason);
    } else {
      predownloadFailed(reason);
    }
  }

  private static void predownloadSucceeded(PredownloadCompleteReason reason) {
    File statusFolder = prepareStatusFolder();
    dumpSuccessfulStatus(statusFolder);
    System.exit(0);
  }

  private static void predownloadFailed(PredownloadCompleteReason reason) {
    if (reason.isRetriable()) {
      predownloadRetriableFailed(reason);
    } else {
      predownloadNonRetriableFailed(reason);
    }
  }

  private static void predownloadRetriableFailed(PredownloadCompleteReason reason) {
    File statusFolder = prepareStatusFolder();
    checkAndDumpRetriableFailureStatus(statusFolder);
    System.exit(1);
  }

  private static void predownloadNonRetriableFailed(PredownloadCompleteReason reason) {
    File statusFolder = prepareStatusFolder();
    dumpNonRetriableFailureStatus(statusFolder);
    System.exit(2);
  }

  private static File prepareStatusFolder() {
    File statusFolder = new File(_statusRecordFolder);
    try {
      if (statusFolder.isFile()) {
        FileUtils.forceDelete(statusFolder);
      }
      FileUtils.forceMkdir(statusFolder);
    } catch (IOException e) {
      LOGGER.error("Failed to create status folder", e);
    }
    return statusFolder;
  }

  private static void dumpSuccessfulStatus(File statusFolder) {
    try {
      // clean all previous status first and then create a new success status file.
      FileUtils.cleanDirectory(statusFolder);
      File successFile = new File(statusFolder, String.format(SUCCESS_STATUS, System.currentTimeMillis()));
      successFile.createNewFile();
    } catch (IOException e) {
      LOGGER.error("Failed to dump successful status to folder", e);
    }
  }

  private static void dumpNonRetriableFailureStatus(File statusFolder) {
    try {
      // clean all previous status first and then create a new non-retriable failure status file.
      FileUtils.cleanDirectory(statusFolder);
      File nonRetriableFailureFile =
          new File(statusFolder, String.format(NON_RETRIABLE_FAILURE_STATUS, System.currentTimeMillis()));
      nonRetriableFailureFile.createNewFile();
    } catch (IOException e) {
      LOGGER.error("Failed to dump non-retriable failure status to folder", e);
    }
  }

  private static void checkAndDumpRetriableFailureStatus(File statusFolder) {
    try {
      // clean up status success and non-retriable failure status files.
      // clean up retriable failure status files that are older than STATUS_RECORD_EXPIRATION_SEC.
      IOFileFilter toDeleteFileFilter =
          FileFilterUtils.ageFileFilter(System.currentTimeMillis() - STATUS_RECORD_EXPIRATION_SEC * 1000, true);
      toDeleteFileFilter = FileFilterUtils.or(toDeleteFileFilter,
          FileFilterUtils.prefixFileFilter(SUCCESS_STATUS.substring(0, FAILURE_STATUS.length() - 2)));
      toDeleteFileFilter = FileFilterUtils.or(toDeleteFileFilter,
          FileFilterUtils.prefixFileFilter(NON_RETRIABLE_FAILURE_STATUS.substring(0, FAILURE_STATUS.length() - 2)));
      Collection<File> toDeleteFiles = FileUtils.listFiles(statusFolder, toDeleteFileFilter, null);
      for (File file : toDeleteFiles) {
        FileUtils.forceDelete(file);
      }
      File retriableFailureFile = new File(statusFolder, String.format(FAILURE_STATUS, System.currentTimeMillis()));
      retriableFailureFile.createNewFile();
    } catch (IOException e) {
      LOGGER.error("Failed to dump retriable failure status to folder", e);
    }
  }
}
