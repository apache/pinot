/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.server.predownload;

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


// Helper class to test the System.exit() in UT
class ExitHelper {
  private static ExitAction _exitAction = new DefaultExitAction();

  private ExitHelper() {
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
  }

  public static void exit(int status) {
    _exitAction.exit(status);
  }

  public static void setExitAction(ExitAction action) {
    _exitAction = action;
  }

  public static void resetExitAction() {
    _exitAction = new DefaultExitAction();
  }

  public interface ExitAction {
    void exit(int status);
  }

  // Default implementation of ExitAction
  private static class DefaultExitAction implements ExitAction {
    @Override
    public void exit(int status) {
      System.exit(status);
    }
  }
}

// Record the status of the pre-download to be consumed by odin-pinot-worker.
public class PredownloadStatusRecorder {
  private static final Logger LOGGER = LoggerFactory.getLogger(PredownloadStatusRecorder.class);
  private static final long STATUS_RECORD_EXPIRATION_SEC = 3600 * 6; // 6 hours
  // TODO: make those name configurable
  private static final String SUCCESS_STATUS = "SUCCESS_%s";
  private static final String FAILURE_STATUS = "FAILURE_%s";
  private static final String NON_RETRIABLE_FAILURE_STATUS = "NONRETRIABLEFAILURE_%s";
  private static String _statusRecordFolder = "/shared/predownload/status/";
  @Nullable
  private static PredownloadMetrics _predownloadMetrics;

  private PredownloadStatusRecorder() {
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
  }

  public static void registerMetrics(PredownloadMetrics predownloadMetrics) {
    PredownloadStatusRecorder._predownloadMetrics = predownloadMetrics;
  }

  @VisibleForTesting
  static void setStatusRecordFolder(String statusRecordFolder) {
    _statusRecordFolder = statusRecordFolder;
  }

  public static void predownloadComplete(PredownloadCompletionReason reason, String clusterName, String instanceName,
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

  private static void predownloadSucceeded(PredownloadCompletionReason reason) {
    File statusFolder = prepareStatusFolder();
    dumpSuccessfulStatus(statusFolder);
    ExitHelper.exit(0);
  }

  private static void predownloadFailed(PredownloadCompletionReason reason) {
    if (reason.isRetriable()) {
      predownloadRetriableFailed(reason);
    } else {
      predownloadNonRetriableFailed(reason);
    }
  }

  private static void predownloadRetriableFailed(PredownloadCompletionReason reason) {
    File statusFolder = prepareStatusFolder();
    checkAndDumpRetriableFailureStatus(statusFolder);
    ExitHelper.exit(1);
  }

  private static void predownloadNonRetriableFailed(PredownloadCompletionReason reason) {
    File statusFolder = prepareStatusFolder();
    dumpNonRetriableFailureStatus(statusFolder);
    ExitHelper.exit(2);
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
