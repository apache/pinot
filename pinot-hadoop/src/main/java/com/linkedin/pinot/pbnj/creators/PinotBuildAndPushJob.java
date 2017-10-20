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
package com.linkedin.pinot.pbnj.creators;

import azkaban.jobExecutor.AbstractJob;
import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.hadoop.job.JobConfigConstants;
import com.linkedin.pinot.hadoop.job.PushLocationTranslator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotBuildAndPushJob extends AbstractJob {

  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PinotBuildAndPushJob.class);

  private static GeneratePinotData _segmentCreationJob;
  private static String _disablePush;
  private static String _disableCreation;
  private static Properties _properties;

  private static final int PUSH_TIMEOUT_SECONDS = 60;
  private static final int MAX_RETRIES = 5;
  private static final int PUSH_SLEEP_BETWEEN_RETRIES_IN_SECONDS = 60;

  private static final String DISABLE_PUSH = "disable.push";
  private static final String DISABLE_CREATION = "disable.creation";

  @Override
  public void run() throws Exception {
    int maxParallelPushesPerColo = 1; // Default serial push per colo

    if (_disableCreation == null || _disableCreation.equalsIgnoreCase("false")) {
      LOGGER.info("Segment creation is enabled");
      _segmentCreationJob.run();
      _properties.setProperty(JobConfigConstants.PATH_TO_OUTPUT, _segmentCreationJob.getOutputDir());
      LOGGER.info("Setting " + JobConfigConstants.PATH_TO_OUTPUT + " in PinotBuildAndPushJob to: " + _properties.getProperty(JobConfigConstants.PATH_TO_OUTPUT));
    } else {
      LOGGER.info("Segment creation is disabled");
    }
    String pushLocation = _properties.getProperty("push.location");
    LOGGER.info("Push location is: " + pushLocation);
    PushLocationTranslator pushLocationTranslator = new PushLocationTranslator();
    List<String> pushLocations = pushLocationTranslator.getSegmentPostUris(pushLocation);

    String maxParallelPushesPerColoStr = _properties.getProperty(JobConfigConstants.PUSH_PARALLELISM_SEGMENTS, Integer.toString(maxParallelPushesPerColo));
    LOGGER.info("Max job parallelism is " + maxParallelPushesPerColoStr);
    try {
      maxParallelPushesPerColo = Integer.parseInt(maxParallelPushesPerColoStr);
      LOGGER.info("Set max parallel pushes to " + maxParallelPushesPerColo);
    } catch (Exception e) {
      LOGGER.warn("Could not convert " + JobConfigConstants.PUSH_PARALLELISM_SEGMENTS + "set to " + maxParallelPushesPerColoStr + " to int. Default value " + maxParallelPushesPerColo);
    }

    if (_disablePush == null || _disablePush.equalsIgnoreCase("false")) {
      LOGGER.info("Segment push is enabled");
      FileSystem fs = FileSystem.get(new Configuration());

      int pushTimeoutMs = PUSH_TIMEOUT_SECONDS * 1000;

      try {
        String maxTimeoutSegmentPushSeconds = _properties.getProperty(JobConfigConstants.MAX_TIMEOUT_SEGMENT_PUSH_SECONDS);
        LOGGER.info("max.timeout.segment.push.seconds is " + maxTimeoutSegmentPushSeconds);
        if (maxTimeoutSegmentPushSeconds != null) {
          pushTimeoutMs = Integer.parseInt(maxTimeoutSegmentPushSeconds) * 1000;
        }
      } catch (Exception e) {
        LOGGER.warn("Could not parse max timeout seconds {}", e);
      }

      Path dirPath = new Path(_segmentCreationJob.getOutputDir());
      List<Path> tarFilePathList = new ArrayList<Path>();
      FileStatus[] fileStatuses = fs.globStatus(dirPath);

      if (fileStatuses == null || fileStatuses.length == 0) {
        throw new RuntimeException("Input path: " + dirPath + " does not exist or does not contain any file");
      }
      getTarFilePathsFromDir(fs, fileStatuses, tarFilePathList);

      ArrayList<Future<?>> futuresPerColo = new ArrayList<>();
      ArrayList<ExecutorService> executorServices = new ArrayList<>();

      // Create a threadpool for each host of size max parallel pushes per colo
      try {
        for (int i = 0; i < pushLocations.size(); i++) {
          ExecutorService fixedPoolPerColo = Executors.newFixedThreadPool(maxParallelPushesPerColo);
          executorServices.add(fixedPoolPerColo);
          for (Path filePath : tarFilePathList) {
            LOGGER.info("Scheduling file to " + filePath + " to " + pushLocations.get(i));

            futuresPerColo.add(fixedPoolPerColo
                .submit(
                    new PushFileToHostCallable(fs, filePath, pushLocations.get(i), pushTimeoutMs, MAX_RETRIES, PUSH_SLEEP_BETWEEN_RETRIES_IN_SECONDS)));
          }
        }
        for (int i = 0; i < futuresPerColo.size(); i++) {
          futuresPerColo.get(i).get();
        }
      } catch (Exception e) {
        LOGGER.info("At least one of your pushes failed. Please retry with " + DISABLE_CREATION + " set to true and "
            + JobConfigConstants.PATH_TO_OUTPUT + " to your generated segment path. For common causes, "
            + "See https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Failures+for+PBNJ");
        throw new RuntimeException(e);
      } finally {
        for (ExecutorService executorService : executorServices) {
          executorService.shutdownNow();
        }
      }
    } else {
      LOGGER.info("Segment push is disabled");
    }
  }

  public PinotBuildAndPushJob(String name, azkaban.utils.Props jobProps) throws Exception {
    super(name, Logger.getLogger(name));

    _properties = jobProps.toProperties();
    LOGGER.info("properties: " + _properties);

    _disablePush = _properties.getProperty(DISABLE_PUSH);
    _disableCreation = _properties.getProperty(DISABLE_CREATION);

    _segmentCreationJob = new GeneratePinotData(name, _properties);
  }

  /**
   * Recursively add all tar file paths under the {@link FileStatus} array into the file paths list.
   */
  private static void getTarFilePathsFromDir(FileSystem fs, FileStatus[] fileStatuses, List<Path> tarFilePathList)
      throws IOException {
    for (FileStatus fileStatus : fileStatuses) {
      Path filePath = fileStatus.getPath();
      String filePathString = filePath.toString();
      if (fileStatus.isDirectory()) {
        LOGGER.info("Trying to add all tar files from directory: " + filePathString);
        getTarFilePathsFromDir(fs, fs.listStatus(filePath), tarFilePathList);
      } else {
        if (filePathString.endsWith(JobConfigConstants.TARGZ)) {
          LOGGER.info("  Adding tar file: " + filePathString);
          tarFilePathList.add(filePath);
        }
      }
    }
  }

  /**
   * Callable class which pushes one file to one host.
   * <p>Throw exception when push failed.
   */
  private static class PushFileToHostCallable implements Callable<Void> {
    private static final int FILE_EXTENSION_LENGTH = ".tar.gz".length();
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PinotBuildAndPushJob.class);

    private final FileSystem _fs;
    private final Path _filePath;
    private final String _uri;
    private final int _pushTimeoutMs;
    private final int _maxRetries;
    private final int _sleepTimeSec;

    public PushFileToHostCallable(FileSystem fs, Path filePath, String uri, int pushTimeoutMs,
        int maxRetries, int sleepTimeSec) {
      _fs = fs;
      _filePath = filePath;
      _uri = uri;
      _pushTimeoutMs = pushTimeoutMs;
      _maxRetries = maxRetries;
      _sleepTimeSec = sleepTimeSec;
    }

    @Override
    public Void call()
        throws Exception {
      String fileName = _filePath.getName();
      String segmentName = fileName.substring(0, fileName.length() - FILE_EXTENSION_LENGTH);
      LOGGER.info("Sending " + segmentName + " to location " + _uri);
      int responseCode =
          FileUploadUtils
              .sendSegment(_uri, segmentName, _pushTimeoutMs, _filePath, _fs, _maxRetries, _sleepTimeSec);

      LOGGER.info(
          "************** Response code: " + responseCode + " received for file " + fileName + "from uri " + _uri
              + "**************");
      return null;
    }
  }
}
