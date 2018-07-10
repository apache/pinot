/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.hadoop;

import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Properties;

import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.hadoop.job.SegmentCreationJob;
import com.linkedin.pinot.hadoop.job.SegmentTarPushJob;
import com.linkedin.pinot.hadoop.job.SegmentUriPushJob;


public class PinotHadoopJobLauncher {

  enum PinotHadoopJobType {
    SegmentCreation,
    SegmentTarPush,
    SegmentUriPush,
    SegmentCreationAndTarPush,
    SegmentCreationAndUriPush
  }

  private static final String USAGE = "usage: [job_type] [job.properties]";
  private static final String SUPPORT_JOB_TYPES = "\tsupport job types: " + Arrays.toString(PinotHadoopJobType.values());
  private static final String SEGMENT_CREATION_JOB_NAME = PinotHadoopJobType.SegmentCreation.toString();
  private static final String SEGMENT_PUSH_TAR_JOB_NAME = PinotHadoopJobType.SegmentTarPush.toString();
  private static final String SEGMENT_PUSH_URI_JOB_NAME = PinotHadoopJobType.SegmentUriPush.toString();
  private static final String SEGMENT_CREATION_AND_TAR_PUSH_JOB_NAME = PinotHadoopJobType.SegmentCreationAndTarPush.toString();
  private static final String SEGMENT_CREATION_AND_URI_PUSH_JOB_NAME = PinotHadoopJobType.SegmentCreationAndUriPush.toString();

  private static void usage() {
    System.err.println(USAGE);
    System.err.println(SUPPORT_JOB_TYPES);
  }

  private static void kickOffPinotHadoopJob(PinotHadoopJobType jobType, Properties jobConf) throws Exception {
    switch (jobType) {
      case SegmentCreation:
        new SegmentCreationJob(SEGMENT_CREATION_JOB_NAME, jobConf).run();
        break;
      case SegmentTarPush:
        new SegmentTarPushJob(SEGMENT_PUSH_TAR_JOB_NAME, jobConf).run();
        break;
      case SegmentUriPush:
        new SegmentUriPushJob(SEGMENT_PUSH_URI_JOB_NAME, jobConf).run();
        break;
      case SegmentCreationAndTarPush:
        new SegmentCreationJob(StringUtil.join(":", SEGMENT_CREATION_JOB_NAME, SEGMENT_CREATION_AND_TAR_PUSH_JOB_NAME), jobConf).run();
        new SegmentTarPushJob(StringUtil.join(":", SEGMENT_PUSH_TAR_JOB_NAME, SEGMENT_CREATION_AND_TAR_PUSH_JOB_NAME), jobConf).run();
        break;
      case SegmentCreationAndUriPush:
        new SegmentCreationJob(StringUtil.join(":", SEGMENT_CREATION_JOB_NAME, SEGMENT_CREATION_AND_URI_PUSH_JOB_NAME), jobConf).run();
        new SegmentUriPushJob(StringUtil.join(":", SEGMENT_PUSH_TAR_JOB_NAME, SEGMENT_CREATION_AND_URI_PUSH_JOB_NAME), jobConf).run();
        break;
      default:
        throw new RuntimeException("Not a valid jobType - " + jobType);
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      usage();
      System.exit(1);
    }
    PinotHadoopJobType jobType = null;
    Properties jobConf = null;
    try {
      jobType = PinotHadoopJobType.valueOf(args[0]);
      jobConf = new Properties();
      jobConf.load(new FileInputStream(args[1]));
    } catch (Exception e) {
      usage();
      System.exit(1);
    }
    kickOffPinotHadoopJob(jobType, jobConf);
  }

}
