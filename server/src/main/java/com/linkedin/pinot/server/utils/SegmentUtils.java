/*******************************************************************************
 * © [2013] LinkedIn Corp. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.linkedin.pinot.server.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.linkedin.pinot.index.segment.IndexSegment;
import com.linkedin.pinot.server.partition.PartitionDataManager;
import com.linkedin.pinot.server.zookeeper.SegmentInfo;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;


public class SegmentUtils {
  private static Logger logger = Logger.getLogger(PartitionDataManager.class);

  private static final Timer segmentSuccesfulInstantiateTime = Metrics.newTimer(new MetricName(
      PartitionDataManager.class, "segmentTotalSuccesfulInstantiateTime"), TimeUnit.MILLISECONDS, TimeUnit.DAYS);
  private static final Timer segmentFailedInstantiateTime = Metrics.newTimer(new MetricName(PartitionDataManager.class,
      "segmentTotalFailedInstantiateTime"), TimeUnit.MILLISECONDS, TimeUnit.DAYS);
  private static final Timer segmentLoadIntoMemoryTime = Metrics.newTimer(new MetricName(PartitionDataManager.class,
      "segmentLoadIntoMemoryTime"), TimeUnit.MILLISECONDS, TimeUnit.DAYS);
  private static final Timer segmentDownloadTime = Metrics.newTimer(new MetricName(PartitionDataManager.class,
      "segmentDownloadTime"), TimeUnit.MILLISECONDS, TimeUnit.DAYS);
  private static final Counter segmentDownloadSpeed = Metrics.newCounter(PartitionDataManager.class,
      "segmentDownloadSpeed");

  public static IndexSegment instantiateSegment(String segmentId, SegmentInfo segmentInfo, Configuration config) {
    long time = System.currentTimeMillis();
    List<String> uris = new ArrayList<String>(segmentInfo.getPathUrls());
    IndexSegment ret = null;
    boolean success = false;
    Collections.shuffle(uris);
    for (String currentUri : uris) {
      currentUri = currentUri.trim();
      logger.info("trying to load segment  + " + segmentId + ", by uri - " + currentUri);
      ret = instantiateSegmentForUri(segmentId, currentUri, config);
      if (ret != null) {
        success = true;
        logger.info("Succesfully loaded  segment - " + segmentId + " by the uri " + currentUri);
        break;
      } else {
        logger.warn("Couldn't load the segment by the uri " + currentUri);
      }
    }

    long duration = System.currentTimeMillis() - time;
    if (!success) {
      logger.warn("[final]Failed to load the segment - " + segmentId + ", by the collection of uris"
          + segmentInfo.getPathUrls());
      segmentFailedInstantiateTime.update(duration, TimeUnit.MILLISECONDS);
      logger.error("[final]Failed to load the segment - " + segmentId + ", by the uris -" + segmentInfo.getPathUrls());
    } else {
      segmentSuccesfulInstantiateTime.update(duration, TimeUnit.MILLISECONDS);

    }
    return ret;
  }

  public static IndexSegment instantiateSegmentForUri(String segmentId, String uri, Configuration config) {
    try {
      List<File> uncompressedFiles = null;
      if (uri.startsWith("hdfs:")) {
        throw new UnsupportedOperationException("Not implemented yet");
      } else {
        if (uri.startsWith("http:")) {
          File tempFile = new File(config.getString("IndexDir"), segmentId + ".tar.gz");
          long downloadTime = System.currentTimeMillis();
          long httpGetResponseContentLength = FileUploadUtils.getFile(uri, tempFile);
          logger.info("Http GET response content length: " + httpGetResponseContentLength
              + ", Length of downloaded file : " + tempFile.length());
          long duration = System.currentTimeMillis() - downloadTime;
          segmentDownloadTime.update(duration, TimeUnit.MILLISECONDS);
          segmentDownloadSpeed.clear();
          segmentDownloadSpeed.inc(tempFile.length() / (duration * 1000));
          logger.info("Downloaded file from " + uri);
          uncompressedFiles = TarGzCompressionUtils.unTar(tempFile, new File(config.getString("IndexDir")));
          FileUtils.deleteQuietly(tempFile);
        } else {
          uncompressedFiles = TarGzCompressionUtils.unTar(new File(uri), new File(config.getString("IndexDir")));
        }
        File file = new File(config.getString("IndexDir"), segmentId);
        logger.info("Uncompressed segment into " + file.getAbsolutePath());
        Thread.sleep(100);
        if (uncompressedFiles.size() > 0 && !segmentId.equals(uncompressedFiles.get(0).getName())) {
          if (file.exists()) {
            logger.info("Deleting the directory and recreating it again- " + file.getAbsolutePath());
            FileUtils.deleteDirectory(file);
          }
          File srcDir = uncompressedFiles.get(0);
          logger.warn("The directory - " + file.getAbsolutePath() + " doesn't exist. Would try to rename the dir - "
              + srcDir.getAbsolutePath() + " to it. The segment id is - " + segmentId);
          FileUtils.moveDirectory(srcDir, file);
          if (!new File(config.getString("IndexDir"), segmentId).exists()) {
            throw new IllegalStateException("The index directory hasn't been created");
          } else {
            logger.info("Was able to succesfully rename the dir to match the segmentId - " + segmentId);
          }
        }
        new File(file, "finishedLoading").createNewFile();
        long loadTime = System.currentTimeMillis();

        IndexSegment indexSegment = SegmentLoader.loadIndexSegmentFromDir(file);

        if (indexSegment != null) {
          logger.info("Loaded the new segment " + segmentId);
        }

        if (indexSegment == null) {
          FileUtils.deleteDirectory(file);
          throw new IllegalStateException("The directory " + file.getAbsolutePath()
              + " doesn't contain the fully loaded segment");
        }
        segmentLoadIntoMemoryTime.update(System.currentTimeMillis() - loadTime, TimeUnit.MILLISECONDS);
        return indexSegment;
      }
    } catch (Exception ex) {
      logger.error(ex.getMessage(), ex);
    }
    return null;
  }
}
