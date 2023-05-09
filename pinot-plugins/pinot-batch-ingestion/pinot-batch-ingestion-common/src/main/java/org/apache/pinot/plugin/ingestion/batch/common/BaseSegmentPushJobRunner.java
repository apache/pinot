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
package org.apache.pinot.plugin.ingestion.batch.common;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.segment.generation.SegmentGenerationUtils;
import org.apache.pinot.segment.local.utils.ConsistentDataPushUtils;
import org.apache.pinot.segment.local.utils.SegmentPushUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.runner.IngestionJobRunner;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;

public abstract class BaseSegmentPushJobRunner implements IngestionJobRunner {

  protected SegmentGenerationJobSpec _spec;
  protected String[] _files;
  protected PinotFS _outputDirFS;
  protected URI _outputDirURI;
  protected TableConfig _tableConfig;
  protected boolean _consistentPushEnabled;

  /**
   * Initialize BaseSegmentPushJobRunner with SegmentGenerationJobSpec
   * Checks for required parameters in the spec and enablement of consistent data push.
   */
  @Override
  public void init(SegmentGenerationJobSpec spec) {
    _spec = spec;
    if (_spec.getPushJobSpec() == null) {
      throw new RuntimeException("Missing PushJobSpec");
    }

    // Read Table spec
    if (_spec.getTableSpec() == null) {
      throw new RuntimeException("Missing tableSpec");
    }

    // Read Table config
    if (_spec.getTableSpec().getTableConfigURI() != null) {
      _tableConfig =
          SegmentGenerationUtils.getTableConfig(_spec.getTableSpec().getTableConfigURI(), spec.getAuthToken());
      _consistentPushEnabled = ConsistentDataPushUtils.consistentDataPushEnabled(_tableConfig);
    }
  }

  /**
   * Initialize filesystems and obtain the raw input files for upload.
   */
  public void initFileSys() {
    // Init all file systems
    List<PinotFSSpec> pinotFSSpecs = _spec.getPinotFSSpecs();
    for (PinotFSSpec pinotFSSpec : pinotFSSpecs) {
      PinotFSFactory.register(pinotFSSpec.getScheme(), pinotFSSpec.getClassName(), new PinotConfiguration(pinotFSSpec));
    }

    // Get outputFS for writing output Pinot segments
    try {
      _outputDirURI = new URI(_spec.getOutputDirURI());
      if (_outputDirURI.getScheme() == null) {
        _outputDirURI = new File(_spec.getOutputDirURI()).toURI();
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException("outputDirURI is not valid - '" + _spec.getOutputDirURI() + "'");
    }
    _outputDirFS = PinotFSFactory.create(_outputDirURI.getScheme());

    // Get list of files to process
    try {
      _files = _outputDirFS.listFiles(_outputDirURI, true);
    } catch (IOException e) {
      throw new RuntimeException("Unable to list all files under outputDirURI - '" + _outputDirURI + "'");
    }
  }

  /**
   * Returns segment names, which will be supplied to the segment replacement protocol as the new set of segments to
   * atomically update when consistent data push is enabled.
   * @param segmentsUriToTarPathMap Map from segment URI to corresponding tar path. Either the URIs (keys), the
   *                                tarPaths (values), or both may be used depending on upload mode.
   */
  public List<String> getSegmentsToReplace(Map<String, String> segmentsUriToTarPathMap) {
    Collection<String> tarFilePaths = segmentsUriToTarPathMap.values();
    List<String> segmentNames = new ArrayList<>(tarFilePaths.size());
    for (String tarFilePath : tarFilePaths) {
      File tarFile = new File(tarFilePath);
      String fileName = tarFile.getName();
      Preconditions.checkArgument(fileName.endsWith(Constants.TAR_GZ_FILE_EXT));
      String segmentName = fileName.substring(0, fileName.length() - Constants.TAR_GZ_FILE_EXT.length());
      segmentNames.add(segmentName);
    }
    return segmentNames;
  }

  /**
   * Upload segments supplied in segmentsUriToTarPathMap.
   * @param segmentsUriToTarPathMap Map from segment URI to corresponding tar path. Either the URIs (keys), the
   *                                tarPaths (values), or both may be used depending on upload mode.
   */
  public abstract void uploadSegments(Map<String, String> segmentsUriToTarPathMap)
      throws Exception;

  /**
   * Runs the main logic of the segment push job runner.
   * First initialize the filesystem, then upload the segments, while optionally configured to be wrapped around by
   * the consistent data push protocol.
   */
  @Override
  public void run()
      throws Exception {
    initFileSys();
    Map<URI, String> uriToLineageEntryIdMap = null;
    try {
      Map<String, String> segmentsUriToTarPathMap =
          SegmentPushUtils.getSegmentUriToTarPathMap(_outputDirURI, _spec.getPushJobSpec(), _files);
      if (_consistentPushEnabled) {
        List<String> segmentsToReplace = getSegmentsToReplace(segmentsUriToTarPathMap);
        uriToLineageEntryIdMap = ConsistentDataPushUtils.preUpload(_spec, segmentsToReplace);
      }
      uploadSegments(segmentsUriToTarPathMap);
      if (_consistentPushEnabled) {
        ConsistentDataPushUtils.postUpload(_spec, uriToLineageEntryIdMap);
      }
    } catch (Exception e) {
      if (_consistentPushEnabled) {
        ConsistentDataPushUtils.handleUploadException(_spec, uriToLineageEntryIdMap, e);
      }
      throw e;
    }
  }
}
