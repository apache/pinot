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
package com.linkedin.pinot.tools.admin.command;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.linkedin.pinot.common.data.DateTimeFieldSpec;
import com.linkedin.pinot.common.data.StarTreeIndexSpec;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.segment.StarTreeMetadata;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.SegmentType;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.data.readers.PinotSegmentRecordReader;
import com.linkedin.pinot.core.data.readers.BackfillDateTimeRecordReader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.RecordReaderSegmentCreationDataSource;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.tools.Command;
import com.linkedin.pinot.tools.backfill.BackfillSegmentUtils;


/**
 * Class to download a segment, and backfill it with dateTimeFieldSpec corresponding to the timeFieldSpec
 *
 */
public class BackfillSegmentColumnCommand extends AbstractBaseAdminCommand implements Command {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String OUTPUT_FOLDER = "output";
  private static final String DOWNLOAD_FOLDER = "download";
  private static final String BACKUP_FOLDER = "backup";

  private static final Logger LOGGER = LoggerFactory.getLogger(BackfillSegmentColumnCommand.class);

  @Option(name = "-controllerHost", required = true, metaVar = "<String>", usage = "host name for controller.")
  private String _controllerHost;

  @Option(name = "-controllerPort", required = true, metaVar = "<int>", usage = "Port number for controller.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @Option(name = "-tableName", required = true, metaVar = "<string>", usage = "Name of the table to backfill")
  private String _tableName;

  @Option(name = "-segmentName", required = false, metaVar = "<string>",
      usage = "Name of the segment to backfill (if not specified, all segments will be backfilled)")
  private String _segmentName;

  @Option(name = "-segmentType", required = false, metaVar = "<OFFLINE/REALTIME>",
      usage = "Type of segments to backfill (if not specified, all types will be backfilled)")
  private SegmentType _segmentType;

  @Option(name = "-srcTimeFieldSpec", required = true, metaVar = "<string>", usage = "File containing timeFieldSpec as json")
  private String _srcTimeFieldSpec;

  @Option(name = "-destDateTimeFieldSpec", required = true, metaVar = "<string>", usage = "File containing dateTimeFieldSpec as json")
  private String _destDateTimeFieldSpec;


  @Option(name = "-backupDir", required = false, metaVar = "<string>", usage = "Path to backup segments")
  private String _backupDir;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"},
      usage = "Print this message.")
  private boolean _help = false;


  public BackfillSegmentColumnCommand setControllerHost(String controllerHost) {
    _controllerHost = controllerHost;
    return this;
  }

  public BackfillSegmentColumnCommand setControllerPort(String controllerPort) {
    _controllerPort = controllerPort;
    return this;
  }

  public BackfillSegmentColumnCommand setTableName(String tableName) {
    _tableName = tableName;
    return this;
  }


  public BackfillSegmentColumnCommand setSegmentName(String segmentName) {
    _segmentName = segmentName;
    return this;
  }

  public BackfillSegmentColumnCommand setSegmentType(SegmentType segmentType) {
    _segmentType = segmentType;
    return this;
  }

  public BackfillSegmentColumnCommand setSrcTimeFieldSpec(String srcTimeFieldSpec) {
    _srcTimeFieldSpec = srcTimeFieldSpec;
    return this;
  }

  public BackfillSegmentColumnCommand setDestDateTimeFieldSpec(String destDateTimeFieldSpec) {
    _destDateTimeFieldSpec = destDateTimeFieldSpec;
    return this;
  }

  public BackfillSegmentColumnCommand setBackupDir(String backupDir) {
    _backupDir = backupDir;
    return this;
  }


  @Override
  public String toString() {
    return ("BackfillSegmentColumn  -controllerHost " + _controllerHost + " -controllerPort " + _controllerPort
        + " -tableName " + _tableName + " -segmentName " + _segmentName + " -segmentType " + _segmentType
        + " -srcTimeFieldSpec " + _srcTimeFieldSpec + " _destDateTimeFieldSpec " + _destDateTimeFieldSpec
        + " -backupDir " + _backupDir);
  }

  @Override
  public final String getName() {
    return "BackfillSegmentColumn";
  }

  @Override
  public String description() {
    return "Backfill a column in segments of a pinot table, with a millis value corresponding to the time column";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public boolean execute() throws Exception {
    LOGGER.info("Executing command: {}", toString());


    if (_controllerHost == null || _controllerPort == null) {
      throw new RuntimeException("Must specify controller host and port.");
    }

    if (_srcTimeFieldSpec == null || _destDateTimeFieldSpec == null) {
      throw new RuntimeException("Must specify srcTimeFieldSpec and destTimeFieldSpec.");
    }
    TimeFieldSpec timeFieldSpec = OBJECT_MAPPER.readValue(new File(_srcTimeFieldSpec), TimeFieldSpec.class);
    DateTimeFieldSpec dateTimeFieldSpec = OBJECT_MAPPER.readValue(new File(_destDateTimeFieldSpec), DateTimeFieldSpec.class);

    if (_tableName == null) {
      throw new RuntimeException("Must specify tableName.");
    }

    BackfillSegmentUtils backfillSegmentUtils =
        new BackfillSegmentUtils(_controllerHost, _controllerPort);

    List<String> segmentNames = new ArrayList<>();
    List<String> allSegmentNames = backfillSegmentUtils.getAllSegments(_tableName, _segmentType);
    if (_segmentName == null) {
      segmentNames = allSegmentNames;
    } else {
      if (allSegmentNames.contains(_segmentName)) {
        segmentNames.add(_segmentName);
      } else {
        throw new RuntimeException("Segment with name " + _segmentName + " does not exist.");
      }
    }

    if (_backupDir == null) {
      _backupDir = TMP_DIR;
    }
    File backupDir = new File(_backupDir, BACKUP_FOLDER);
    File tableBackupDir = new File(backupDir, _tableName);
    File downloadDir = new File(TMP_DIR, DOWNLOAD_FOLDER);
    LOGGER.info("Backup dir {}", tableBackupDir);
    LOGGER.info("DownloadDir {}", downloadDir);

    for (String segmentName : segmentNames) {

      LOGGER.info("*************** Segment {}", segmentName);

      // download segment
      File downloadSegmentDir = new File(downloadDir, segmentName);
      LOGGER.info("Downloading segment {} to {}", segmentName, downloadDir.getAbsolutePath());
      boolean downloadStatus = backfillSegmentUtils.downloadSegment(_tableName, segmentName, downloadSegmentDir, tableBackupDir);
      LOGGER.info("Download status for segment {} is {}", segmentName, downloadStatus);
      if (!downloadStatus) {
        LOGGER.error("Failed to download segment {}. Skipping it.", segmentName);
        continue;
      }

      // create new segment
      File segmentDir = new File(downloadSegmentDir, segmentName);
      SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDir);
      PinotSegmentRecordReader segmentRecordReader = new PinotSegmentRecordReader(segmentDir);
      // read only rawdocs, everything after that, if present, is generated by star tree
      BackfillDateTimeRecordReader wrapperReader =
          new BackfillDateTimeRecordReader(segmentRecordReader, timeFieldSpec, dateTimeFieldSpec, metadata.getTotalRawDocs());
      File outputDir = new File(downloadSegmentDir, OUTPUT_FOLDER);
      LOGGER.info("Segment dir: {} Output Dir: {}", segmentDir.getAbsolutePath(), outputDir.getAbsolutePath());

      LOGGER.info("Creating segment generator config for {}", segmentName);
      SegmentGeneratorConfig config = new SegmentGeneratorConfig();
      config.setInputFilePath(segmentDir.getAbsolutePath());
      config.setFormat(FileFormat.PINOT);
      config.setOutDir(outputDir.getAbsolutePath());
      config.setOverwrite(true);
      config.setTableName(_tableName);
      config.setSegmentName(segmentName);
      config.setSchema(wrapperReader.getSchema());

      StarTreeMetadata starTreeMetadata = metadata.getStarTreeMetadata();
      if (starTreeMetadata != null) {
        config.setEnableStarTreeIndex(true);
        StarTreeIndexSpec starTreeIndexSpec = new StarTreeIndexSpec();
        starTreeIndexSpec.setDimensionsSplitOrder(starTreeMetadata.getDimensionsSplitOrder());
        starTreeIndexSpec.setMaxLeafRecords((int) starTreeMetadata.getMaxLeafRecords());
        starTreeIndexSpec.setSkipMaterializationCardinalityThreshold((int) starTreeMetadata.getSkipMaterializationCardinality());
        starTreeIndexSpec.setSkipMaterializationForDimensions(Sets.newHashSet(starTreeMetadata.getSkipMaterializationForDimensions()));
        starTreeIndexSpec.setSkipStarNodeCreationForDimensions(Sets.newHashSet(starTreeMetadata.getSkipStarNodeCreationForDimensions()));
        config.setStarTreeIndexSpec(starTreeIndexSpec );
      }

      LOGGER.info("Creating segment for {} with config {}", segmentName, config.toString());
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, new RecordReaderSegmentCreationDataSource(wrapperReader));
      driver.build();

      // upload segment
      LOGGER.info("Uploading segment {} to host: {} port: {}", segmentName, _controllerHost, _controllerPort);
      backfillSegmentUtils.uploadSegment(segmentName, new File(outputDir, segmentName), outputDir);
    }

    // verify that all segments exist
    List<String> missingSegments = new ArrayList<>();
    allSegmentNames = backfillSegmentUtils.getAllSegments(_tableName, _segmentType);
    for (String segmentName : segmentNames) {
      if (!allSegmentNames.contains(segmentName)) {
        missingSegments.add(segmentName);
      }
    }
    if (missingSegments.size() != 0) {
      LOGGER.error("Failed to backfill and upload segments {}", missingSegments);
      return false;
    }

    LOGGER.info("Original segment backup is at {}", tableBackupDir.getAbsolutePath());
    return true;
  }

}
