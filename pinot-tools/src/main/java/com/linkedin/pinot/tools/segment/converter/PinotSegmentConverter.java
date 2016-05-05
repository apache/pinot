/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.tools.segment.converter;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.tools.Command;
import com.linkedin.pinot.tools.admin.command.AbstractBaseAdminCommand;

/**
 * Tool to convert pinot segment into other formats such as AVRO, JSON, CSV
 */
public class PinotSegmentConverter extends AbstractBaseAdminCommand implements Command {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentConverter.class);

  @Option(name = "-segmentIndexDir", required = true, metaVar = "<String>", usage = "Path to segment index directroy")
  String segmentIndexDir;

  @Option(name = "-outputDir", required = true, metaVar = "<String>", usage = "Path for generation of output")
  String outputDir;

  @Option(name = "-fileFormat", required = true, metaVar = "<String>", usage = "AVRO/CSV/JSON")
  String fileFormat;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"},
      usage = "Print this message.")
  private boolean help = false;

  @Override
  public boolean execute() throws Exception {
    if (fileFormat.equals(FileFormat.AVRO.toString())) {
      PinotSegmentToAvroConverter pinotSegmentToAvroConverter = new PinotSegmentToAvroConverter(segmentIndexDir, outputDir);
      pinotSegmentToAvroConverter.convert();
    } else if (fileFormat.equals(FileFormat.CSV.toString())) {
      LOGGER.error("FileFormat CSV not currently supported");
    } else if (fileFormat.equals(FileFormat.JSON.toString())) {
      LOGGER.error("FileFormat JSON not currently supported");
    } else if (fileFormat.equals(FileFormat.GZIPPED_AVRO)){
      LOGGER.error("FileFormat GZIPPED_AVRO not currently supported");
    } else {
      LOGGER.error("Unknown FileFormat {} must be one of {}", fileFormat, FileFormat.values());
    }
    return false;
  }

  @Override
  public String description() {
    return "Converting pinot segment to another format such as AVRO/CSV/JSON";
  }

  @Override
  public boolean getHelp() {
    return help;
  }

  public static void main(String[] args)
      throws Exception {
    PinotSegmentConverter converter = new PinotSegmentConverter();
    CmdLineParser cmdLineParser = new CmdLineParser(converter);
    try {
      cmdLineParser.parseArgument(args);
    } catch (CmdLineException e) {
      LOGGER.error("Failed to read command line arguments: ", e);
      cmdLineParser.printUsage(System.err);
      System.exit(1);
    }
    converter.execute();
  }

}
