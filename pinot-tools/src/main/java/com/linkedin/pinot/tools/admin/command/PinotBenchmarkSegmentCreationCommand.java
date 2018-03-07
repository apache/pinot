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

import com.linkedin.pinot.tools.Command;
import com.linkedin.pinot.tools.pacelab.benchmark.EventTableGenerator;
import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class PinotBenchmarkSegmentCreationCommand extends AbstractBaseAdminCommand implements Command {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateSegmentCommand.class);
    @Option(name = "-dataDir", required = false, metaVar = "<string>", usage = "Directory containing the event table data.")
    private String _dataDir;

    @Option(name = "-outDir", required = false, metaVar = "<string>", usage = "Name of output directory.")
    private String _outDir;

    @Option(name = "-overwrite", required = false, usage = "Overwrite existing output directory.")
    private boolean _overwrite = false;

    final String _timeIntervalConfig = "pinot_benchmark/event_data_config/time_intervals_100_days_of_2017_2018.properties";
    final String _tableNameFile = "pinot_benchmark/event_data_config/event_table_config.properties";



    private void createOutDir(String dirPath) throws Exception
    {
        // Make sure output directory does not already exist, or can be overwritten.
        File outDir = new File(dirPath);
        if (outDir.exists()) {
            if (!_overwrite) {
                throw new IOException("Output directory " + dirPath + " already exists.");
            } else {
                FileUtils.deleteDirectory(outDir);
            }
        }

        outDir.mkdir();
    }


    @Override
    public boolean execute() throws Exception {
        ClassLoader classLoader = PinotBenchmarkSegmentCreationCommand.class.getClassLoader();
        String tableNameFilePath = EventTableGenerator.getFileFromResourceUrl(classLoader.getResource(_tableNameFile));
        List<String> tablesInfo = FileUtils.readLines(new File(tableNameFilePath));

        String configFile = EventTableGenerator.getFileFromResourceUrl(classLoader.getResource(_timeIntervalConfig));
        List<String> configLines =  FileUtils.readLines(new File(configFile));


        for(int i=1;i<tablesInfo.size();i++)
        {
            String[] tableInfoRecord = tablesInfo.get(i).split(",");

            String tableSegmentsDir = _outDir+"/"+tableInfoRecord[0];
            createOutDir(tableSegmentsDir);

            for(int j=1;j<configLines.size();j++)
            {
                String[] evenDataInfo = configLines.get(j).split(",");

                String eventDataDir = _dataDir + "/" + evenDataInfo[0] + "/" + tableInfoRecord[0];
                String segmentDir = tableSegmentsDir + "/" + evenDataInfo[0];
                String schemaFilePath = EventTableGenerator.getFileFromResourceUrl(classLoader.getResource(tableInfoRecord[1]));

                CreateSegmentCommand createSegmentCommand = new CreateSegmentCommand();
                createSegmentCommand.setDataDir(eventDataDir);
                createSegmentCommand.setOutDir(segmentDir);
                createSegmentCommand.setTableName(tableInfoRecord[0]);
                createSegmentCommand.setSchemaFile(schemaFilePath);
                createSegmentCommand.setOverwrite(_overwrite);
                createSegmentCommand.setSegmentName(evenDataInfo[i+2]);
                createSegmentCommand.execute();
            }
        }

        return true;
    }

    @Override
    public String description() {
        return null;
    }

    @Override
    public boolean getHelp() {
        return false;
    }
}
