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

import com.linkedin.pinot.core.query.utils.Pair;
import com.linkedin.pinot.tools.Command;
import com.linkedin.pinot.tools.pacelab.benchmark.EventTableGenerator;
import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Random;

public class PinotBenchmarkEventTableCreationCommand extends AbstractBaseAdminCommand implements Command {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateSegmentCommand.class);
    @Option(name = "-dataDir", required = false, metaVar = "<string>", usage = "Directory containing the data.")
    private String _dataDir;

    @Option(name = "-outDir", required = false, metaVar = "<string>", usage = "Name of output directory.")
    private String _outDir;

    @Option(name = "-overwrite", required = false, usage = "Overwrite existing output directory.")
    private boolean _overwrite = false;

    @Option(name = "-numRecords", required = false, metaVar = "<int>", usage = "Number of records to generate.")
    private int _numRecords = 10000;


    final String _timeIntervalConfig = "pinot_benchmark/event_data_config/time_intervals.properties";
    final String _tableConfig = "pinot_benchmark/event_data_config/event_table_config.properties";


    private int[] _numRecordList={200000,210000,220000,230000,240000,250000,260000,270000,280000,290000,300000};
    private double[] _numRecordRatio={1,0.6,0.5,0.3};

    public PinotBenchmarkEventTableCreationCommand setDataDir(String dataDir) {
        _dataDir = dataDir;
        return this;
    }


    public PinotBenchmarkEventTableCreationCommand setOutDir(String outDir) {
        _outDir = outDir;
        return this;
    }


    public PinotBenchmarkEventTableCreationCommand setOverwrite(boolean overwrite) {
        _overwrite = overwrite;
        return this;
    }

    @Override
    public boolean execute() throws Exception {

        ClassLoader classLoader = PinotBenchmarkEventTableCreationCommand.class.getClassLoader();
        String timeIntervalConfigPath = EventTableGenerator.getFileFromResourceUrl(classLoader.getResource(_timeIntervalConfig));
        List<String> timeIntervals =  FileUtils.readLines(new File(timeIntervalConfigPath));
        //BufferedReader br = new BufferedReader(new FileReader(trained_cost_file));
        Random rand = new Random(System.currentTimeMillis());

        for(int i=1; i < timeIntervals.size();i++)
        {
            String[] timeIntervalInfo = timeIntervals.get(i).split(",");
            String outDir = _outDir + "/" +timeIntervalInfo[0];
            File outDirFile = new File(outDir);
            outDirFile.mkdir();

            long timeIntervalStart= Long.parseLong(timeIntervalInfo[1]);
            long timeIntervalEnd= Long.parseLong(timeIntervalInfo[2]);
            int index = rand.nextInt(_numRecordList.length);
            int numRecord = _numRecordList[index];
            EventTableGenerator eventTableGenerator = new EventTableGenerator(_dataDir,outDir, _numRecords);
            eventTableGenerator.generateProfileViewTable(timeIntervalStart,timeIntervalEnd, (int)(numRecord*_numRecordRatio[0]));
            eventTableGenerator.generateAdClickTable(timeIntervalStart,timeIntervalEnd,(int)(numRecord*_numRecordRatio[1]));
            eventTableGenerator.generateArticleReadTable(timeIntervalStart,timeIntervalEnd,(int)(numRecord*_numRecordRatio[2]));
            eventTableGenerator.generateJobApplyTable(timeIntervalStart,timeIntervalEnd,(int)(numRecord*_numRecordRatio[3]));

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
