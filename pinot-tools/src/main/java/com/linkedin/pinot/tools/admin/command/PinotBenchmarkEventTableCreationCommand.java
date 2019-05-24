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
import java.util.List;
import java.util.Random;

public class PinotBenchmarkEventTableCreationCommand extends AbstractBaseAdminCommand implements Command {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateSegmentCommand.class);
    //final String _timeIntervalConfig = "pinot_benchmark/event_data_config/time_intervals_100_days_of_2017_2018.properties";
    //final String _timeIntervalConfig = "pinot_benchmark/event_data_config/time_intervals_feb_2018.properties";

    final String _timeIntervalConfig = "pinot_benchmark/event_data_config/time_intervals_5_tables_120_days_of_2019.properties";

    final String _tableConfig = "pinot_benchmark/event_data_config/event_table_config.properties";
    private final int _minimumRecordCount = 2000;
    @Option(name = "-dataDir", required = false, metaVar = "<string>", usage = "Directory containing the data.")
    private String _dataDir;
    @Option(name = "-outDir", required = false, metaVar = "<string>", usage = "Name of output directory.")
    private String _outDir;
    @Option(name = "-overwrite", required = false, usage = "Overwrite existing output directory.")
    private boolean _overwrite = false;



   // private int[] _numRecordList={200000,210000,220000,230000,240000,250000,260000,270000,280000,290000,300000};
    private double[] _numRecordRatio={1,0.6,0.5,0.3,0.2};

    //private int[] _numRecordList={200000,210000,220000,230000,240000,250000,260000,270000,280000,290000,300000};
    @Option(name = "-numRecords", required = false, metaVar = "<int>", usage = "Number of records to generate.")
    private int _numRecords = 10000;
    private int[] _numRecordList = {80000, 90000, 100000, 110000, 120000, 130000, 140000, 150000};
    //private double[] _numRecordRatio = {1, 0.6, 0.5, 0.3};
    //private int[] _meanDocCount = {30000, 15000, 10000, 20000};
    //private int[] _standardDeviation = {1000, 4000, 2000, 3000};
    private int[] _meanDocCount = {30000, 10000, 3000, 15000,1000};
    private int[] _standardDeviation = {1000, 600, 200, 750,100};


    private int[] _varianceList = {5000, 10000, 15000, 20000,25000};
    private int _varianceListSize = 5;

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
        List <String> timeIntervals = FileUtils.readLines(new File(timeIntervalConfigPath));
        //BufferedReader br = new BufferedReader(new FileReader(trained_cost_file));
        Random rand = new Random(System.currentTimeMillis());
        int[] everyRoundRecordCount = new int[5];


	//for (int i = 1; i < 31; i++) { 
        for (int i = 1; i < timeIntervals.size(); i++) {
        //for (int i = 1; i < 2; i++) {
            String[] timeIntervalInfo = timeIntervals.get(i).split(",");
            String outDir = _outDir + "/" + timeIntervalInfo[0];
            File outDirFile = new File(outDir);
            outDirFile.mkdir();

            long timeIntervalStart = Long.parseLong(timeIntervalInfo[1]);
            long timeIntervalEnd = Long.parseLong(timeIntervalInfo[2]);


            for (int j = 0; j < 5; j++) {
                double gussianNumber = rand.nextGaussian();
                everyRoundRecordCount[j] = (int) (gussianNumber * _standardDeviation[j] + _meanDocCount[j]);

                if (everyRoundRecordCount[j] < 0) {
                    everyRoundRecordCount[j] = _minimumRecordCount;
                }
            }

            EventTableGenerator eventTableGenerator = new EventTableGenerator(_dataDir, outDir);
            eventTableGenerator.generateProfileViewTable(timeIntervalStart, timeIntervalEnd, everyRoundRecordCount[0]);
            //eventTableGenerator.generateAdClickTable(timeIntervalStart, timeIntervalEnd, everyRoundRecordCount[1]);
            eventTableGenerator.generateArticleReadTable(timeIntervalStart, timeIntervalEnd, everyRoundRecordCount[2]);
            eventTableGenerator.generateJobApplyTable(timeIntervalStart, timeIntervalEnd, everyRoundRecordCount[3]);
            //eventTableGenerator.generateCompanySearchTable(timeIntervalStart,timeIntervalEnd, everyRoundRecordCount[4]);
        }


        //createVaryigSizeProfileView();

        return true;
    }


    private void createVaryigSizeProfileView() throws  Exception{
        ClassLoader classLoader = PinotBenchmarkEventTableCreationCommand.class.getClassLoader();
        String timeIntervalConfigPath = EventTableGenerator.getFileFromResourceUrl(classLoader.getResource(_timeIntervalConfig));
        List <String> timeIntervals = FileUtils.readLines(new File(timeIntervalConfigPath));
        //BufferedReader br = new BufferedReader(new FileReader(trained_cost_file));
        Random rand = new Random(System.currentTimeMillis());

        for (int v = 0; v < _varianceListSize; v++)
        {
            LOGGER.info("Creating data for std deviation of: " + _varianceList[v]);
            String currOutDir = _outDir + "/events_for_stddev_" +  _varianceList[v];
            File outDirFile = new File(currOutDir);
            outDirFile.mkdir();

            for (int i = 1; i < timeIntervals.size(); i++) {
                String[] timeIntervalInfo = timeIntervals.get(i).split(",");
                String outDir = currOutDir + "/" + timeIntervalInfo[0];
                outDirFile = new File(outDir);
                outDirFile.mkdir();

                long timeIntervalStart = Long.parseLong(timeIntervalInfo[1]);
                long timeIntervalEnd = Long.parseLong(timeIntervalInfo[2]);

                double gussianNumber = rand.nextGaussian();
                int roundRecordCount = (int) (gussianNumber * _varianceList[v] + _meanDocCount[0]);

                if (roundRecordCount < 0)
                {
                    roundRecordCount = _minimumRecordCount;
                }

                LOGGER.info("NumRecord: " + roundRecordCount);
                EventTableGenerator eventTableGenerator = new EventTableGenerator(_dataDir, outDir);
                eventTableGenerator.generateProfileViewTable(timeIntervalStart, timeIntervalEnd, roundRecordCount);
            }
        }
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
