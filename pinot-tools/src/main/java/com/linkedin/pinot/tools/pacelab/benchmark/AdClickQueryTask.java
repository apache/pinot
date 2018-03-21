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
package com.linkedin.pinot.tools.pacelab.benchmark;

import com.linkedin.pinot.core.data.GenericRow;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.math.LongRange;
import org.xerial.util.ZipfRandom;

import java.util.List;
import java.util.Properties;
import java.util.Random;

public class AdClickQueryTask extends QueryTask {
    List<GenericRow> _adTable;
    ZipfRandom _zipfRandom;
    final static int HourSecond = 3600;
    Random _adIndexGenerator;

    public AdClickQueryTask(Properties config, String[] queries, String dataDir, int testDuration) {
        setConfig(config);
        setQueries(queries);
        setDataDir(dataDir);
        setTestDuration(testDuration);
        EventTableGenerator eventTableGenerator = new EventTableGenerator(_dataDir);

        long minClickTime = Long.parseLong(config.getProperty("MinClickTime"));
        long maxClickTime = Long.parseLong(config.getProperty("MaxClickTime"));

        double zipfS = Double.parseDouble(config.getProperty("ZipfSParameter"));

        int hourCount = (int) Math.ceil((maxClickTime-minClickTime)/(HourSecond));
        _zipfRandom = new ZipfRandom(zipfS,hourCount);

        _adIndexGenerator =  new Random(System.currentTimeMillis());
        try
        {
            _adTable = eventTableGenerator.readAdTable();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

    }

    @Override
    public void run() {
        super.run();
    }

    public void generateAndRunQuery(int queryId) throws Exception {
        EventTableGenerator eventTableGenerator = new EventTableGenerator(_dataDir);
        Properties config = getConfig();
        String[] queries = getQueries();

        long minClickTime = Long.parseLong(config.getProperty("MinClickTime"));
        long maxClickTime = Long.parseLong(config.getProperty("MaxClickTime"));

        /*
        double zipfS = Double.parseDouble(config.getProperty("ZipfSParameter"));
        //LongRange timeRange = CommonTools.getZipfRandomDailyTimeRange(minClickTime,maxClickTime,zipfS);
        LongRange timeRange = CommonTools.getZipfRandomHourlyTimeRange(minClickTime,maxClickTime,zipfS);
        */

        int firstHour = _zipfRandom.nextInt();
        //int secondHour = _zipfRandom.nextInt();

        //long queriedEndTime = maxApplyStartTime - firstHour*HourSecond;
        long queriedEndTime = maxClickTime;
        long queriedStartTime = Math.max(minClickTime,queriedEndTime - firstHour*HourSecond);

        LongRange timeRange =  new LongRange(queriedStartTime,queriedEndTime);


        int selectLimit = CommonTools.getSelectLimt(config);
        int groupByLimit = Integer.parseInt(config.getProperty("GroupByLimit"));

        //List<GenericRow> profileTable = eventTableGenerator.readProfileTable();
        //GenericRow randomProfile = eventTableGenerator.getRandomGenericRow(profileTable);


        GenericRow randomAd = eventTableGenerator.getRandomGenericRow(_adTable,_adIndexGenerator);

        String query = "";
        switch (queryId) {
            case 0:
                query = String.format(queries[queryId], timeRange.getMaximumLong(), timeRange.getMaximumLong());
                runQuery(query);
                break;
            case 1:

                query = String.format(queries[queryId], timeRange.getMaximumLong(), timeRange.getMaximumLong(), randomAd.getValue("ID"));
                runQuery(query);
                break;
            case 2:
                query = String.format(queries[queryId], timeRange.getMaximumLong(), timeRange.getMaximumLong(), groupByLimit);
                runQuery(query);
                break;
            case 3:
                query = String.format(queries[queryId], timeRange.getMaximumLong(), timeRange.getMaximumLong(), groupByLimit);
                runQuery(query);
                break;
        }

    }

}
