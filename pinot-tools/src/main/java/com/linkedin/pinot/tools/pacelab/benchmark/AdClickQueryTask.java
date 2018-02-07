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

import java.util.List;
import java.util.Properties;

public class AdClickQueryTask extends QueryTask {
    List<GenericRow> _adTable;


    public AdClickQueryTask(Properties config, String[] queries, String dataDir) {
        setConfig(config);
        setQueries(queries);
        setDataDir(dataDir);
        EventTableGenerator eventTableGenerator = new EventTableGenerator(_dataDir);
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

        double zipfS = Double.parseDouble(config.getProperty("ZipfSParameter"));
        LongRange timeRange = CommonTools.getZipfRandomTimeRange(minClickTime,maxClickTime,zipfS);

        int selectLimit = CommonTools.getSelectLimt(config);
        int groupByLimit = Integer.parseInt(config.getProperty("GroupByLimit"));

        //List<GenericRow> profileTable = eventTableGenerator.readProfileTable();
        //GenericRow randomProfile = eventTableGenerator.getRandomGenericRow(profileTable);


        GenericRow randomAd = eventTableGenerator.getRandomGenericRow(_adTable);

        String query = "";
        switch (queryId) {
            case 0:
                query = String.format(queries[queryId], timeRange.getMaximumLong(), timeRange.getMaximumLong(), selectLimit);
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
