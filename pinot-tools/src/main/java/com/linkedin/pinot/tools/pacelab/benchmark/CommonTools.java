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

import org.apache.commons.lang.math.LongRange;
import org.xerial.util.ZipfRandom;

import java.util.Properties;
import java.util.Random;

public class CommonTools {
    final static int DaySecond = 3600*24;
    public static LongRange getZipfRandomTimeRange(long minProfileViewStartTime, long maxProfileViewStartTime, double zipfS)
    {
        int dayCount = (int)((maxProfileViewStartTime-minProfileViewStartTime)/(DaySecond));
        ZipfRandom zipfRandom = new ZipfRandom(zipfS,dayCount);

        int firstDay = zipfRandom.nextInt();
        int secondDay = zipfRandom.nextInt();
        while(secondDay == firstDay)
        {
            secondDay = zipfRandom.nextInt();
        }

        long queriedStartTime;
        long queriedEndTime;

        if(firstDay<secondDay)
        {
            queriedStartTime = minProfileViewStartTime + firstDay*DaySecond;
            queriedEndTime = minProfileViewStartTime + secondDay*DaySecond;
        }
        else
        {
            queriedStartTime = minProfileViewStartTime + secondDay*DaySecond;
            queriedEndTime = minProfileViewStartTime + firstDay*DaySecond;
        }
        return new LongRange(queriedStartTime,queriedEndTime);
    }
    public static int getSelectLimt(Properties config)
    {
        Random randGen = new Random(System.currentTimeMillis());
        int minSelectLimit = Integer.parseInt(config.getProperty("MinSelectLimit"));
        int maxSelectLimit = Integer.parseInt(config.getProperty("MaxSelectLimit"));
        int selectLimit = minSelectLimit + randGen.nextInt(maxSelectLimit - minSelectLimit);
        return  selectLimit;
    }
}
