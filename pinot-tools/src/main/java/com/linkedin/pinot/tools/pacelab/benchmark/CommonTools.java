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
    final static int HourSecond = 3600;

    public static LongRange getZipfRandomDailyTimeRange(long minProfileViewStartTime, long maxProfileViewStartTime, double zipfS)
    {


        //int dayCount = (int) Math.ceil((maxProfileViewStartTime-minProfileViewStartTime)/(DaySecond));

        int hourCount = (int) Math.ceil((maxProfileViewStartTime-minProfileViewStartTime)/(HourSecond));
        ZipfRandom zipfRandom = new ZipfRandom(zipfS,hourCount);

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

            queriedStartTime = maxProfileViewStartTime - secondDay*DaySecond;
            queriedEndTime = maxProfileViewStartTime - firstDay*DaySecond;
        }
        else
        {
            queriedStartTime = maxProfileViewStartTime - firstDay*DaySecond;
            queriedEndTime = maxProfileViewStartTime - secondDay*DaySecond;
        }
        return new LongRange(queriedStartTime,queriedEndTime);
    }

    public static LongRange getZipfRandomHourlyTimeRange(long minProfileViewStartTime, long maxProfileViewStartTime, double zipfS)
    {

        Random rand = new Random(System.currentTimeMillis());

        int hourCount = (int) Math.ceil((maxProfileViewStartTime-minProfileViewStartTime)/(HourSecond));
        ZipfRandom zipfRandom = new ZipfRandom(zipfS,hourCount);


        /*
        int firstHour = zipfRandom.nextInt();
        int secondHour = zipfRandom.nextInt();
        while(secondHour == firstHour)
        {
            secondHour = zipfRandom.nextInt();
        }

        long queriedStartTime;
        long queriedEndTime;

        if(firstHour<secondHour)
        {

            queriedStartTime = maxProfileViewStartTime - secondHour*HourSecond;
            queriedEndTime = maxProfileViewStartTime - firstHour*HourSecond;
        }
        else
        {
            queriedStartTime = maxProfileViewStartTime - firstHour*HourSecond;
            queriedEndTime = maxProfileViewStartTime - secondHour*HourSecond;
        }
        return new LongRange(queriedStartTime,queriedEndTime);
        */

        /*
        int firstHour = zipfRandom.nextInt();
        int secondHour = zipfRandom.nextInt();
        //Did not work properly
        long queriedStartTime;
        long queriedEndTime;

        queriedEndTime = maxProfileViewStartTime - firstHour*HourSecond;
        queriedStartTime = queriedEndTime - secondHour*HourSecond;
        */

        int firstHour = zipfRandom.nextInt();
        int meanHourBack = 7*24;
        int stdHourBack = 83 * 24;
        double gussianNumber = rand.nextGaussian();
        int hoursBack = Math.abs((int) (gussianNumber * stdHourBack + meanHourBack));
        long queriedEndTime = maxProfileViewStartTime - firstHour*HourSecond;
        long queriedStartTime = queriedEndTime - hoursBack*HourSecond;;

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
