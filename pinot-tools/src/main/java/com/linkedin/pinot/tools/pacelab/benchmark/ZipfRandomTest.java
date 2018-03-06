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

import org.xerial.util.ZipfRandom;

public class ZipfRandomTest {
    public static void main(String[] args)
    {
        ZipfRandom zipfRandom = new ZipfRandom(0.8, 100);
        int[] randomNumFreq = new int[101];
        for(int i=0;i<1000;i++)
        {
            randomNumFreq[zipfRandom.nextInt()]++;
        }

        for(int i=1; i<101; i++)
        {
            System.out.println(randomNumFreq[i]);
        }

    }
}
