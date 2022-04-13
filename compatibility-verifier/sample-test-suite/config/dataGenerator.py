#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import random
import csv
import string

'''Script used for one time random generation of test data FeatureTest3-data-realtime-00.csv, 
   do not use for adding new columns in existing dataset. Left this here for future reference. '''

class DataGenerator:

    def __init__(self):
        self.__NUMBER_OF_LINES = 300
        self.__RANGE_OF_LONG_IDX = 100
        self.__RANGE_OF_DOUBLE_IDX = 100
        self.__VAR_LEN_STRING_LOWER = 8
        self.__VAR_LEN_STRING_UPPER = 16
        return

    def generateLine(self, longDimSV2Prev):
        line = []

        # hours since epoch
        line.append("123456")

        # generationNumber
        line.append("__GENERATION_NUMBER__")

        # stringDimSV1 varlen,bloom
        letters = string.ascii_lowercase
        num_letters = random.randint(self.__VAR_LEN_STRING_LOWER, self.__VAR_LEN_STRING_UPPER)
        line.append(''.join(random.choice(letters) for _ in range(num_letters)))

        # stringDimSV2 noDict
        letters = string.ascii_lowercase
        num_letters = random.randint(self.__VAR_LEN_STRING_LOWER, self.__VAR_LEN_STRING_UPPER)
        line.append(''.join(random.choice(letters) for _ in range(num_letters)))

        # longDimSV1 sorted
        line.append(longDimSV2Prev[0])
        longDimSV2Prev[0] -= 1

        # doubleDimSV1 range
        line.append(random.randint(0, self.__RANGE_OF_DOUBLE_IDX * 100) / 100)

        # SV intMetric1 noDict
        line.append(random.randint(0, 10000))

        # SV longMetric1 noDict
        line.append(2**48+random.randint(0, 10000))

        # SV floatMetric1 noDict
        line.append(random.randint(0, 10000) / 100)

        # SV doubleMetric1 noDict
        line.append(3.4E45+random.randint(0, 100) / 10 * (10**45))

        # SV bytesMetric1 noDict
        line.append(random.choice(["deadbeef", "deed0507", "01a0bc", "d54d0507"]))

        return line

    def generateLines(self):
        ret = []
        longDimSV2 = [self.__NUMBER_OF_LINES]
        for _ in range(self.__NUMBER_OF_LINES):
            ret.append(self.generateLine(longDimSV2))

        return ret


if __name__ == '__main__':
    dataGenerator = DataGenerator()
    with open('FeatureTest3-data-realtime-00.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, quotechar='\'', delimiter=',')
        writer.writerows(dataGenerator.generateLines())
