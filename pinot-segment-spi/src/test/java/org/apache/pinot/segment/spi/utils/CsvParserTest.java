/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.spi.utils;

import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CsvParserTest {
    @Test
    public void testEscapeTrueTrimFalse() {
        String input = " \\,.\n\t()[]{}\"':=-_$\\?@&|#+/,:=[]$@&|#";
        List<String> actualParsedOutput = CsvParser.parse(input, true, false);
        List<String> expectedParsedOutput = Arrays.asList(" ,.\n\t()[]{}\"':=-_$\\?@&|#+/", ":=[]$@&|#");
        Assert.assertEquals(actualParsedOutput, expectedParsedOutput);
        Assert.assertEquals(CsvParser.serialize(actualParsedOutput, true, false), input);
    }

    @Test
    public void testEscapeTrueTrimTrue() {
        String input = " \\,.\n\t()[]{}\"':=-_$\\?@&|#+/,:=[]$@&|#";
        List<String> expectedOutput = Arrays.asList(",.\n\t()[]{}\"':=-_$\\?@&|#+/", ":=[]$@&|#");
        Assert.assertEquals(CsvParser.parse(input, true, true), expectedOutput);
    }

    @Test
    public void testEscapeFalseTrimTrue() {
        String input = "abc\\,def.ghi, abc.def.ghi\n";
        List<String> expectedOutput = Arrays.asList("abc\\", "def.ghi", "abc.def.ghi");
        Assert.assertEquals(CsvParser.parse(input, false, true), expectedOutput);
    }
}
