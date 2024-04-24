package org.apache.pinot.segment.spi.utils;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

public class CsvParserTest {
    @Test
    public void testEscapeTrueTrimFalse() {
        String input = " \\,.\n\t()[]{}\"':=-_$\\?@&|#+/,:=[]$@&|#";
        List<String> expectedOutput = Arrays.asList(" ,.\n\t()[]{}\"':=-_$\\?@&|#+/", ":=[]$@&|#");
        Assert.assertEquals(CsvParser.parse(input, true, false), expectedOutput);
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
