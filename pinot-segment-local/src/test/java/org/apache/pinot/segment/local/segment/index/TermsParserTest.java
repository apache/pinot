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
package org.apache.pinot.segment.local.segment.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.text.TermsParser;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TermsParserTest {

  private List<String> getData() {
    return Arrays.asList("Prince Andrew kept looking with an amused smile from Pierre",
        "vicomte and from the vicomte to their hostess. In the first moment of",
        "Pierre’s outburst Anna Pávlovna, despite her social experience, was",
        "horror-struck. But when she saw that Pierre’s sacrilegious words",
        "had not exasperated the vicomte, and had convinced herself that it was",
        "impossible to stop him, she rallied her forces and joined the vicomte in", "a vigorous attack on the orator",
        "horror-struck. But when she", "she rallied her forces and joined", "outburst Anna Pávlovna",
        "she rallied her forces and", "despite her social experience", "had not exasperated the vicomte",
        " despite her social experience", "impossible to stop him", "despite her social experience");
  }

  @Test
  public void testParsing() {
    TermsParser termsParser =
        new TermsParser(new StandardAnalyzer(LuceneTextIndexCreator.ENGLISH_STOP_WORDS_SET), "testColumn");
    List<String> testData = getData();

    for (String text : testData) {
      try {
        Analyzer analyzer = new StandardAnalyzer(LuceneTextIndexCreator.ENGLISH_STOP_WORDS_SET);
        List<String> luceneResults = analyzeWithLuceneAnalyzer(text, analyzer);
        Iterable<String> resultData = termsParser.parse(text);

        for (String resultValue : resultData) {
          Assert.assertTrue(luceneResults.contains(resultValue));
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private List<String> analyzeWithLuceneAnalyzer(String text, Analyzer analyzer)
      throws IOException {
    List<String> result = new ArrayList<>();
    TokenStream tokenStream = analyzer.tokenStream("testColumn", text);
    CharTermAttribute attr = tokenStream.addAttribute(CharTermAttribute.class);
    tokenStream.reset();
    while (tokenStream.incrementToken()) {
      result.add(attr.toString());
    }
    return result;
  }
}
