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
package org.apache.pinot.common.function.scalar;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;


public class UrlFunctionsTest {

  @Test
  public void testUrlProtocol() {
    //test cases for http, https, ftp, mailto, tel, magnet.
    assertEquals(UrlFunctions.urlProtocol("http://example.com"), "http");
    assertEquals(UrlFunctions.urlProtocol("https://example.com"), "https");
    assertEquals(UrlFunctions.urlProtocol("ftp://example.com"), "ftp");
    assertEquals(UrlFunctions.urlProtocol("mailto:name@email.com"), "mailto");
    assertEquals(UrlFunctions.urlProtocol("tel:1234567890"), "tel");
    assertEquals(UrlFunctions.urlProtocol("magnet:?xt=urn:btih:1234567890"), "magnet");
    //test case for invalid url
    assertNull(UrlFunctions.urlProtocol("invalid_url"));
  }

  @Test
  public void testUrlDomain() {
    assertEquals(UrlFunctions.urlDomain("https://example.com"), "example.com");
    assertEquals(UrlFunctions.urlDomain("http://example.com"), "example.com");
    assertEquals(UrlFunctions.urlDomain("https://sub.example.com"), "sub.example.com");
    assertEquals(UrlFunctions.urlDomain("https://example.co.uk"), "example.co.uk");
    assertNull(UrlFunctions.urlDomain("invalid_url"));
    assertNull(UrlFunctions.urlDomain("http://"));
    assertNull(UrlFunctions.urlDomain("https://"));
    assertNull(UrlFunctions.urlDomain(""));
    assertNull(UrlFunctions.urlDomain(null));
  }

  @Test
  public void testUrlDomainWithoutWWW() {
    assertEquals(UrlFunctions.urlDomainWithoutWWW("https://www.example.com"), "example.com");
    assertEquals(UrlFunctions.urlDomainWithoutWWW("https://example.com"), "example.com");
    assertEquals(UrlFunctions.urlDomainWithoutWWW("https://sub.example.com"), "sub.example.com");
    assertEquals(UrlFunctions.urlDomainWithoutWWW("https://www.sub.example.com"), "sub.example.com");
    assertEquals(UrlFunctions.urlDomainWithoutWWW("https://example.co.uk"), "example.co.uk");
    assertEquals(UrlFunctions.urlDomainWithoutWWW("https://www.example.co.uk"), "example.co.uk");
    assertNull(UrlFunctions.urlDomainWithoutWWW("invalid_url"));
    assertNull(UrlFunctions.urlDomainWithoutWWW("http://"));
    assertNull(UrlFunctions.urlDomainWithoutWWW("https://"));
    assertNull(UrlFunctions.urlDomainWithoutWWW(""));
    assertNull(UrlFunctions.urlDomainWithoutWWW(null));
  }

  @Test
  public void testTopLevelUrlDomain() {
    assertEquals(UrlFunctions.urlTopLevelDomain("https://example.com"), "com");
    assertEquals(UrlFunctions.urlTopLevelDomain("http://example.org"), "org");
    assertEquals(UrlFunctions.urlTopLevelDomain("https://example.co.uk"), "uk");
    assertEquals(UrlFunctions.urlTopLevelDomain("https://sub.example.com"), "com");
    assertEquals(UrlFunctions.urlTopLevelDomain("https://example.travel"), "travel");
    assertNull(UrlFunctions.urlTopLevelDomain("invalid_url"));
    assertNull(UrlFunctions.urlTopLevelDomain("http://"));
    assertNull(UrlFunctions.urlTopLevelDomain("https://"));
    assertNull(UrlFunctions.urlTopLevelDomain(""));
    assertNull(UrlFunctions.urlTopLevelDomain(null));
  }

  @Test
  public void testUrlFirstSignificantSubdomain() {
    assertEquals(UrlFunctions.urlFirstSignificantSubdomain("https://news.example.com"), "example");
    assertEquals(UrlFunctions.urlFirstSignificantSubdomain("https://example.com"), "example");
    assertEquals(UrlFunctions.urlFirstSignificantSubdomain("https://sub.example.co.uk"), "example");
    assertEquals(UrlFunctions.urlFirstSignificantSubdomain("https://www.sub.example.com"), "example");
    assertEquals(UrlFunctions.urlFirstSignificantSubdomain("https://example.travel"), "example");
    assertNull(UrlFunctions.urlFirstSignificantSubdomain("invalid_url"));
    assertNull(UrlFunctions.urlFirstSignificantSubdomain("http://"));
    assertNull(UrlFunctions.urlFirstSignificantSubdomain("https://"));
    assertNull(UrlFunctions.urlFirstSignificantSubdomain(""));
    assertNull(UrlFunctions.urlFirstSignificantSubdomain(null));
  }

  @Test
  public void testCutToFirstSignificantSubdomain() {
    assertEquals(UrlFunctions.cutToFirstSignificantSubdomain("https://news.example.com"), "example.com");
    assertEquals(UrlFunctions.cutToFirstSignificantSubdomain("https://news.example.com.cn"), "example.com.cn");
    assertEquals(UrlFunctions.cutToFirstSignificantSubdomain("https://example.com"), "example.com");
    assertEquals(UrlFunctions.cutToFirstSignificantSubdomain("https://sub.example.co.uk"), "example.co.uk");
    assertEquals(UrlFunctions.cutToFirstSignificantSubdomain("https://www.sub.example.com"), "example.com");
    assertEquals(UrlFunctions.cutToFirstSignificantSubdomain("https://example.travel"), "example.travel");
    assertNull(UrlFunctions.cutToFirstSignificantSubdomain("www.cn"));
    assertNull(UrlFunctions.cutToFirstSignificantSubdomain("cn"));
    assertNull(UrlFunctions.cutToFirstSignificantSubdomain("invalid_url"));
    assertNull(UrlFunctions.cutToFirstSignificantSubdomain("http://"));
    assertNull(UrlFunctions.cutToFirstSignificantSubdomain("https://"));
    assertNull(UrlFunctions.cutToFirstSignificantSubdomain(""));
    assertNull(UrlFunctions.cutToFirstSignificantSubdomain(null));
  }

  @Test
  public void testCutToFirstSignificantSubdomainWithWWW() {
    assertEquals(UrlFunctions.cutToFirstSignificantSubdomainWithWWW("https://news.example.com"), "example.com");
    assertEquals(UrlFunctions.cutToFirstSignificantSubdomainWithWWW("https://example.com"), "example.com");
    assertEquals(UrlFunctions.cutToFirstSignificantSubdomainWithWWW("https://www.example.com"), "www.example.com");
    assertEquals(UrlFunctions.cutToFirstSignificantSubdomainWithWWW("https://sub.example.co.uk"), "example.co.uk");
    assertEquals(UrlFunctions.cutToFirstSignificantSubdomainWithWWW("https://www.example.co.uk"), "www.example.co.uk");
    assertEquals(UrlFunctions.cutToFirstSignificantSubdomainWithWWW("https://www.sub.example.com"),
        "example.com");
    assertEquals(UrlFunctions.cutToFirstSignificantSubdomainWithWWW("https://example.travel"), "example.travel");
    assertNull(UrlFunctions.cutToFirstSignificantSubdomainWithWWW("invalid_url"));
    assertNull(UrlFunctions.cutToFirstSignificantSubdomainWithWWW("http://"));
    assertNull(UrlFunctions.cutToFirstSignificantSubdomainWithWWW("https://"));
    assertNull(UrlFunctions.cutToFirstSignificantSubdomainWithWWW(""));
    assertNull(UrlFunctions.cutToFirstSignificantSubdomainWithWWW(null));
  }

  @Test
  public void testUrlPort() {
    assertEquals(UrlFunctions.urlPort("https://example.com:8080"), 8080);
    assertEquals(UrlFunctions.urlPort("http://example.com:80"), 80);
    assertEquals(UrlFunctions.urlPort("https://example.com"), -1);
    assertEquals(UrlFunctions.urlPort("http://example.com"), -1);
    assertEquals(UrlFunctions.urlPort("https://example.com:invalid"), -1);
    assertEquals(UrlFunctions.urlPort("invalid_url"), -1);
    assertEquals(UrlFunctions.urlPort("http://"), -1);
    assertEquals(UrlFunctions.urlPort("https://"), -1);
    assertEquals(UrlFunctions.urlPort(""), -1);
    assertEquals(UrlFunctions.urlPort(null), -1);
  }

  @Test
  public void testUrlPath() {
    assertEquals(UrlFunctions.urlPath("https://example.com/path"), "/path");
    assertEquals(UrlFunctions.urlPath("https://example.com/path/to/resource"), "/path/to/resource");
    assertEquals(UrlFunctions.urlPath("https://example.com/"), "/");
    assertEquals(UrlFunctions.urlPath("https://example.com"), "");
    assertEquals(UrlFunctions.urlPath("https://example.com/path/to/resource?query=param"), "/path/to/resource");
    assertEquals(UrlFunctions.urlPath("https://example.com/path/to/resource#fragment"), "/path/to/resource");
    assertNull(UrlFunctions.urlPath("invalid_url"));
    assertNull(UrlFunctions.urlPath("http://"));
    assertNull(UrlFunctions.urlPath("https://"));
    assertNull(UrlFunctions.urlPath(""));
    assertNull(UrlFunctions.urlPath(null));
  }

  @Test
  public void testUrlPathWithQuery() {
    assertEquals(UrlFunctions.urlPathWithQuery("https://example.com/path?query=value"), "/path");
    assertEquals(UrlFunctions.urlPathWithQuery("https://example.com/?query=value"), "/");
    assertEquals(UrlFunctions.urlPathWithQuery("https://example.com/path/to/resource?query=param"),
        "/path/to/resource");
    assertEquals(UrlFunctions.urlPathWithQuery("https://example.com/path/to/resource#fragment"),
        "/path/to/resource");
    assertNull(UrlFunctions.urlPathWithQuery("invalid_url"));
    assertNull(UrlFunctions.urlPathWithQuery("http://"));
    assertNull(UrlFunctions.urlPathWithQuery("https://"));
    assertNull(UrlFunctions.urlPathWithQuery(""));
    assertNull(UrlFunctions.urlPathWithQuery(null));
  }

  @Test
  public void testUrlQueryString() {
    assertEquals(UrlFunctions.urlQueryString("https://example.com/path?query=value"), "query=value");
    assertEquals(UrlFunctions.urlQueryString("https://example.com/path?param1=value1&param2=value2"),
        "param1=value1&param2=value2");
    assertEquals(UrlFunctions.urlQueryString("https://example.com/path?param=value#fragment"), "param=value");
    assertNull(UrlFunctions.urlQueryString("https://example.com/path"));
    assertNull(UrlFunctions.urlQueryString("invalid_url"));
    assertNull(UrlFunctions.urlQueryString("http://"));
    assertNull(UrlFunctions.urlQueryString("https://"));
    assertNull(UrlFunctions.urlQueryString(""));
    assertNull(UrlFunctions.urlQueryString(null));
  }

  @Test
  public void testUrlFragment() {
    assertEquals(UrlFunctions.urlFragment("https://example.com/path#fragment"), "fragment");
    assertEquals(UrlFunctions.urlFragment("https://example.com/path/to/resource#section"), "section");
    assertEquals(UrlFunctions.urlFragment("https://example.com/#top"), "top");
    assertNull(UrlFunctions.urlFragment("https://example.com/path"));
    assertNull(UrlFunctions.urlFragment("invalid_url"));
    assertNull(UrlFunctions.urlFragment("http://"));
    assertNull(UrlFunctions.urlFragment("https://"));
    assertNull(UrlFunctions.urlFragment(""));
    assertNull(UrlFunctions.urlFragment(null));
  }

  @Test
  public void testUrlQueryStringAndFragment() {
    assertEquals(UrlFunctions.urlQueryStringAndFragment("https://example.com/path?query=value#fragment"),
        "query=value#fragment");
    assertEquals(UrlFunctions.urlQueryStringAndFragment("https://example.com/path?param1=value1&param2=value2#section"),
        "param1=value1&param2=value2#section");
    assertEquals(UrlFunctions.urlQueryStringAndFragment("https://example.com/path?param=value"), "param=value");
    assertEquals(UrlFunctions.urlQueryStringAndFragment("https://example.com/path#fragment"), "fragment");
    assertNull(UrlFunctions.urlQueryStringAndFragment("https://example.com/path"));
    assertNull(UrlFunctions.urlQueryStringAndFragment("invalid_url"));
    assertNull(UrlFunctions.urlQueryStringAndFragment("http://"));
    assertNull(UrlFunctions.urlQueryStringAndFragment("https://"));
    assertNull(UrlFunctions.urlQueryStringAndFragment(""));
    assertNull(UrlFunctions.urlQueryStringAndFragment(null));
  }

  @Test
  public void testExtractURLParameter() {
    assertEquals(UrlFunctions.extractURLParameter("https://example.com/path?param=value", "param"), "value");
    assertEquals(UrlFunctions.extractURLParameter("https://example.com/path?param1=value1&param2=value2", "param2"),
        "value2");
    assertEquals(UrlFunctions.extractURLParameter("https://example.com/path?param=value#fragment", "param"), "value");
    assertEquals(UrlFunctions.extractURLParameter("https://example.com/path?param=value&param=", "param"), "value");
    assertEquals(UrlFunctions.extractURLParameter("https://example.com/path?param=value&param2=", "param2"), "");
    assertEquals(UrlFunctions.extractURLParameter("https://example.com/path?param=value&param2", "param2"), "");
    assertEquals(UrlFunctions.extractURLParameter("https://example.com/path?param=value&param2", "param3"), "");
    assertEquals(UrlFunctions.extractURLParameter("https://example.com/path", "param"), "");
    assertEquals(UrlFunctions.extractURLParameter("https://example.com/path?", "param"), "");
    assertEquals(UrlFunctions.extractURLParameter("https://example.com/path?param", "param"), "");
    assertEquals(UrlFunctions.extractURLParameter("https://example.com/path?param=", "param"), "");
    assertEquals(UrlFunctions.extractURLParameter("https://example.com/path?param=value&param2=value2", "param2"),
        "value2");
    assertEquals(UrlFunctions.extractURLParameter("https://example.com/path?param=value&param2=value2", "param"),
        "value");
    assertEquals(UrlFunctions.extractURLParameter("https://example.com/path?param=value&param2=value2", "param3"), "");
    assertEquals(UrlFunctions.extractURLParameter("https://example.com/path?param=value&param2=value2", ""), "");
    assertEquals(UrlFunctions.extractURLParameter("https://example.com/path?param=value&param2=value2", null), "");
  }

  @Test
  public void testExtractURLParameters() {
    String[] expected = {"param1=value1", "param2=value2"};
    assertArrayEquals(expected,
        UrlFunctions.extractURLParameters("https://example.com/path?param1=value1&param2=value2"));
    assertArrayEquals(new String[0], UrlFunctions.extractURLParameters("https://example.com/path"));
    assertArrayEquals(new String[0], UrlFunctions.extractURLParameters("invalid_url"));
    assertArrayEquals(new String[0], UrlFunctions.extractURLParameters("http://"));
    assertArrayEquals(new String[0], UrlFunctions.extractURLParameters("https://"));
    assertArrayEquals(new String[0], UrlFunctions.extractURLParameters(""));
    assertArrayEquals(new String[0], UrlFunctions.extractURLParameters(null));
  }

  @Test
  public void testExtractURLParameterNames() {
    String[] expected = {"param1", "param2"};
    assertArrayEquals(expected,
        UrlFunctions.extractURLParameterNames("https://example.com/path?param1=value1&param2=value2"));
    assertArrayEquals(new String[0], UrlFunctions.extractURLParameterNames("https://example.com/path"));
  }

  @Test
  public void testUrlHierarchy() {
    assertArrayEquals(new String[]{"https://example.com", "https://example.com/path", "https://example.com/path/to"},
        UrlFunctions.urlHierarchy("https://example.com/path/to"));

    assertArrayEquals(new String[]{"https://example.com"}, UrlFunctions.urlHierarchy("https://example.com"));

    assertArrayEquals(new String[]{"https://example.com", "https://example.com/path"},
        UrlFunctions.urlHierarchy("https://example.com/path"));

    assertArrayEquals(new String[0], UrlFunctions.urlHierarchy("invalid_url"));
    assertArrayEquals(new String[0], UrlFunctions.urlHierarchy("http://"));
    assertArrayEquals(new String[0], UrlFunctions.urlHierarchy("https://"));
    assertArrayEquals(new String[0], UrlFunctions.urlHierarchy(""));
    assertArrayEquals(new String[0], UrlFunctions.urlHierarchy(null));
  }

  @Test
  public void testUrlPathHierarchy() {
    assertArrayEquals(new String[]{"/path", "/path/to"},
        UrlFunctions.urlPathHierarchy("https://example.com/path/to"));
    assertArrayEquals(new String[0], UrlFunctions.urlPathHierarchy("https://example.com/"));
    assertArrayEquals(new String[0], UrlFunctions.urlPathHierarchy("https://example.com"));
    assertArrayEquals(new String[0], UrlFunctions.urlPathHierarchy("invalid_url"));
    assertArrayEquals(new String[0], UrlFunctions.urlPathHierarchy(null));
    assertArrayEquals(new String[]{"/path", "/path/to", "/path/to/resource"},
        UrlFunctions.urlPathHierarchy("https://example.com/path/to/resource"));
  }

  @Test
  public void testUrlEncode() {
    assertEquals(UrlFunctions.urlEncode("https://example.com/path to resource"),
        "https%3A%2F%2Fexample.com%2Fpath+to+resource");
    assertEquals(UrlFunctions.urlEncode("https://example.com/path/to/resource"),
        "https%3A%2F%2Fexample.com%2Fpath%2Fto%2Fresource");
    assertEquals(UrlFunctions.urlEncode("https://example.com/path?query=value"),
        "https%3A%2F%2Fexample.com%2Fpath%3Fquery%3Dvalue");
    assertEquals(UrlFunctions.urlEncode("https://example.com/path#fragment"),
        "https%3A%2F%2Fexample.com%2Fpath%23fragment");
    assertEquals(UrlFunctions.urlEncode("https://example.com/path?query=value#fragment"),
        "https%3A%2F%2Fexample.com%2Fpath%3Fquery%3Dvalue%23fragment");

    assertEquals(UrlFunctions.urlEncode("invalid_url"), "invalid_url");
    assertEquals(UrlFunctions.urlEncode(""), "");
    assertNull(UrlFunctions.urlEncode(null));
  }

  @Test
  public void testUrlDecode() {
    assertEquals(UrlFunctions.urlDecode("https%3A%2F%2Fexample.com%2Fpath%20to%20resource"),
        "https://example.com/path to resource");
    assertEquals(UrlFunctions.urlDecode("https%3A%2F%2Fexample.com%2Fpath%2Fto%2Fresource"),
        "https://example.com/path/to/resource");
    assertEquals(UrlFunctions.urlDecode("https%3A%2F%2Fexample.com%2Fpath%3Fquery%3Dvalue"),
        "https://example.com/path?query=value");
    assertEquals(UrlFunctions.urlDecode("https%3A%2F%2Fexample.com%2Fpath%23fragment"),
        "https://example.com/path#fragment");
    assertEquals(UrlFunctions.urlDecode("https%3A%2F%2Fexample.com%2Fpath%3Fquery%3Dvalue%23fragment"),
        "https://example.com/path?query=value#fragment");
    assertEquals(UrlFunctions.urlDecode("random_path"), "random_path");
    assertEquals(UrlFunctions.urlDecode(""), "");
    assertNull(UrlFunctions.urlDecode(null));
  }

  @Test
  public void testUrlEncodeFormComponent() {
    assertEquals(UrlFunctions.urlEncodeFormComponent("https://example.com/path to resource"),
        "https%3A%2F%2Fexample.com%2Fpath+to+resource");
    assertEquals(UrlFunctions.urlEncodeFormComponent("https://example.com/path/to/resource"),
        "https%3A%2F%2Fexample.com%2Fpath%2Fto%2Fresource");
    assertEquals(UrlFunctions.urlEncodeFormComponent("https://example.com/path?query=value"),
        "https%3A%2F%2Fexample.com%2Fpath%3Fquery%3Dvalue");
    assertEquals(UrlFunctions.urlEncodeFormComponent("https://example.com/path#fragment"),
        "https%3A%2F%2Fexample.com%2Fpath%23fragment");
    assertEquals(UrlFunctions.urlEncodeFormComponent("https://example.com/path?query=value#fragment"),
        "https%3A%2F%2Fexample.com%2Fpath%3Fquery%3Dvalue%23fragment");
    assertNull(UrlFunctions.urlEncodeFormComponent(null));
    assertEquals(UrlFunctions.urlEncodeFormComponent(""), "");
  }

  @Test
  public void testUrlDecodeFormComponent() {
    assertEquals(UrlFunctions.urlDecodeFormComponent("https%3A%2F%2Fexample.com%2Fpath+to+resource"),
        "https://example.com/path to resource");
    assertEquals(UrlFunctions.urlDecodeFormComponent("https%3A%2F%2Fexample.com%2Fpath%2Fto%2Fresource"),
        "https://example.com/path/to/resource");
    assertEquals(UrlFunctions.urlDecodeFormComponent("https%3A%2F%2Fexample.com%2Fpath%3Fquery%3Dvalue"),
        "https://example.com/path?query=value");
    assertEquals(UrlFunctions.urlDecodeFormComponent("https%3A%2F%2Fexample.com%2Fpath%23fragment"),
        "https://example.com/path#fragment");
    assertEquals(UrlFunctions.urlDecodeFormComponent("https%3A%2F%2Fexample.com%2Fpath%3Fquery%3Dvalue%23fragment"),
        "https://example.com/path?query=value#fragment");
    assertNull(UrlFunctions.urlDecodeFormComponent(null));
    assertEquals(UrlFunctions.urlDecodeFormComponent(""), "");
  }

  @Test
  public void testUrlNetloc() {
    assertEquals(UrlFunctions.urlNetloc("https://user@example.com:8080/path"), "user@example.com:8080");
    assertEquals(UrlFunctions.urlNetloc("https://user:pass@example.com:8080/path"), "user:pass@example.com:8080");
    assertEquals(UrlFunctions.urlNetloc("https://example.com:8080/path"), "example.com:8080");
    assertEquals(UrlFunctions.urlNetloc("https://example.com/path"), "example.com");
    assertEquals(UrlFunctions.urlNetloc("https://user@example.com/path"), "user@example.com");
    assertEquals(UrlFunctions.urlNetloc("https://example.com"), "example.com");
    assertEquals(UrlFunctions.urlNetloc("random"), "");
    assertEquals(UrlFunctions.urlNetloc(""), "");
    assertNull(UrlFunctions.urlNetloc("http://"));
    assertNull(UrlFunctions.urlNetloc("https://"));
    assertNull(UrlFunctions.urlNetloc(null));
  }

  @Test
  public void testCutWWW() {
    assertEquals(UrlFunctions.cutWWW("https://www.example.com"), "https://example.com");
    assertEquals(UrlFunctions.cutWWW("http://www.example.com"), "http://example.com");
    assertEquals(UrlFunctions.cutWWW("https://www.sub.example.com"), "https://sub.example.com");
    assertEquals(UrlFunctions.cutWWW("http://www.sub.example.com"), "http://sub.example.com");

    assertEquals(UrlFunctions.cutWWW("https://example.com"), "https://example.com");
    assertEquals(UrlFunctions.cutWWW("http://example.com"), "http://example.com");
    assertEquals(UrlFunctions.cutWWW("https://sub.example.com"), "https://sub.example.com");
    assertEquals(UrlFunctions.cutWWW("http://sub.example.com"), "http://sub.example.com");

    assertEquals(UrlFunctions.cutWWW("invalid_url"), "invalid_url");
    assertEquals(UrlFunctions.cutWWW("http://"), "http://");
    assertEquals(UrlFunctions.cutWWW("https://"), "https://");
    assertEquals(UrlFunctions.cutWWW(""), "");
    assertNull(UrlFunctions.cutWWW(null));
  }

  @Test
  public void testCutQueryString() {
    assertEquals(UrlFunctions.cutQueryString("https://example.com/path?query=value"), "https://example.com/path");
    assertEquals(UrlFunctions.cutQueryString("https://example.com/path?param1=value1&param2=value2"),
        "https://example.com/path");
    assertEquals(UrlFunctions.cutQueryString("https://example.com/path?param=value#fragment"),
        "https://example.com/path#fragment");

    assertEquals(UrlFunctions.cutQueryString("https://example.com/path"), "https://example.com/path");
    assertEquals(UrlFunctions.cutQueryString("https://example.com/path#fragment"), "https://example.com/path#fragment");
    assertEquals(UrlFunctions.cutQueryString("https://example.com"), "https://example.com");

    assertEquals(UrlFunctions.cutQueryString("invalid_url"), "invalid_url");
    assertEquals(UrlFunctions.cutQueryString("http://"), "http://");
    assertEquals(UrlFunctions.cutQueryString("https://"), "https://");
    assertEquals(UrlFunctions.cutQueryString(""), "");
    assertNull(UrlFunctions.cutQueryString(null));
  }

  @Test
  public void testCutFragment() {
    assertEquals(UrlFunctions.cutFragment("https://example.com/path#fragment"), "https://example.com/path");
    assertEquals(UrlFunctions.cutFragment("https://example.com/path/to/resource#section"),
        "https://example.com/path/to/resource");
    assertEquals(UrlFunctions.cutFragment("https://example.com/#top"), "https://example.com/");
    assertEquals(UrlFunctions.cutFragment("https://example.com/path"), "https://example.com/path");
    assertEquals(UrlFunctions.cutFragment("https://example.com"), "https://example.com");
    assertEquals(UrlFunctions.cutFragment("invalid_url"), "invalid_url");
    assertEquals(UrlFunctions.cutFragment("http://"), "http://");
    assertEquals(UrlFunctions.cutFragment("https://"), "https://");
    assertEquals(UrlFunctions.cutFragment(""), "");
    assertNull(UrlFunctions.cutFragment(null));
  }

  @Test
  public void testCutQueryStringAndFragment() {
    assertEquals(UrlFunctions.cutQueryStringAndFragment("https://example.com/path?query=value#fragment"),
        "https://example.com/path");
    assertEquals(UrlFunctions.cutQueryStringAndFragment("https://example.com/path?param1=value1&param2=value2#section"),
        "https://example.com/path");
    assertEquals(UrlFunctions.cutQueryStringAndFragment("https://example.com/path?param=value"),
        "https://example.com/path");
    assertEquals(UrlFunctions.cutQueryStringAndFragment("https://example.com/path#fragment"),
        "https://example.com/path");

    assertEquals(UrlFunctions.cutQueryStringAndFragment("https://example.com/path"), "https://example.com/path");
    assertEquals(UrlFunctions.cutQueryStringAndFragment("https://example.com"), "https://example.com");

    assertEquals(UrlFunctions.cutQueryStringAndFragment("invalid_url"), "invalid_url");
    assertEquals(UrlFunctions.cutQueryStringAndFragment("http://"), "http://");
    assertEquals(UrlFunctions.cutQueryStringAndFragment("https://"), "https://");
    assertEquals(UrlFunctions.cutQueryStringAndFragment(""), "");
    assertNull(UrlFunctions.cutQueryStringAndFragment(null));
  }

  @Test
  public void testCutURLParameter() {
    assertEquals(
        UrlFunctions.cutURLParameter("https://example.com/path?param1=value1&param2=value2", "param1"),
        "https://example.com/path?param2=value2");
    assertEquals(
        UrlFunctions.cutURLParameter("https://example.com/path?param1=value1&param2=value2", "param2"),
        "https://example.com/path?param1=value1");
    assertEquals(UrlFunctions.cutURLParameter("https://example.com/path?param=value", "param"),
        "https://example.com/path");
    assertEquals(UrlFunctions.cutURLParameter("https://example.com/path?param1=value1&param2=value2&param3=value3",
        "param2"), "https://example.com/path?param1=value1&param3=value3");
    assertEquals(
        UrlFunctions.cutURLParameter("https://example.com/path?param1=value1&param2=value2&param3=value3#fragment",
            "param3"), "https://example.com/path?param1=value1&param2=value2#fragment");
    assertEquals(UrlFunctions.cutURLParameter(
            "https://example.com/path?param1=value1&param2=value2&param4&param3=value3#fragment", "param4"),
        "https://example.com/path?param1=value1&param2=value2&param3=value3#fragment");

    assertEquals(UrlFunctions.cutURLParameter("https://example.com/path", "param"),
        "https://example.com/path");
    assertEquals(UrlFunctions.cutURLParameter("https://example.com", "param"), "https://example.com");

    assertEquals(UrlFunctions.cutURLParameter("invalid_url", "param"), "invalid_url");
    assertEquals(UrlFunctions.cutURLParameter("http://", "param"), "http://");
    assertEquals(UrlFunctions.cutURLParameter("https://", "param"), "https://");
    assertEquals(UrlFunctions.cutURLParameter("", "param"), "");
    assertNull(UrlFunctions.cutURLParameter(null, "param"));

    assertEquals(UrlFunctions.cutURLParameter("https://example.com/path?param=value", ""),
        "https://example.com/path?param=value");
    assertEquals(UrlFunctions.cutURLParameter("https://example.com/path", ""),
        "https://example.com/path");

    assertEquals(UrlFunctions.cutURLParameter("https://example.com/path?param=value", null),
        "https://example.com/path?param=value");
    assertEquals(UrlFunctions.cutURLParameter("https://example.com/path", null), "https://example.com/path");
  }

  @Test
  public void testCutURLParameters() {
    assertEquals(
        UrlFunctions.cutURLParameters("https://example.com/path?param1=value1&param2=value2", new String[]{"param1"}),
        "https://example.com/path?param2=value2");
    assertEquals(
        UrlFunctions.cutURLParameters("https://example.com/path?param1=value1&param2=value2", new String[]{"param2"}),
        "https://example.com/path?param1=value1");
    assertEquals(UrlFunctions.cutURLParameters("https://example.com/path?param=value", new String[]{"param"}),
        "https://example.com/path");
    assertEquals(UrlFunctions.cutURLParameters("https://example.com/path?param1=value1&param2=value2&param3=value3",
            new String[]{"param2", "param3"}),
        "https://example.com/path?param1=value1");
    assertEquals(
        UrlFunctions.cutURLParameters("https://example.com/path?param1=value1&param2=value2&param3=value3#fragment",
            new String[]{"param2", "param3"}),
        "https://example.com/path?param1=value1#fragment");
    assertEquals(
        UrlFunctions.cutURLParameters(
            "https://example.com/path?param1=value1&param2=value2&param4&param3=value3#fragment",
            new String[]{"param2", "param3"}),
        "https://example.com/path?param1=value1&param4#fragment");

    assertEquals(UrlFunctions.cutURLParameters("https://example.com/path", new String[]{"param"}),
        "https://example.com/path");
    assertEquals(UrlFunctions.cutURLParameters("https://example.com", new String[]{"param"}), "https://example.com");

    assertEquals(UrlFunctions.cutURLParameters("invalid_url", new String[]{"param"}), "invalid_url");
    assertEquals(UrlFunctions.cutURLParameters("http://", new String[]{"param"}), "http://");
    assertEquals(UrlFunctions.cutURLParameters("https://", new String[]{"param"}), "https://");
    assertEquals(UrlFunctions.cutURLParameters("", new String[]{"param"}), "");
    assertNull(UrlFunctions.cutURLParameters(null, new String[]{"param"}));

    assertEquals(UrlFunctions.cutURLParameters("https://example.com/path?param=value", new String[]{""}),
        "https://example.com/path?param=value");
    assertEquals(UrlFunctions.cutURLParameters("https://example.com/path", new String[]{""}),
        "https://example.com/path");

    assertEquals(UrlFunctions.cutURLParameters("https://example.com/path?param=value", new String[0]),
        "https://example.com/path?param=value");
    assertEquals(UrlFunctions.cutURLParameters("https://example.com/path", new String[0]), "https://example.com/path");
  }
}
