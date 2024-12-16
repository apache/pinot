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

import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;
import org.apache.pinot.spi.annotations.ScalarFunction;


/**
 * URL Transformation Functions
 * The functions can be used as UDFs in Query when added in the FunctionRegistry.
 * {@code @ScalarFunction} annotation is used with each method for the registration
 */
public class UrlFunctions {
  private UrlFunctions() {
  }

  /**
   * Extracts the protocol (scheme) from the URL.
   *
   * @param url URL string
   * @return Protocol or null if invalid
   */
  @Nullable
  @ScalarFunction
  public static String urlProtocol(String url) {
    try {
      return URI.create(url).getScheme();
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Extracts the domain from the URL.
   *
   * @param url URL string
   * @return Domain or null if invalid
   */
  @Nullable
  @ScalarFunction
  public static String urlDomain(String url) {
    try {
      return URI.create(url).getHost();
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Extracts the domain without the leading "www." if present.
   *
   * @param url URL string
   * @return Domain without "www." or null if invalid
   */
  @Nullable
  @ScalarFunction
  public static String urlDomainWithoutWWW(String url) {
    try {
      String domain = URI.create(url).getHost();
      if (domain != null && domain.startsWith("www.")) {
        return domain.substring(4);
      }
      return domain;
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Extracts the top-level domain (TLD) from the URL.
   *
   * @param url URL string
   * @return Top-level domain or null if invalid
   */
  @Nullable
  @ScalarFunction
  public static String urlTopLevelDomain(String url) {
    try {
      String domain = URI.create(url).getHost();
      if (domain != null) {
        String[] domainParts = domain.split("\\.");
        return domainParts[domainParts.length - 1];
      }
      return null;
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Extracts the first significant subdomain from the URL.
   *
   * @param url URL string
   * @return First significant subdomain or null if invalid
   */
  @Nullable
  @ScalarFunction
  public static String urlFirstSignificantSubdomain(String url) {
    try {
      String domain = URI.create(url).getHost();
      if (domain != null) {
        String[] domainParts = domain.split("\\.");
        if (domainParts.length <= 2) {
          return domainParts[0];
        }
        String tld = domainParts[domainParts.length - 1];
        if (tld.equals("com") || tld.equals("net") || tld.equals("org") || tld.equals("co")) {
          return domainParts[domainParts.length - 2];
        }
        return domainParts[domainParts.length - 3];
      }
      return null;
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Extracts the first significant subdomain and the top-level domain from the URL.
   *
   * @param url URL string
   * @return First significant subdomain and top-level domain or null if invalid
   */
  @Nullable
  @ScalarFunction
  public static String cutToFirstSignificantSubdomain(String url) {
    try {
      String domain = URI.create(url).getHost();
      if (domain != null) {
        String[] domainParts = domain.split("\\.");
        if (domainParts.length <= 2) {
          return domain;
        }
        String tld = domainParts[domainParts.length - 1];
        if (tld.equals("com") || tld.equals("net") || tld.equals("org") || tld.equals("co")) {
          return domainParts[domainParts.length - 2] + "." + domainParts[domainParts.length - 1];
        }
        return domainParts[domainParts.length - 3] + "." + domainParts[domainParts.length - 2] + "." + domainParts[
            domainParts.length - 1];
      }
      return null;
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Returns the part of the domain that includes top-level subdomains up to the "first significant subdomain",
   * without stripping www.
   *
   * @param url URL string
   * @return First significant subdomain and top-level domain or null if invalid
   */
  @Nullable
  @ScalarFunction
  public static String cutToFirstSignificantSubdomainWithWWW(String url) {
    try {
      String domain = URI.create(url).getHost();
      if (domain != null) {
        String[] domainParts = domain.split("\\.");
        if (domainParts.length <= 2) {
          return domain;
        }
        String tld = domainParts[domainParts.length - 1];
        if (tld.equals("com") || tld.equals("net") || tld.equals("org") || tld.equals("co")) {
          String subDomain = domainParts[domainParts.length - 2] + "." + domainParts[domainParts.length - 1];
          if (domainParts[0].equals("www") && domainParts.length == 3) {
            return "www." + subDomain;
          }
          return subDomain;
        }
        String subDomain =
            domainParts[domainParts.length - 3] + "." + domainParts[domainParts.length - 2] + "." + domainParts[
                domainParts.length - 1];
        if (domainParts[0].equals("www") && domainParts.length == 4) {
          return "www." + subDomain;
        }
        return subDomain;
      }
      return null;
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Extracts the port from the URL.
   *
   * @param url URL string
   * @return Port or -1 if invalid or not specified
   */
  @ScalarFunction
  public static int urlPort(String url) {
    try {
      return URI.create(url).getPort();
    } catch (Exception e) {
      return -1;
    }
  }

  /**
   * Extracts the path from the URL without the query string.
   *
   * @param url URL string
   * @return Path or null if invalid
   */
  @Nullable
  @ScalarFunction
  public static String urlPath(String url) {
    try {
      URI uri = new URI(url);
      if (uri.getScheme() == null || uri.getHost() == null) {
        return null; // Ensure the URL has a valid scheme and host
      }
      return uri.getPath();
    } catch (Exception e) {
      return null; // Return null for any invalid input
    }
  }

  /**
   * Function to extract the path from the URL with query string.
   *
   * @param url URL string
   * @return path with query string or null if invalid
   */
  @Nullable
  @ScalarFunction
  public static String urlPathWithQuery(String url) {
    try {
      URI uri = new URI(url);
      if (uri.getScheme() == null || uri.getHost() == null) {
        return null; // Ensure the URL has a valid scheme and host
      }
      return URI.create(url).getRawPath();
    } catch (Exception e) {
      return null; // Return null for any invalid input
    }
  }

  /**
   * Extracts the query string without the initial question mark (`?`) and excludes
   * the fragment (`#`) and everything after it.
   *
   * @param url URL string
   * @return Query string without `?` or null if invalid or not present
   */
  @Nullable
  @ScalarFunction
  public static String urlQueryString(String url) {
    try {
      if (url == null) {
        return null;
      }

      URI uri = new URI(url);
      // Extract the query string (raw, un-decoded)
      return uri.getRawQuery(); // Return the query string directly (excluding `?`)
    } catch (Exception e) {
      return null; // Return null for invalid URLs
    }
  }

  /**
   * Extracts the fragment identifier (without the hash symbol) from the URL.
   *
   * @param url URL string
   * @return Fragment or null if invalid or not present
   */
  @Nullable
  @ScalarFunction
  public static String urlFragment(String url) {
    try {
      return URI.create(url).getFragment();
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Extracts the query string and fragment identifier from the URL.
   * Example:
   * Input: "<a href="https://example.com/path?page=1#section">https://example.com/path?page=1#section</a>"
   * Output: "page=1#section"
   *
   * @param url URL string
   * @return Query string and fragment identifier, or null if invalid or not present
   */
  @Nullable
  @ScalarFunction
  public static String urlQueryStringAndFragment(String url) {
    try {
      if (url == null) {
        return null;
      }

      URI uri = new URI(url);
      String query = uri.getQuery();
      String fragment = uri.getFragment();

      if (query == null && fragment == null) {
        return null;
      }

      StringBuilder result = new StringBuilder();
      if (query != null) {
        result.append(query);
      }
      if (fragment != null) {
        if (result.length() > 0) {
          result.append("#");
        }
        result.append(fragment);
      }

      return result.toString();
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Extracts the value of a specific query parameter from the URL.
   * If multiple parameters with the same name exist, the first one is returned.
   * Example:
   * Input: ("<a href="https://example.com/path?page=1&lr=213">https://example.com/path?page=1&lr=213</a>", "page")
   * Output: "1"
   *
   * @param url  URL string
   * @param name Name of the parameter to extract
   * @return Value of the parameter, or an empty string if not found or invalid
   */
  @ScalarFunction
  public static String extractURLParameter(String url, String name) {
    try {
      if (url == null || name == null) {
        return "";
      }

      URI uri = new URI(url);
      String query = uri.getQuery();
      if (query == null) {
        return "";
      }

      for (String param : query.split("&")) {
        String[] keyValue = param.split("=", 2);
        if (keyValue[0].equals(name)) {
          return keyValue.length > 1 ? keyValue[1] : "";
        }
      }

      return "";
    } catch (Exception e) {
      return "";
    }
  }

  /**
   * Extracts all query parameters from the URL as an array of name=value pairs.
   * Example:
   * Input: "<a href="https://example.com/path?page=1&lr=213">https://example.com/path?page=1&lr=213</a>"
   * Output: ["page=1", "lr=213"]
   *
   * @param url URL string
   * @return Array of name=value pairs, or an empty array if no query parameters are present
   */
  @ScalarFunction
  public static String[] extractURLParameters(String url) {
    try {
      if (url == null) {
        return new String[0];
      }

      URI uri = new URI(url);
      String query = uri.getQuery();
      if (query == null) {
        return new String[0];
      }

      return query.split("&");
    } catch (Exception e) {
      return new String[0];
    }
  }

  /**
   * Extracts all parameter names from the URL query string.
   * Example:
   * Input: "<a href="https://example.com/path?page=1&lr=213">https://example.com/path?page=1&lr=213</a>"
   * Output: ["page", "lr"]
   *
   * @param url URL string
   * @return Array of parameter names, or an empty array if no query parameters are present
   */
  @ScalarFunction
  public static String[] extractURLParameterNames(String url) {
    try {
      if (url == null) {
        return new String[0];
      }

      URI uri = new URI(url);
      String query = uri.getQuery();
      if (query == null) {
        return new String[0];
      }

      String[] params = query.split("&");
      String[] names = new String[params.length];
      for (int i = 0; i < params.length; i++) {
        names[i] = params[i].split("=", 2)[0];
      }
      return names;
    } catch (Exception e) {
      return new String[0];
    }
  }

  /**
   * Generates a hierarchy of URLs truncated at path and query separators.
   * Each level of the path is included in the hierarchy.
   *
   * @param url URL string
   * @return Array of truncated URLs representing the hierarchy, or an empty array if invalid
   */
  @ScalarFunction
  public static String[] urlHierarchy(String url) {
    try {
      if (url == null) {
        return new String[0];
      }

      URI uri = new URI(url);
      if (uri.getScheme() == null || uri.getHost() == null) {
        return new String[0]; // Ensure the URL has a valid scheme and host
      }
      String baseUrl = uri.getScheme() + "://" + uri.getHost();
      String path = uri.getPath();

      if (path == null || path.isEmpty()) {
        return new String[]{baseUrl}; // Return only the base URL
      }

      String[] pathParts = path.split("/");
      String[] hierarchy = new String[pathParts.length];
      hierarchy[0] = baseUrl;
      StringBuilder currentPath = new StringBuilder(baseUrl);

      for (int i = 1; i < pathParts.length; i++) {
        currentPath.append("/").append(pathParts[i]);
        hierarchy[i] = currentPath.toString();
      }

      return hierarchy;
    } catch (Exception e) {
      return new String[0];
    }
  }

  /**
   * Generates a hierarchy of path elements from the URL.
   * The protocol and host are excluded. The root ("/") is not included.
   * Example:
   * Input: "<a href="https://example.com/browse/CONV-6788">https://example.com/browse/CONV-6788</a>"
   * Output: ["/browse/", "/browse/CONV-6788"]
   *
   * @param url URL string
   * @return Array of truncated path elements, or an empty array if invalid
   */
  @ScalarFunction
  public static String[] urlPathHierarchy(String url) {
    try {
      if (url == null) {
        return new String[0];
      }

      URI uri = new URI(url);
      String path = uri.getPath();
      if (path == null || path.isEmpty()) {
        return new String[0];
      }

      String[] pathParts = path.split("/");
      String[] hierarchy = new String[pathParts.length - 1];
      for (int i = 1; i < pathParts.length; i++) {
        StringBuilder part = new StringBuilder();
        for (int j = 1; j <= i; j++) {
          part.append("/").append(pathParts[j]);
        }
        hierarchy[i - 1] = part.toString();
      }

      return hierarchy;
    } catch (Exception e) {
      return new String[0];
    }
  }

  /**
   * Encodes a string into a URL-safe format.
   *
   * @param url String to encode
   * @return URL-encoded string or null if invalid
   */
  @Nullable
  @ScalarFunction
  public static String urlEncode(String url) {
    try {
      return URLEncoder.encode(url, StandardCharsets.UTF_8).replace("%20", "+");
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Decodes a URL-encoded string.
   *
   * @param url URL-encoded string
   * @return Decoded string or null if invalid
   */
  @Nullable
  @ScalarFunction
  public static String urlDecode(String url) {
    try {
      return URLDecoder.decode(url, StandardCharsets.UTF_8);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Encodes the URL string following RFC-1866 standards.
   * Spaces are encoded as `+`.
   *
   * @param url URL string to encode
   * @return Encoded URL string
   */
  @Nullable
  @ScalarFunction
  public static String urlEncodeFormComponent(String url) {
    try {
      if (url == null) {
        return null;
      }
      return URLEncoder.encode(url, StandardCharsets.UTF_8);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Decodes the URL string following RFC-1866 standards.
   * `+` is decoded as a space.
   *
   * @param url Encoded URL string
   * @return Decoded URL string
   */
  @Nullable
  @ScalarFunction
  public static String urlDecodeFormComponent(String url) {
    try {
      if (url == null) {
        return null;
      }
      return URLDecoder.decode(url.replace("+", "%20"), StandardCharsets.UTF_8);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Extracts the network locality (username:password@host:port) from the URL.
   *
   * @param url URL string
   * @return Network locality string, or null if invalid
   */
  @Nullable
  @ScalarFunction
  public static String urlNetloc(String url) {
    try {
      if (url == null) {
        return null;
      }

      URI uri = new URI(url);

      // Extract user info (username:password)
      String userInfo = uri.getUserInfo();

      // Extract host
      String host = uri.getHost();

      // Extract port
      int port = uri.getPort();
      String portStr = (port == -1) ? "" : ":" + port;

      // Combine into netloc format
      StringBuilder netloc = new StringBuilder();
      if (userInfo != null && !userInfo.isEmpty()) {
        netloc.append(userInfo).append("@");
      }
      if (host != null) {
        netloc.append(host);
      }
      netloc.append(portStr);

      return netloc.toString();
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Removes the leading www. from a URL’s domain.
   */
  @ScalarFunction
  public static String cutWWW(String url) {
    try {
      URI uri = new URI(url);
      String host = uri.getHost();
      if (host != null && host.startsWith("www.")) {
        host = host.substring(4);
      }
      return new URI(uri.getScheme(), uri.getUserInfo(), host, uri.getPort(), uri.getPath(), uri.getQuery(),
          uri.getFragment()).toString();
    } catch (Exception e) {
      return url; // Return unchanged if there's an error
    }
  }

  /**
   * Removes the query string, including the question mark.
   */
  @ScalarFunction
  public static String cutQueryString(String url) {
    try {
      URI uri = new URI(url);
      return new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(), null,
          uri.getFragment()).toString();
    } catch (Exception e) {
      return url; // Return unchanged if there's an error
    }
  }

  /**
   * Removes the fragment identifier, including the number sign.
   */
  @ScalarFunction
  public static String cutFragment(String url) {
    try {
      URI uri = new URI(url);
      return new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(), uri.getQuery(),
          null).toString();
    } catch (Exception e) {
      return url; // Return unchanged if there's an error
    }
  }

  /**
   * Removes both the query string and fragment identifier.
   */
  @ScalarFunction
  public static String cutQueryStringAndFragment(String url) {
    try {
      URI uri = new URI(url);
      return new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(), null,
          null).toString();
    } catch (Exception e) {
      return url; // Return unchanged if there's an error
    }
  }

  /**
   * Removes specific query parameters from a URL.
   */
  @ScalarFunction
  public static String cutURLParameter(String url, String name) {
    try {
      URI uri = new URI(url);
      String query = uri.getQuery();
      if (query == null || query.isEmpty()) {
        return url; // No query string to process
      }

      StringBuilder newQuery = new StringBuilder();
      for (String param : query.split("&")) {
        String[] pair = param.split("=", 2);
        String key = URLDecoder.decode(pair[0], StandardCharsets.UTF_8);
        if (!key.equals(name)) {
          if (newQuery.length() > 0) {
            newQuery.append("&");
          }
          newQuery.append(pair[0]);
          if (pair.length > 1) {
            newQuery.append("=").append(pair[1]);
          }
        }
      }

      return new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(),
          newQuery.length() > 0 ? newQuery.toString() : null, uri.getFragment()).toString();
    } catch (Exception e) {
      return url; // Return unchanged if there's an error
    }
  }

  /**
   * Removes specific query parameters from a URL.
   *
   * @param url   The URL string from which the query parameters should be removed.
   * @param names An array of query parameter names to remove.
   * @return The URL string with the specified query parameters removed.
   */
  @ScalarFunction
  public static String cutURLParameters(String url, String[] names) {
    for (String name : names) {
      url = cutURLParameter(url, name);
    }
    return url;
  }
}
