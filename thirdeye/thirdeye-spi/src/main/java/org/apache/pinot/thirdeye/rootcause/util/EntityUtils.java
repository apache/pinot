/*
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

package org.apache.pinot.thirdeye.rootcause.util;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import org.apache.pinot.thirdeye.rootcause.Entity;
import org.apache.pinot.thirdeye.rootcause.MaxScoreSet;
import org.apache.pinot.thirdeye.rootcause.impl.AnomalyEventEntity;
import org.apache.pinot.thirdeye.rootcause.impl.DatasetEntity;
import org.apache.pinot.thirdeye.rootcause.impl.DimensionEntity;
import org.apache.pinot.thirdeye.rootcause.impl.DimensionsEntity;
import org.apache.pinot.thirdeye.rootcause.impl.EntityType;
import org.apache.pinot.thirdeye.rootcause.impl.HyperlinkEntity;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.rootcause.impl.ServiceEntity;
import org.apache.pinot.thirdeye.rootcause.impl.TimeRangeEntity;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;


/**
 * Utility class to simplify type-checking and extraction of entities
 */
public class EntityUtils {
  private static Pattern PATTERN_FILTER_OPERATOR = Pattern.compile("!=|>=|<=|==|>|<|=");

  private static Map<String, String> FILTER_TO_OPERATOR = new LinkedHashMap<>();
  static {
    FILTER_TO_OPERATOR.put("!", "!=");
    FILTER_TO_OPERATOR.put("<=", "<=");
    FILTER_TO_OPERATOR.put("<", "<");
    FILTER_TO_OPERATOR.put(">=", ">=");
    FILTER_TO_OPERATOR.put(">", ">");
  }

  /**
   * Returns {@code true} if the URN encodes the specified entity type {@code type}, or
   * {@code false} otherwise.
   *
   * @param urn entity urn
   * @param type entity type
   * @return {@code true} if entity type matches, {@code false} otherwise.
   */
  public static boolean isType(String urn, EntityType type) {
    return urn.startsWith(type.getPrefix());
  }

  /**
   * Returns {@code true} if the URN encodes the specified entity type {@code type}, or
   * {@code false} otherwise.
   *
   * @param e entity
   * @param type entity type
   * @return {@code true} if entity type matches, {@code false} otherwise.
   */
  public static boolean isType(Entity e, EntityType type) {
    return e.getUrn().startsWith(type.getPrefix());
  }

  /**
   * Returns a mapping of URNs to entities derived from a collection of entities. In case
   * the same URN is used by multiple entities only one entity is referenced in the resulting map.
   *
   * @param entities entities
   * @param <T> (sub-)class of Entity
   * @return mapping of URNs to Entities
   */
  public static <T extends Entity> Map<String, T> mapEntityURNs(Collection<T> entities) {
    Map<String, T> map = new HashMap<>();
    for(T e : entities) {
      map.put(e.getUrn(), e);
    }
    return map;
  }

  /**
   * Throws an IllegalArgumentException if the URN does not encode the specified entity type.
   *
   * @param urn entity URN
   * @param type entity type
   * @throws IllegalArgumentException if the URN does not encode the specified entity type
   * @return the entity urn
   */
  public static String assertType(String urn, EntityType type) {
    if(!isType(urn, type))
      throw new IllegalArgumentException(String.format("Entity '%s' is not a '%s'", urn, type.getPrefix()));
    return urn;
  }

  /**
   * Throws an IllegalArgumentException if the entity's URN does not encode the specified entity type.
   *
   * @param entity entity
   * @param type entity type
   * @throws IllegalArgumentException if the entity's URN does not encode the specified entity type
   * @return the entity
   */
  public static Entity assertType(Entity entity, EntityType type) {
    assertType(entity.getUrn(), type);
    return entity;
  }

  /**
   * Normalizes scores among a set of entities to a range between {@code 0.0} and {@code 1.0}.
   *
   * @param entities entities
   * @return entities with normalized scores
   */
  public static Set<Entity> normalizeScores(Collection<? extends Entity> entities) {
    double min = Double.MAX_VALUE;
    double max = Double.MIN_VALUE;

    for(Entity e : entities) {
      min = Math.min(e.getScore(), min);
      max = Math.max(e.getScore(), max);
    }

    double range = max - min;
    Set<Entity> out = new HashSet<>();

    if(range <= 0) {
      for(Entity e : entities) {
        out.add(e.withScore(1.0));
      }
      return out;
    }

    for(Entity e : entities) {
      out.add(e.withScore((e.getScore() - min) / range));
    }

    return out;
  }

  /**
   * Returns the top {@code K} entities based on score
   *
   * @param entities entities
   * @param k top k elements to return (<0 indicates all)
   * @return top k entities
   */
  public static <T extends Entity> Set<T> topk(Collection<T> entities, int k) {
    if (k < 0)
      return new HashSet<>(entities);
    List<T> sorted = new ArrayList<>(entities);
    Collections.sort(sorted, Entity.HIGHEST_SCORE_FIRST);
    return new HashSet<>(sorted.subList(0, Math.min(k, sorted.size())));
  }

  /**
   * Returns the top {@code K} entities based on score after normalizing scores to the interval
   * {@code [0.0, 1.0]}.
   *
   * @param entities entities
   * @param k top k elements to return (<0 indicates all)
   * @return top k normalized entities
   */
  public static Set<Entity> topkNormalized(Collection<? extends Entity> entities, int k) {
    return topk(normalizeScores(entities), k);
  }

  /**
   * Filters the input collection by (super) class {@code clazz}.
   * Returns a set of typed Entities or an empty set if no matching instances are found.  URN
   * conflicts are resolved by preserving the entity with the highest score.
   *
   * @param clazz (super) class to filter by
   * @param <T> (super) class of output collection
   * @return set of Entities with given super class
   */
  public static <T extends Entity> Set<T> filter(Collection<? extends Entity> entities, Class<? extends T> clazz) {
    Set<T> filtered = new MaxScoreSet<>();
    for (Entity e : entities) {
      if (clazz.isInstance(e))
        filtered.add((T) e);
    }
    return filtered;
  }

  /**
   * Attemps to parse {@code urn} and return a specific Entity subtype with the given {@code score}
   * Supports {@code MetricEntity}, {@code DimensionEntity}, {@code TimeRangeEntity}, and
   * {@code ServiceEntity}.
   *
   * @param urn entity urn
   * @param score entity score
   * @throws IllegalArgumentException, if the urn cannot be parsed
   * @return entity subtype instance
   */
  public static Entity parseURN(String urn, double score) {
    if(DimensionEntity.TYPE.isType(urn)) {
      return DimensionEntity.fromURN(urn, score);

    } else if(MetricEntity.TYPE.isType(urn)) {
      return MetricEntity.fromURN(urn, score);

    } else if(TimeRangeEntity.TYPE.isType(urn)) {
      return TimeRangeEntity.fromURN(urn, score);

    } else if(ServiceEntity.TYPE.isType(urn)) {
      return ServiceEntity.fromURN(urn, score);

    } else if(DatasetEntity.TYPE.isType(urn)) {
      return DatasetEntity.fromURN(urn, score);

    } else if(HyperlinkEntity.TYPE.isType(urn)) {
      return HyperlinkEntity.fromURL(urn, score);

    } else if(AnomalyEventEntity.TYPE.isType(urn)) {
      return AnomalyEventEntity.fromURN(urn, score);

    } else if(DimensionsEntity.TYPE.isType(urn)) {
      return DimensionsEntity.fromURN(urn, score);

    }
    throw new IllegalArgumentException(String.format("Could not parse URN '%s'", urn));
  }

  /**
   * Attemps to parse {@code urn} and return a specific Entity subtype with the given {@code score}
   * Supports {@code MetricEntity}, {@code DimensionEntity}, {@code TimeRangeEntity}, and
   * {@code ServiceEntity}.
   * If Urn can't be parsed return raw entity
   *
   * @param urn entity urn
   * @param score entity score
   * @return entity subtype instance
   */
  public static Entity parseURNRaw(String urn, double score) {
    try {
      return parseURN(urn, score);
    } catch (IllegalArgumentException e) {
      return new Entity(urn, score, new ArrayList<Entity>());
    }
  }

  /**
   * Sets a list of entities as the related entities set.
   * <br/><b>NOTE:</b> co-variant. Requires {@code Entity.withRelated(Entity)}
   * to return an instance of the respective {@code Entity} subclass.
   *
   * @param entities base entities to set related entities on
   * @param related related entities
   * @return List of base entities with related entities
   */
  public static <T extends Entity> List<T> withRelated(Iterable<T> entities, List<? extends Entity> related) {
    List<T> tagged = new ArrayList<>();
    for(T e : entities) {
      tagged.add((T)e.withRelated(related));
    }
    return tagged;
  }

  /**
   * Sets a single entity as the related entities set.
   * <br/><b>NOTE:</b> co-variant. Requires {@code Entity.withRelated(Entity)}
   * to return an instance of the respective {@code Entity} subclass.
   *
   * @see EntityUtils#addRelated(Iterable, List)
   *
   * @param entities base entities to set related entity on
   * @param related related entity
   * @return List of base entities with added related entity
   */
  public static <T extends Entity> List<T> withRelated(Iterable<T> entities, Entity related) {
    return withRelated(entities, Collections.singletonList(related));
  }

  /**
   * Adds a list of entities to the related entities set.
   * <br/><b>NOTE:</b> co-variant. Requires {@code Entity.withRelated(Entity)}
   * to return an instance of the respective {@code Entity} subclass.
   *
   * @param entities base entities to add related entities to
   * @param related related entities
   * @return List of base entities with related entities
   */
  public static <T extends Entity> List<T> addRelated(Iterable<T> entities, List<? extends Entity> related) {
    List<T> tagged = new ArrayList<>();
    for(T e : entities) {
      List<Entity> newRelated = new ArrayList<>(e.getRelated());
      newRelated.addAll(related);
      tagged.add((T)e.withRelated(newRelated));
    }
    return tagged;
  }

  /**
   * Adds a list of entities to the related entities set.
   * <br/><b>NOTE:</b> co-variant. Requires {@code Entity.withRelated(Entity)}
   * to return an instance of the respective {@code Entity} subclass.
   *
   * @see EntityUtils#addRelated(Iterable, List)
   *
   * @param entities base entities to add related entities to
   * @param related related entities
   * @return List of base entities with related entities
   */
  public static <T extends Entity> List<T> addRelated(Iterable<T> entities, Entity related) {
    return addRelated(entities, Collections.singletonList(related));
  }

  /**
   * Decode URN fragment to original data.
   * <br/><b>NOTE:</b> compatible with JavaScript's decodeURIComponent
   *
   * @param value urn fragment value
   * @return decoded value
   */
  public static String decodeURNComponent(String value) {
    try {
      return URLDecoder.decode(value, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      // must not happen, utf-8 is part of java spec
      throw new IllegalStateException(e);
    }
  }

  /**
   * Encode data to URN fragment.
   * <br/><b>NOTE:</b> similar to JavaScript's encodeURIComponent for basic ascii set
   *
   * @param value value
   * @return encoded urn fragment
   */
  public static String encodeURNComponent(String value) {
    try {
      return URLEncoder.encode(value, "UTF-8")
          .replace("+", "%20")
          .replace("%21", "!")
          .replace("%27", "\'")
          .replace("%28", "(")
          .replace("%29", ")")
          .replace("%7E", "~");
    } catch (UnsupportedEncodingException e) {
      // must not happen, utf-8 is part of java spec
      throw new IllegalStateException(e);
    }
  }

  /**
   * Decodes filter string fragments to a dimensions multimap
   *
   * @param filterStrings dimension fragments
   * @return dimensions multimap
   */
  public static Multimap<String, String> decodeDimensions(List<String> filterStrings) {
    Multimap<String, String> filters = TreeMultimap.create();

    for(String filterString : filterStrings) {
      if (StringUtils.isBlank(filterString)) {
        continue;
      }

      String[] parts = EntityUtils.decodeURNComponent(filterString).split("!=|<=|>=|<|>|=", 2);
      if (parts.length != 2) {
        throw new IllegalArgumentException(String.format("Could not parse filter string '%s'", filterString));
      }

      filters.put(parts[0], parts[1]);
    }

    return filters;
  }

  /**
   * Encodes dimensions multimap to filter strings.
   *
   * @param filters dimensions multimap
   * @return filter string fragments
   */
  public static List<String> encodeDimensions(Multimap<String, String> filters) {
    List<String> output = new ArrayList<>();

    Multimap<String, String> sorted = TreeMultimap.create(filters);
    for(Map.Entry<String, String> entry : sorted.entries()) {
      String operator = "=";
      String value = entry.getValue();

      // check for exclusion case
      for (Map.Entry<String, String> prefix : FILTER_TO_OPERATOR.entrySet()) {
        if (entry.getValue().startsWith(prefix.getKey())) {
          operator = prefix.getValue();
          value = entry.getValue().substring(prefix.getKey().length());
          break;
        }
      }

      output.add(EntityUtils.encodeURNComponent(String.format("%s%s%s", entry.getKey(), operator, value)));
    }

    return output;
  }

  /**
   * Returns the parsed urn without filters.
   *
   * @param urn entity urn string
   * @return ParsedUrn
   */
  public static ParsedUrn parseUrnString(String urn) {
    return new ParsedUrn(Arrays.asList(urn.split(":")), Collections.<FilterPredicate>emptySet());
  }

  /**
   * Returns the parsed urn for a given filter start offset. Handles ambiguous filter/urn values.
   *
   * <br/><b>Example:</b>
   * <pre>
   *   >> "thirdeye:metric:123:filter!=double:colon"
   *   << {["thirdeye", "metric", "123"], {"filter", "!=", "double:colon"}}
   * </pre>
   *
   * @param urn entity urn string
   * @param filterOffset start offset for filter values
   * @return ParsedUrn
   */
  public static ParsedUrn parseUrnString(String urn, int filterOffset) {
    String[] parts = urn.split(":", filterOffset + 1);

    // leading fragments are copied as-is
    List<String> prefixes = Arrays.asList(Arrays.copyOf(parts, filterOffset));

    // filter fragment (last fragment) is parsed back to front
    Set<FilterPredicate> predicates = new HashSet<>();

    if (parts.length > filterOffset && !parts[filterOffset].isEmpty()) {
      String[] filterFragments = parts[filterOffset].split(":");

      String currentFragment = decodeURNComponent(filterFragments[filterFragments.length - 1]);
      for (int i = filterFragments.length - 1; i > 0; i--) {
        if (currentFragment.isEmpty()) {
          // skip empty filter fragment, retain separator
          currentFragment = ":" + currentFragment;
          continue;
        }

        try {
          // attempt parsing current filter fragment
          predicates.add(extractFilterPredicate(currentFragment));
          currentFragment = decodeURNComponent(filterFragments[i-1]);

        } catch (IllegalArgumentException ignore) {
          // merge filter fragment with next if it doesn't parse
          currentFragment = String.format("%s:%s", decodeURNComponent(filterFragments[i - 1]), currentFragment);
        }
      }

      // last (combined) filter fragment must parse
      predicates.add(extractFilterPredicate(currentFragment));
    }

    return new ParsedUrn(prefixes, predicates);
  }

  /**
   * Return the urn parsed for the given type. Validates whether urn matches type and extracts
   * optional filter tail.
   *
   * @param urn entity urn string
   * @param type expected entity type
   * @return ParsedUrn
   */
  public static ParsedUrn parseUrnString(String urn, EntityType type) {
    if (!type.isType(urn)) {
      throw new IllegalArgumentException(String.format("Expected type '%s' but got '%s'", type.getPrefix(), urn));
    }
    return parseUrnString(urn);
  }

  /**
   * Return the urn parsed for the given type. Validates whether urn matches type and extracts
   * optional filter tail.
   *
   * @param urn entity urn string
   * @param type expected entity type
   * @param filterOffset start offset for filter values
   * @return ParsedUrn
   */
  public static ParsedUrn parseUrnString(String urn, EntityType type, int filterOffset) {
    if (urn == null || !type.isType(urn)) {
      throw new IllegalArgumentException(String.format("Expected type '%s' but got '%s'", type.getPrefix(), urn));
    }
    return parseUrnString(urn, filterOffset);
  }

  /**
   * Returns the filter predicate for a given filter string
   *
   * <br/><b>Example:</b>
   * <pre>
   *   >> "country != us"
   *   << {"country", "!=", "us"}
   * </pre>
   *
   * @param filterString raw (decoded) filter string
   * @return filter predicate
   */
  public static FilterPredicate extractFilterPredicate(String filterString) {
    Matcher m = PATTERN_FILTER_OPERATOR.matcher(filterString);
    if (!m.find()) {
      throw new IllegalArgumentException(
          String.format("Could not find filter predicate operator. Expected regex '%s'", PATTERN_FILTER_OPERATOR.pattern()));
    }

    int keyStart = 0;
    int keyEnd = m.start();
    String key = filterString.substring(keyStart, keyEnd);

    int opStart = m.start();
    int opEnd = m.end();
    String operator = filterString.substring(opStart, opEnd);

    int valueStart = m.end();
    int valueEnd = filterString.length();
    String value = filterString.substring(valueStart, valueEnd);

    return new FilterPredicate(key, operator, value);
  }

}
