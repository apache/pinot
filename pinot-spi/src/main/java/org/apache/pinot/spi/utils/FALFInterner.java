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

package org.apache.pinot.spi.utils;


import com.google.common.collect.Interner;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;

/**
 * Fixed-size Array-based, Lock-Free Interner.
 *
 * !!!!!!!!!!!!!!! READ THE PARAGRAPH BELOW BEFORE USING THIS CLASS !!!!!!!!!!!!!!!!
 * This class is technically not thread-safe. Therefore if it's called from multiple
 * threads, it should either be used with proper synchronization (in the same way as
 * you would use e.g. a HashMap), or under the following conditions:
 * all the objects being interned are not just immutable, but also final (that is, all
 * their fields used in equals() and hashCode() methods are explicitly marked final).
 * That's to ensure that all threads always see the same contents of these objects. If
 * this rule is not followed, using this class from multiple threads may lead to strange
 * non-deterministic errors. Note that objects with all private fields that are not
 * marked final, or immutable collections created via Collection.unmodifiableMap() etc,
 * don't qualify.
 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 *
 * This interner is intended to be used when either:
 * (a) distribution of values among the objects to be interned make them not suitable
 *     for standard interners
 * (b) speed is more important than ultimate memory savings
 *
 * Problem (a) occurs when both the total number of objects AND the number of unique
 * values is large. For example, there are 1M strings that look like "a", "a", "b", "b",
 * "c", "c", ... - that is, for each unique value there are only two separate objects.
 * Another problematic case is when "a" has 1000 copies, "b" has 900 copies, etc.,
 * but in the last few hundred thousand objects each one is unique. In both cases, if
 * we use a standard interner such as a Guava interner or a ConcurrentHashMap to
 * deduplicate such objects, the amount of memory consumed by the interner itself to
 * store objects that have few or no duplicates, can be comparable, or even exceed, the
 * savings achieved by getting rid of duplicate objects.
 *
 * This implementation addresses the above problems by interning objects "optimistically".
 * It is a fixed-size, open-hashmap-based object cache. When there is a cache miss,
 * a cached object in the given slot is always replaced with a new object. There is
 * no locking and no synchronization, and thus, no associated overhead. In essence,
 * this cache is based on the idea that an object with value X, that has many copies,
 * has a higher chance of staying in the cache for long enough to guarantee several
 * cache hits for itself before a miss evicts it and replaces it with an object with
 * a different value Y.
 *
 * This interner has a minimum possible memory footprint. You should be careful when
 * choosing its capacity. In general, the bigger the better, but if some of the objects
 * that are interned eventually go away, an interner with too big a capacity may still
 * keep these objects in memory. Also, since there are no collision chains, it is
 * very important to use a hash function with the most uniform distribution, to minimize
 * a chance that two or more objects with many duplicates compete for the same array slot.
 *
 * For more information, see https://dzone.com/articles/duplicate-objects-in-java-not-just-strings
 * Credits to the author: Misha Dmitriev
 */
public class FALFInterner<T> implements Interner<T> {
  private static final int MAXIMUM_CAPACITY = 1 << 30;

  private final Object[] _cache;
  private final int _cacheLengthMinusOne;
  private final BiPredicate<T, T> _equalsFunction;
  private final ToIntFunction<T> _hashFunction;

  /**
   * Constructs a new instance with the specified capacity.
   * Actual capacity will be a power of two number >= expectedCapacity.
   */
  public FALFInterner(int expectedCapacity) {
    this(expectedCapacity, Objects::hashCode);
  }

  /**
   * Constructs a new instance with the specified capacity and a custom hash function.
   * Actual capacity will be a power of two number >= expectedCapacity.
   */
  public FALFInterner(int expectedCapacity, ToIntFunction<T> hashFunction) {
    this(expectedCapacity, hashFunction, Objects::equals);
  }

  /**
   * Constructs a new instance with the specified capacity and custom equals and hash functions.
   * Actual capacity will be a power of two number >= expectedCapacity.
   */
  public FALFInterner(int expectedCapacity, ToIntFunction<T> hashFunction, BiPredicate<T, T> equalsFunction) {
    _cache = new Object[tableSizeFor(expectedCapacity)];
    _cacheLengthMinusOne = _cache.length - 1;
    _equalsFunction = Objects.requireNonNull(equalsFunction);
    _hashFunction = Objects.requireNonNull(hashFunction);
  }

  /**
   * IMPORTANT: OBJECTS TO INTERN SHOULD BE IMMUTABLE AND FINAL!
   * SEE THE JAVADOC OF THIS CLASS FOR MORE INFORMATION.
   *
   * Interns the given object. That is, if a cached object obj1 such that
   * obj1.equals(obj) is available, returns obj1. Otherwise, caches obj and
   * returns it. None of the cached objects is guaranteed to survive in the
   * cache.
   */
  @Override
  public T intern(T obj) {
    int slot = hash(obj) & _cacheLengthMinusOne;
    T cachedObj = (T) _cache[slot];
    if (cachedObj != null && _equalsFunction.test(obj, cachedObj)) {
      return cachedObj;
    }
    _cache[slot] = obj;
    return obj;
  }

  private int hash(T key) {
    int h = _hashFunction.applyAsInt(key);
    return h ^ (h >>> 16);
  }

  private static int tableSizeFor(int cap) {
    // Calculated in the same way as in java.util.HashMap
    int n = cap - 1;
    n |= n >>> 1;
    n |= n >>> 2;
    n |= n >>> 4;
    n |= n >>> 8;
    n |= n >>> 16;
    return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
  }
}
