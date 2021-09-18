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
package org.apache.pinot.segment.local.customobject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.PeekingIterator;
import com.google.common.util.concurrent.AtomicDouble;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;


/**
 * Re-implement io.airlift.stats.QuantileDigest with additional methods facilitating serialization.
 */
public class QuantileDigest {
  private static final int MAX_BITS = 64;
  private static final double MAX_SIZE_FACTOR = 1.5;

  private static final int HEADER_BYTE_SIZE = Double.BYTES  // Max error
      + Double.BYTES                                        // Alpha
      + Long.BYTES                                          // Landmark
      + Long.BYTES                                          // Min
      + Long.BYTES                                          // Max
      + Integer.BYTES;                                      // Node count
  private static final int NODE_BYTE_SIZE = Byte.BYTES      // Flags
      + Byte.BYTES                                          // Level
      + Long.BYTES                                          // Value
      + Double.BYTES;                                       // Weight

  // needs to be such that Math.exp(alpha * seconds) does not grow too big
  static final long RESCALE_THRESHOLD_SECONDS = 50;
  static final double ZERO_WEIGHT_THRESHOLD = 1e-5;

  private final double _maxError;
  private final Ticker _ticker;
  private final double _alpha;
  private final boolean _compressAutomatically;

  private Node _root;

  private double _weightedCount;
  private long _max = Long.MIN_VALUE;
  private long _min = Long.MAX_VALUE;

  private long _landmarkInSeconds;

  private int _totalNodeCount = 0;
  private int _nonZeroNodeCount = 0;
  private int _compressions = 0;

  private enum TraversalOrder {
    FORWARD, REVERSE
  }

  /**
   * <p>Create a QuantileDigest with a maximum error guarantee of "maxError" and no decay.
   *
   * @param maxError the max error tolerance
   */
  public QuantileDigest(double maxError) {
    this(maxError, 0);
  }

  /**
   * <p>Create a QuantileDigest with a maximum error guarantee of "maxError" and exponential decay
   * with factor "alpha".</p>
   *
   * @param maxError the max error tolerance
   * @param alpha    the exponential decay factor
   */
  public QuantileDigest(double maxError, double alpha) {
    this(maxError, alpha, Ticker.systemTicker(), true);
  }

  @VisibleForTesting
  QuantileDigest(double maxError, double alpha, Ticker ticker, boolean compressAutomatically) {
    checkArgument(maxError >= 0 && maxError <= 1, "maxError must be in range [0, 1]");
    checkArgument(alpha >= 0 && alpha < 1, "alpha must be in range [0, 1)");

    _maxError = maxError;
    _alpha = alpha;
    _ticker = ticker;
    _compressAutomatically = compressAutomatically;

    _landmarkInSeconds = TimeUnit.NANOSECONDS.toSeconds(ticker.read());
  }

  public QuantileDigest(QuantileDigest quantileDigest) {
    this(quantileDigest.getMaxError(), quantileDigest.getAlpha());
    merge(quantileDigest);
  }

  public double getMaxError() {
    return _maxError;
  }

  public double getAlpha() {
    return _alpha;
  }

  public void add(long value) {
    add(value, 1);
  }

  /**
   * Adds a value to this digest. The value must be {@code >= 0}
   */
  public void add(long value, long count) {
    checkArgument(count > 0, "count must be > 0");

    long nowInSeconds = TimeUnit.NANOSECONDS.toSeconds(_ticker.read());

    int maxExpectedNodeCount = 3 * calculateCompressionFactor();
    if (nowInSeconds - _landmarkInSeconds >= RESCALE_THRESHOLD_SECONDS) {
      rescale(nowInSeconds);
      compress(); // need to compress to get rid of nodes that may have decayed to ~ 0
    } else if (_nonZeroNodeCount > MAX_SIZE_FACTOR * maxExpectedNodeCount && _compressAutomatically) {
      // The size (number of non-zero nodes) of the digest is at most 3 * compression factor
      // If we're over MAX_SIZE_FACTOR of the expected size, compress
      // Note: we don't compress as soon as we go over expectedNodeCount to avoid unnecessarily
      // running a compression for every new added element when we're close to boundary
      compress();
    }

    double weight = weight(TimeUnit.NANOSECONDS.toSeconds(_ticker.read())) * count;

    _max = Math.max(_max, value);
    _min = Math.min(_min, value);

    insert(longToBits(value), weight);
  }

  public void merge(QuantileDigest other) {
    rescaleToCommonLandmark(this, other);

    // 2. merge other into this (don't modify other)
    _root = merge(_root, other._root);

    _max = Math.max(_max, other._max);
    _min = Math.min(_min, other._min);

    // 3. compress to remove unnecessary nodes
    compress();
  }

  /**
   * Gets the values at the specified quantiles +/- maxError. The list of quantiles must be sorted
   * in increasing order, and each value must be in the range [0, 1]
   */
  public List<Long> getQuantiles(List<Double> quantiles) {
    checkArgument(Ordering.natural().isOrdered(quantiles), "quantiles must be sorted in increasing order");
    for (double quantile : quantiles) {
      checkArgument(quantile >= 0 && quantile <= 1, "quantile must be between [0,1]");
    }

    final ImmutableList.Builder<Long> builder = ImmutableList.builder();
    final PeekingIterator<Double> iterator = Iterators.peekingIterator(quantiles.iterator());

    postOrderTraversal(_root, new Callback() {
      private double _sum = 0;

      @Override
      public boolean process(Node node) {
        _sum += node._weightedCount;

        while (iterator.hasNext() && _sum > iterator.peek() * _weightedCount) {
          iterator.next();

          // we know the max value ever seen, so cap the percentile to provide better error
          // bounds in this case
          long value = Math.min(node.getUpperBound(), _max);

          builder.add(value);
        }

        return iterator.hasNext();
      }
    });

    // we finished the traversal without consuming all quantiles. This means the remaining quantiles
    // correspond to the max known value
    while (iterator.hasNext()) {
      builder.add(_max);
      iterator.next();
    }

    return builder.build();
  }

  /**
   * Gets the value at the specified quantile +/- maxError. The quantile must be in the range [0, 1]
   */
  public long getQuantile(double quantile) {
    return getQuantiles(ImmutableList.of(quantile)).get(0);
  }

  /**
   * Number (decayed) of elements added to this quantile digest
   */
  public double getCount() {
    return _weightedCount / weight(TimeUnit.NANOSECONDS.toSeconds(_ticker.read()));
  }

  /*
   * Get the exponentially-decayed approximate counts of values in multiple buckets. The elements in
   * the provided list denote the upper bound each of the buckets and must be sorted in ascending
   * order.
   *
   * The approximate count in each bucket is guaranteed to be within 2 * totalCount * maxError of
   * the real count.
   */
  public List<Bucket> getHistogram(List<Long> bucketUpperBounds) {
    checkArgument(Ordering.natural().isOrdered(bucketUpperBounds), "buckets must be sorted in increasing order");

    final ImmutableList.Builder<Bucket> builder = ImmutableList.builder();
    final PeekingIterator<Long> iterator = Iterators.peekingIterator(bucketUpperBounds.iterator());

    final AtomicDouble sum = new AtomicDouble();
    final AtomicDouble lastSum = new AtomicDouble();

    // for computing weighed average of values in bucket
    final AtomicDouble bucketWeightedSum = new AtomicDouble();

    final double normalizationFactor = weight(TimeUnit.NANOSECONDS.toSeconds(_ticker.read()));

    postOrderTraversal(_root, new Callback() {
      @Override
      public boolean process(Node node) {

        while (iterator.hasNext() && iterator.peek() <= node.getUpperBound()) {
          double bucketCount = sum.get() - lastSum.get();

          Bucket bucket = new Bucket(bucketCount / normalizationFactor, bucketWeightedSum.get() / bucketCount);

          builder.add(bucket);
          lastSum.set(sum.get());
          bucketWeightedSum.set(0);
          iterator.next();
        }

        bucketWeightedSum.addAndGet(node.getMiddle() * node._weightedCount);
        sum.addAndGet(node._weightedCount);
        return iterator.hasNext();
      }
    });

    while (iterator.hasNext()) {
      double bucketCount = sum.get() - lastSum.get();
      Bucket bucket = new Bucket(bucketCount / normalizationFactor, bucketWeightedSum.get() / bucketCount);

      builder.add(bucket);

      iterator.next();
    }

    return builder.build();
  }

  public long getMin() {
    final AtomicLong chosen = new AtomicLong(_min);
    postOrderTraversal(_root, new Callback() {
      @Override
      public boolean process(Node node) {
        if (node._weightedCount >= ZERO_WEIGHT_THRESHOLD) {
          chosen.set(node.getLowerBound());
          return false;
        }
        return true;
      }
    }, TraversalOrder.FORWARD);

    return Math.max(_min, chosen.get());
  }

  public long getMax() {
    final AtomicLong chosen = new AtomicLong(_max);
    postOrderTraversal(_root, new Callback() {
      @Override
      public boolean process(Node node) {
        if (node._weightedCount >= ZERO_WEIGHT_THRESHOLD) {
          chosen.set(node.getUpperBound());
          return false;
        }
        return true;
      }
    }, TraversalOrder.REVERSE);

    return Math.min(_max, chosen.get());
  }

  public int getByteSize() {
    return HEADER_BYTE_SIZE + _totalNodeCount * NODE_BYTE_SIZE;
  }

  public byte[] toBytes() {
    byte[] bytes = new byte[getByteSize()];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byteBuffer.putDouble(_maxError);
    byteBuffer.putDouble(_alpha);
    byteBuffer.putLong(_landmarkInSeconds);
    byteBuffer.putLong(_min);
    byteBuffer.putLong(_max);
    byteBuffer.putInt(_totalNodeCount);
    postOrderTraversal(_root, node -> {
      serializeNode(byteBuffer, node);
      return true;
    });
    return bytes;
  }

  private void serializeNode(ByteBuffer byteBuffer, Node node) {
    byte flags = 0;
    if (node._left != null) {
      flags |= Flags.HAS_LEFT;
    }
    if (node._right != null) {
      flags |= Flags.HAS_RIGHT;
    }

    byteBuffer.put(flags);
    byteBuffer.put((byte) node._level);
    byteBuffer.putLong(node._bits);
    byteBuffer.putDouble(node._weightedCount);
  }

  public static QuantileDigest fromBytes(byte[] bytes) {
    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  public static QuantileDigest fromByteBuffer(ByteBuffer byteBuffer) {
    double maxError = byteBuffer.getDouble();
    double alpha = byteBuffer.getDouble();

    QuantileDigest quantileDigest = new QuantileDigest(maxError, alpha);
    quantileDigest._landmarkInSeconds = byteBuffer.getLong();
    quantileDigest._min = byteBuffer.getLong();
    quantileDigest._max = byteBuffer.getLong();
    int numNodes = byteBuffer.getInt();
    quantileDigest._totalNodeCount = numNodes;
    if (numNodes == 0) {
      return quantileDigest;
    }

    Stack<Node> stack = new Stack<>();
    for (int i = 0; i < numNodes; i++) {
      int flags = byteBuffer.get();
      int level = byteBuffer.get() & 0xFF;
      long bits = byteBuffer.getLong();
      double weightedCount = byteBuffer.getDouble();

      Node node = new Node(bits, level, weightedCount);
      if ((flags & Flags.HAS_RIGHT) != 0) {
        node._right = stack.pop();
      }
      if ((flags & Flags.HAS_LEFT) != 0) {
        node._left = stack.pop();
      }
      stack.push(node);

      quantileDigest._weightedCount += weightedCount;
      if (node._weightedCount >= ZERO_WEIGHT_THRESHOLD) {
        quantileDigest._nonZeroNodeCount++;
      }
    }

    checkState(stack.size() == 1, "Tree is corrupted, expecting a single root node");
    quantileDigest._root = stack.pop();

    return quantileDigest;
  }

  @VisibleForTesting
  int getTotalNodeCount() {
    return _totalNodeCount;
  }

  @VisibleForTesting
  int getNonZeroNodeCount() {
    return _nonZeroNodeCount;
  }

  @VisibleForTesting
  int getCompressions() {
    return _compressions;
  }

  @VisibleForTesting
  void compress() {
    _compressions++;

    final int compressionFactor = calculateCompressionFactor();

    postOrderTraversal(_root, new Callback() {
      @Override
      public boolean process(Node node) {
        if (node.isLeaf()) {
          return true;
        }

        // if children's weights are ~0 remove them and shift the weight to their parent

        double leftWeight = 0;
        if (node._left != null) {
          leftWeight = node._left._weightedCount;
        }

        double rightWeight = 0;
        if (node._right != null) {
          rightWeight = node._right._weightedCount;
        }

        boolean shouldCompress =
            node._weightedCount + leftWeight + rightWeight < (int) (_weightedCount / compressionFactor);

        double oldNodeWeight = node._weightedCount;
        if (shouldCompress || leftWeight < ZERO_WEIGHT_THRESHOLD) {
          node._left = tryRemove(node._left);

          _weightedCount += leftWeight;
          node._weightedCount += leftWeight;
        }

        if (shouldCompress || rightWeight < ZERO_WEIGHT_THRESHOLD) {
          node._right = tryRemove(node._right);

          _weightedCount += rightWeight;
          node._weightedCount += rightWeight;
        }

        if (oldNodeWeight < ZERO_WEIGHT_THRESHOLD && node._weightedCount >= ZERO_WEIGHT_THRESHOLD) {
          _nonZeroNodeCount++;
        }

        return true;
      }
    });

    if (_root != null && _root._weightedCount < ZERO_WEIGHT_THRESHOLD) {
      _root = tryRemove(_root);
    }
  }

  private double weight(long timestamp) {
    return Math.exp(_alpha * (timestamp - _landmarkInSeconds));
  }

  private void rescale(long newLandmarkInSeconds) {
    // rescale the weights based on a new landmark to avoid numerical overflow issues

    final double factor = Math.exp(-_alpha * (newLandmarkInSeconds - _landmarkInSeconds));

    _weightedCount *= factor;

    postOrderTraversal(_root, new Callback() {
      @Override
      public boolean process(Node node) {
        double oldWeight = node._weightedCount;

        node._weightedCount *= factor;

        if (oldWeight >= ZERO_WEIGHT_THRESHOLD && node._weightedCount < ZERO_WEIGHT_THRESHOLD) {
          _nonZeroNodeCount--;
        }

        return true;
      }
    });

    _landmarkInSeconds = newLandmarkInSeconds;
  }

  private int calculateCompressionFactor() {
    if (_root == null) {
      return 1;
    }

    return Math.max((int) ((_root._level + 1) / _maxError), 1);
  }

  private void insert(long bits, double weight) {
    long lastBranch = 0;
    Node parent = null;
    Node current = _root;

    while (true) {
      if (current == null) {
        setChild(parent, lastBranch, createLeaf(bits, weight));
        return;
      } else if (!inSameSubtree(bits, current._bits, current._level)) {
        // if bits and node.bits are not in the same branch given node's level,
        // insert a parent above them at the point at which branches diverge
        setChild(parent, lastBranch, makeSiblings(current, createLeaf(bits, weight)));
        return;
      } else if (current._level == 0 && current._bits == bits) {
        // found the node

        double oldWeight = current._weightedCount;

        current._weightedCount += weight;

        if (current._weightedCount >= ZERO_WEIGHT_THRESHOLD && oldWeight < ZERO_WEIGHT_THRESHOLD) {
          _nonZeroNodeCount++;
        }

        _weightedCount += weight;

        return;
      }

      // we're on the correct branch of the tree and we haven't reached a leaf, so keep going down
      long branch = bits & current.getBranchMask();

      parent = current;
      lastBranch = branch;

      if (branch == 0) {
        current = current._left;
      } else {
        current = current._right;
      }
    }
  }

  private void setChild(Node parent, long branch, Node child) {
    if (parent == null) {
      _root = child;
    } else if (branch == 0) {
      parent._left = child;
    } else {
      parent._right = child;
    }
  }

  private Node makeSiblings(Node node, Node sibling) {
    int parentLevel = MAX_BITS - Long.numberOfLeadingZeros(node._bits ^ sibling._bits);

    Node parent = createNode(node._bits, parentLevel, 0);

    // the branch is given by the bit at the level one below parent
    long branch = sibling._bits & parent.getBranchMask();
    if (branch == 0) {
      parent._left = sibling;
      parent._right = node;
    } else {
      parent._left = node;
      parent._right = sibling;
    }

    return parent;
  }

  private Node createLeaf(long bits, double weight) {
    return createNode(bits, 0, weight);
  }

  private Node createNode(long bits, int level, double weight) {
    _weightedCount += weight;
    _totalNodeCount++;
    if (weight >= ZERO_WEIGHT_THRESHOLD) {
      _nonZeroNodeCount++;
    }
    return new Node(bits, level, weight);
  }

  private Node merge(Node node, Node other) {
    if (node == null) {
      return copyRecursive(other);
    } else if (other == null) {
      return node;
    } else if (!inSameSubtree(node._bits, other._bits, Math.max(node._level, other._level))) {
      return makeSiblings(node, copyRecursive(other));
    } else if (node._level > other._level) {
      long branch = other._bits & node.getBranchMask();

      if (branch == 0) {
        node._left = merge(node._left, other);
      } else {
        node._right = merge(node._right, other);
      }
      return node;
    } else if (node._level < other._level) {
      Node result = createNode(other._bits, other._level, other._weightedCount);

      long branch = node._bits & other.getBranchMask();
      if (branch == 0) {
        result._left = merge(node, other._left);
        result._right = copyRecursive(other._right);
      } else {
        result._left = copyRecursive(other._left);
        result._right = merge(node, other._right);
      }

      return result;
    }

    // else, they must be at the same level and on the same path, so just bump the counts
    double oldWeight = node._weightedCount;

    _weightedCount += other._weightedCount;
    node._weightedCount = node._weightedCount + other._weightedCount;
    node._left = merge(node._left, other._left);
    node._right = merge(node._right, other._right);

    if (oldWeight < ZERO_WEIGHT_THRESHOLD && node._weightedCount >= ZERO_WEIGHT_THRESHOLD) {
      _nonZeroNodeCount++;
    }

    return node;
  }

  private static boolean inSameSubtree(long bitsA, long bitsB, int level) {
    return level == MAX_BITS || (bitsA >>> level) == (bitsB >>> level);
  }

  private Node copyRecursive(Node node) {
    Node result = null;

    if (node != null) {
      result = createNode(node._bits, node._level, node._weightedCount);
      result._left = copyRecursive(node._left);
      result._right = copyRecursive(node._right);
    }

    return result;
  }

  /**
   * Remove the node if possible or set its count to 0 if it has children and
   * it needs to be kept around
   */
  private Node tryRemove(Node node) {
    if (node == null) {
      return null;
    }

    if (node._weightedCount >= ZERO_WEIGHT_THRESHOLD) {
      _nonZeroNodeCount--;
    }

    _weightedCount -= node._weightedCount;

    Node result = null;
    if (node.isLeaf()) {
      _totalNodeCount--;
    } else if (node.hasSingleChild()) {
      result = node.getSingleChild();
      _totalNodeCount--;
    } else {
      node._weightedCount = 0;
      result = node;
    }

    return result;
  }

  private boolean postOrderTraversal(Node node, Callback callback) {
    return postOrderTraversal(node, callback, TraversalOrder.FORWARD);
  }

  // returns true if traversal should continue
  private boolean postOrderTraversal(Node node, Callback callback, TraversalOrder order) {
    if (node == null) {
      return false;
    }

    Node first;
    Node second;

    if (order == TraversalOrder.FORWARD) {
      first = node._left;
      second = node._right;
    } else {
      first = node._right;
      second = node._left;
    }

    if (first != null && !postOrderTraversal(first, callback, order)) {
      return false;
    }

    if (second != null && !postOrderTraversal(second, callback, order)) {
      return false;
    }

    return callback.process(node);
  }

  /**
   * Computes the maximum error of the current digest
   */
  public double getConfidenceFactor() {
    return computeMaxPathWeight(_root) * 1.0 / _weightedCount;
  }

  public boolean equivalent(QuantileDigest other) {
    rescaleToCommonLandmark(this, other);

    return (_totalNodeCount == other._totalNodeCount && _nonZeroNodeCount == other._nonZeroNodeCount
        && _min == other._min && _max == other._max && _weightedCount == other._weightedCount && Objects
        .equal(_root, other._root));
  }

  private void rescaleToCommonLandmark(QuantileDigest one, QuantileDigest two) {
    long nowInSeconds = TimeUnit.NANOSECONDS.toSeconds(_ticker.read());

    // 1. rescale this and other to common landmark
    long targetLandmark = Math.max(one._landmarkInSeconds, two._landmarkInSeconds);

    if (nowInSeconds - targetLandmark >= RESCALE_THRESHOLD_SECONDS) {
      targetLandmark = nowInSeconds;
    }

    if (targetLandmark != one._landmarkInSeconds) {
      one.rescale(targetLandmark);
    }

    if (targetLandmark != two._landmarkInSeconds) {
      two.rescale(targetLandmark);
    }
  }

  /**
   * Computes the max "weight" of any path starting at node and ending at a leaf in the
   * hypothetical complete tree. The weight is the sum of counts in the ancestors of a given node
   */
  private double computeMaxPathWeight(Node node) {
    if (node == null || node._level == 0) {
      return 0;
    }

    double leftMaxWeight = computeMaxPathWeight(node._left);
    double rightMaxWeight = computeMaxPathWeight(node._right);

    return Math.max(leftMaxWeight, rightMaxWeight) + node._weightedCount;
  }

  @VisibleForTesting
  void validate() {
    final AtomicDouble sumOfWeights = new AtomicDouble();
    final AtomicInteger actualNodeCount = new AtomicInteger();
    final AtomicInteger actualNonZeroNodeCount = new AtomicInteger();

    if (_root != null) {
      validateStructure(_root);

      postOrderTraversal(_root, new Callback() {
        @Override
        public boolean process(Node node) {
          sumOfWeights.addAndGet(node._weightedCount);
          actualNodeCount.incrementAndGet();

          if (node._weightedCount >= ZERO_WEIGHT_THRESHOLD) {
            actualNonZeroNodeCount.incrementAndGet();
          }

          return true;
        }
      });
    }

    checkState(Math.abs(sumOfWeights.get() - _weightedCount) < ZERO_WEIGHT_THRESHOLD,
        "Computed weight (%s) doesn't match summary (%s)", sumOfWeights.get(), _weightedCount);

    checkState(actualNodeCount.get() == _totalNodeCount, "Actual node count (%s) doesn't match summary (%s)",
        actualNodeCount.get(), _totalNodeCount);

    checkState(actualNonZeroNodeCount.get() == _nonZeroNodeCount,
        "Actual non-zero node count (%s) doesn't match summary (%s)", actualNonZeroNodeCount.get(), _nonZeroNodeCount);
  }

  private void validateStructure(Node node) {
    checkState(node._level >= 0);

    if (node._left != null) {
      validateBranchStructure(node, node._left, node._right, true);
      validateStructure(node._left);
    }

    if (node._right != null) {
      validateBranchStructure(node, node._right, node._left, false);
      validateStructure(node._right);
    }
  }

  private void validateBranchStructure(Node parent, Node child, Node otherChild, boolean isLeft) {
    checkState(child._level < parent._level, "Child level (%s) should be smaller than parent level (%s)", child._level,
        parent._level);

    long branch = child._bits & (1L << (parent._level - 1));
    checkState(branch == 0 && isLeft || branch != 0 && !isLeft, "Value of child node is inconsistent with its branch");

    checkState(parent._weightedCount >= ZERO_WEIGHT_THRESHOLD || child._weightedCount >= ZERO_WEIGHT_THRESHOLD
        || otherChild != null, "Found a linear chain of zero-weight nodes");
  }

  public String toGraphviz() {
    StringBuilder builder = new StringBuilder();

    builder.append("digraph QuantileDigest {\n").append("\tgraph [ordering=\"out\"];");

    final List<Node> nodes = new ArrayList<>();
    postOrderTraversal(_root, new Callback() {
      @Override
      public boolean process(Node node) {
        nodes.add(node);
        return true;
      }
    });

    Multimap<Integer, Node> nodesByLevel = Multimaps.index(nodes, new Function<Node, Integer>() {
      @Override
      public Integer apply(Node input) {
        return input._level;
      }
    });

    for (Map.Entry<Integer, Collection<Node>> entry : nodesByLevel.asMap().entrySet()) {
      builder.append("\tsubgraph level_" + entry.getKey() + " {\n").append("\t\trank = same;\n");

      for (Node node : entry.getValue()) {
        builder.append(String
            .format("\t\t%s [label=\"[%s..%s]@%s\\n%s\", shape=rect, style=filled,color=%s];\n", idFor(node),
                node.getLowerBound(), node.getUpperBound(), node._level, node._weightedCount,
                node._weightedCount > 0 ? "salmon2" : "white"));
      }

      builder.append("\t}\n");
    }

    for (Node node : nodes) {
      if (node._left != null) {
        builder.append(format("\t%s -> %s;\n", idFor(node), idFor(node._left)));
      }
      if (node._right != null) {
        builder.append(format("\t%s -> %s;\n", idFor(node), idFor(node._right)));
      }
    }

    builder.append("}\n");

    return builder.toString();
  }

  private static String idFor(Node node) {
    return String.format("node_%x_%x", node._bits, node._level);
  }

  /**
   * Convert a java long (two's complement representation) to a 64-bit lexicographically-sortable binary
   */
  private static long longToBits(long value) {
    return value ^ 0x8000_0000_0000_0000L;
  }

  /**
   * Convert a 64-bit lexicographically-sortable binary to a java long (two's complement representation)
   */
  private static long bitsToLong(long bits) {
    return bits ^ 0x8000_0000_0000_0000L;
  }

  public static class Bucket {
    private double _count;
    private double _mean;

    public Bucket(double count, double mean) {
      _count = count;
      _mean = mean;
    }

    public double getCount() {
      return _count;
    }

    public double getMean() {
      return _mean;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final Bucket bucket = (Bucket) o;

      if (Double.compare(bucket._count, _count) != 0) {
        return false;
      }
      if (Double.compare(bucket._mean, _mean) != 0) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result;
      long temp;
      temp = _count != +0.0d ? Double.doubleToLongBits(_count) : 0L;
      result = (int) (temp ^ (temp >>> 32));
      temp = _mean != +0.0d ? Double.doubleToLongBits(_mean) : 0L;
      result = 31 * result + (int) (temp ^ (temp >>> 32));
      return result;
    }

    @Override
    public String toString() {
      return String.format("[count: %f, mean: %f]", _count, _mean);
    }
  }

  private static class Node {
    private double _weightedCount;
    private int _level;
    private long _bits;
    private Node _left;
    private Node _right;

    private Node(long bits, int level, double weightedCount) {
      _bits = bits;
      _level = level;
      _weightedCount = weightedCount;
    }

    public boolean isLeaf() {
      return _left == null && _right == null;
    }

    public boolean hasSingleChild() {
      return _left == null && _right != null || _left != null && _right == null;
    }

    public Node getSingleChild() {
      checkState(hasSingleChild(), "Node does not have a single child");
      return firstNonNull(_left, _right);
    }

    public long getUpperBound() {
      // set all lsb below level to 1 (we're looking for the highest value of the range covered by this node)
      long mask = 0;

      if (_level > 0) { // need to special case when level == 0 because (value >> 64 really means value >> (64 % 64))
        mask = 0xFFFF_FFFF_FFFF_FFFFL >>> (MAX_BITS - _level);
      }
      return bitsToLong(_bits | mask);
    }

    public long getBranchMask() {
      return (1L << (_level - 1));
    }

    public long getLowerBound() {
      // set all lsb below level to 0 (we're looking for the lowest value of the range covered by this node)
      long mask = 0;

      if (_level > 0) { // need to special case when level == 0 because (value >> 64 really means value >> (64 % 64))
        mask = 0xFFFF_FFFF_FFFF_FFFFL >>> (MAX_BITS - _level);
      }

      return bitsToLong(_bits & (~mask));
    }

    public long getMiddle() {
      return getLowerBound() + (getUpperBound() - getLowerBound()) / 2;
    }

    @Override
    public String toString() {
      return format("%s (level = %d, count = %s, left = %s, right = %s)", _bits, _level, _weightedCount, _left != null,
          _right != null);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(_weightedCount, _level, _bits, _left, _right);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final Node other = (Node) obj;
      return Objects.equal(_weightedCount, other._weightedCount) && Objects.equal(_level, other._level) && Objects
          .equal(_bits, other._bits) && Objects.equal(_left, other._left) && Objects.equal(_right, other._right);
    }
  }

  private static interface Callback {
    /**
     * @param node the node to process
     * @return true if processing should continue
     */
    boolean process(Node node);
  }

  private static class Flags {
    public static final int HAS_LEFT = 1 << 0;
    public static final int HAS_RIGHT = 1 << 1;
  }

  // ----------------------------
  // additional methods
  // ----------------------------

  public static <T> T firstNonNull(T first, T second) {
    return first != null ? first : checkNotNull(second);
  }

  public void offer(long value) {
    add(value);
  }

  public static QuantileDigest merge(List<QuantileDigest> digests) {
    if (digests.isEmpty()) {
      throw new RuntimeException("Digests to be unioned should not be empty!");
    }

    QuantileDigest ret = digests.get(0);

    for (int i = 1; i < digests.size(); i++) {
      ret.merge(digests.get(i));
    }

    return ret;
  }
}
