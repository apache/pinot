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

  private final double maxError;
  private final Ticker ticker;
  private final double alpha;
  private final boolean compressAutomatically;

  private Node root;

  private double weightedCount;
  private long max = Long.MIN_VALUE;
  private long min = Long.MAX_VALUE;

  private long landmarkInSeconds;

  private int totalNodeCount = 0;
  private int nonZeroNodeCount = 0;
  private int compressions = 0;

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

    this.maxError = maxError;
    this.alpha = alpha;
    this.ticker = ticker;
    this.compressAutomatically = compressAutomatically;

    landmarkInSeconds = TimeUnit.NANOSECONDS.toSeconds(ticker.read());
  }

  public QuantileDigest(QuantileDigest quantileDigest) {
    this(quantileDigest.getMaxError(), quantileDigest.getAlpha());
    merge(quantileDigest);
  }

  public double getMaxError() {
    return maxError;
  }

  public double getAlpha() {
    return alpha;
  }

  public void add(long value) {
    add(value, 1);
  }

  /**
   * Adds a value to this digest. The value must be {@code >= 0}
   */
  public void add(long value, long count) {
    checkArgument(count > 0, "count must be > 0");

    long nowInSeconds = TimeUnit.NANOSECONDS.toSeconds(ticker.read());

    int maxExpectedNodeCount = 3 * calculateCompressionFactor();
    if (nowInSeconds - landmarkInSeconds >= RESCALE_THRESHOLD_SECONDS) {
      rescale(nowInSeconds);
      compress(); // need to compress to get rid of nodes that may have decayed to ~ 0
    } else if (nonZeroNodeCount > MAX_SIZE_FACTOR * maxExpectedNodeCount && compressAutomatically) {
      // The size (number of non-zero nodes) of the digest is at most 3 * compression factor
      // If we're over MAX_SIZE_FACTOR of the expected size, compress
      // Note: we don't compress as soon as we go over expectedNodeCount to avoid unnecessarily
      // running a compression for every new added element when we're close to boundary
      compress();
    }

    double weight = weight(TimeUnit.NANOSECONDS.toSeconds(ticker.read())) * count;

    max = Math.max(max, value);
    min = Math.min(min, value);

    insert(longToBits(value), weight);
  }

  public void merge(QuantileDigest other) {
    rescaleToCommonLandmark(this, other);

    // 2. merge other into this (don't modify other)
    root = merge(root, other.root);

    max = Math.max(max, other.max);
    min = Math.min(min, other.min);

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

    postOrderTraversal(root, new Callback() {
      private double sum = 0;

      @Override
      public boolean process(Node node) {
        sum += node.weightedCount;

        while (iterator.hasNext() && sum > iterator.peek() * weightedCount) {
          iterator.next();

          // we know the max value ever seen, so cap the percentile to provide better error
          // bounds in this case
          long value = Math.min(node.getUpperBound(), max);

          builder.add(value);
        }

        return iterator.hasNext();
      }
    });

    // we finished the traversal without consuming all quantiles. This means the remaining quantiles
    // correspond to the max known value
    while (iterator.hasNext()) {
      builder.add(max);
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
    return weightedCount / weight(TimeUnit.NANOSECONDS.toSeconds(ticker.read()));
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

    final double normalizationFactor = weight(TimeUnit.NANOSECONDS.toSeconds(ticker.read()));

    postOrderTraversal(root, new Callback() {
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

        bucketWeightedSum.addAndGet(node.getMiddle() * node.weightedCount);
        sum.addAndGet(node.weightedCount);
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
    final AtomicLong chosen = new AtomicLong(min);
    postOrderTraversal(root, new Callback() {
      @Override
      public boolean process(Node node) {
        if (node.weightedCount >= ZERO_WEIGHT_THRESHOLD) {
          chosen.set(node.getLowerBound());
          return false;
        }
        return true;
      }
    }, TraversalOrder.FORWARD);

    return Math.max(min, chosen.get());
  }

  public long getMax() {
    final AtomicLong chosen = new AtomicLong(max);
    postOrderTraversal(root, new Callback() {
      @Override
      public boolean process(Node node) {
        if (node.weightedCount >= ZERO_WEIGHT_THRESHOLD) {
          chosen.set(node.getUpperBound());
          return false;
        }
        return true;
      }
    }, TraversalOrder.REVERSE);

    return Math.min(max, chosen.get());
  }

  public int getByteSize() {
    return HEADER_BYTE_SIZE + totalNodeCount * NODE_BYTE_SIZE;
  }

  public byte[] toBytes() {
    byte[] bytes = new byte[getByteSize()];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byteBuffer.putDouble(maxError);
    byteBuffer.putDouble(alpha);
    byteBuffer.putLong(landmarkInSeconds);
    byteBuffer.putLong(min);
    byteBuffer.putLong(max);
    byteBuffer.putInt(totalNodeCount);
    postOrderTraversal(root, node -> {
      serializeNode(byteBuffer, node);
      return true;
    });
    return bytes;
  }

  private void serializeNode(ByteBuffer byteBuffer, Node node) {
    byte flags = 0;
    if (node.left != null) {
      flags |= Flags.HAS_LEFT;
    }
    if (node.right != null) {
      flags |= Flags.HAS_RIGHT;
    }

    byteBuffer.put(flags);
    byteBuffer.put((byte) node.level);
    byteBuffer.putLong(node.bits);
    byteBuffer.putDouble(node.weightedCount);
  }

  public static QuantileDigest fromBytes(byte[] bytes) {
    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  public static QuantileDigest fromByteBuffer(ByteBuffer byteBuffer) {
    double maxError = byteBuffer.getDouble();
    double alpha = byteBuffer.getDouble();

    QuantileDigest quantileDigest = new QuantileDigest(maxError, alpha);
    quantileDigest.landmarkInSeconds = byteBuffer.getLong();
    quantileDigest.min = byteBuffer.getLong();
    quantileDigest.max = byteBuffer.getLong();
    int numNodes = byteBuffer.getInt();
    quantileDigest.totalNodeCount = numNodes;
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
        node.right = stack.pop();
      }
      if ((flags & Flags.HAS_LEFT) != 0) {
        node.left = stack.pop();
      }
      stack.push(node);

      quantileDigest.weightedCount += weightedCount;
      if (node.weightedCount >= ZERO_WEIGHT_THRESHOLD) {
        quantileDigest.nonZeroNodeCount++;
      }
    }

    checkState(stack.size() == 1, "Tree is corrupted, expecting a single root node");
    quantileDigest.root = stack.pop();

    return quantileDigest;
  }

  @VisibleForTesting
  int getTotalNodeCount() {
    return totalNodeCount;
  }

  @VisibleForTesting
  int getNonZeroNodeCount() {
    return nonZeroNodeCount;
  }

  @VisibleForTesting
  int getCompressions() {
    return compressions;
  }

  @VisibleForTesting
  void compress() {
    ++compressions;

    final int compressionFactor = calculateCompressionFactor();

    postOrderTraversal(root, new Callback() {
      @Override
      public boolean process(Node node) {
        if (node.isLeaf()) {
          return true;
        }

        // if children's weights are ~0 remove them and shift the weight to their parent

        double leftWeight = 0;
        if (node.left != null) {
          leftWeight = node.left.weightedCount;
        }

        double rightWeight = 0;
        if (node.right != null) {
          rightWeight = node.right.weightedCount;
        }

        boolean shouldCompress =
            node.weightedCount + leftWeight + rightWeight < (int) (weightedCount / compressionFactor);

        double oldNodeWeight = node.weightedCount;
        if (shouldCompress || leftWeight < ZERO_WEIGHT_THRESHOLD) {
          node.left = tryRemove(node.left);

          weightedCount += leftWeight;
          node.weightedCount += leftWeight;
        }

        if (shouldCompress || rightWeight < ZERO_WEIGHT_THRESHOLD) {
          node.right = tryRemove(node.right);

          weightedCount += rightWeight;
          node.weightedCount += rightWeight;
        }

        if (oldNodeWeight < ZERO_WEIGHT_THRESHOLD && node.weightedCount >= ZERO_WEIGHT_THRESHOLD) {
          ++nonZeroNodeCount;
        }

        return true;
      }
    });

    if (root != null && root.weightedCount < ZERO_WEIGHT_THRESHOLD) {
      root = tryRemove(root);
    }
  }

  private double weight(long timestamp) {
    return Math.exp(alpha * (timestamp - landmarkInSeconds));
  }

  private void rescale(long newLandmarkInSeconds) {
    // rescale the weights based on a new landmark to avoid numerical overflow issues

    final double factor = Math.exp(-alpha * (newLandmarkInSeconds - landmarkInSeconds));

    weightedCount *= factor;

    postOrderTraversal(root, new Callback() {
      @Override
      public boolean process(Node node) {
        double oldWeight = node.weightedCount;

        node.weightedCount *= factor;

        if (oldWeight >= ZERO_WEIGHT_THRESHOLD && node.weightedCount < ZERO_WEIGHT_THRESHOLD) {
          --nonZeroNodeCount;
        }

        return true;
      }
    });

    landmarkInSeconds = newLandmarkInSeconds;
  }

  private int calculateCompressionFactor() {
    if (root == null) {
      return 1;
    }

    return Math.max((int) ((root.level + 1) / maxError), 1);
  }

  private void insert(long bits, double weight) {
    long lastBranch = 0;
    Node parent = null;
    Node current = root;

    while (true) {
      if (current == null) {
        setChild(parent, lastBranch, createLeaf(bits, weight));
        return;
      } else if (!inSameSubtree(bits, current.bits, current.level)) {
        // if bits and node.bits are not in the same branch given node's level,
        // insert a parent above them at the point at which branches diverge
        setChild(parent, lastBranch, makeSiblings(current, createLeaf(bits, weight)));
        return;
      } else if (current.level == 0 && current.bits == bits) {
        // found the node

        double oldWeight = current.weightedCount;

        current.weightedCount += weight;

        if (current.weightedCount >= ZERO_WEIGHT_THRESHOLD && oldWeight < ZERO_WEIGHT_THRESHOLD) {
          ++nonZeroNodeCount;
        }

        weightedCount += weight;

        return;
      }

      // we're on the correct branch of the tree and we haven't reached a leaf, so keep going down
      long branch = bits & current.getBranchMask();

      parent = current;
      lastBranch = branch;

      if (branch == 0) {
        current = current.left;
      } else {
        current = current.right;
      }
    }
  }

  private void setChild(Node parent, long branch, Node child) {
    if (parent == null) {
      root = child;
    } else if (branch == 0) {
      parent.left = child;
    } else {
      parent.right = child;
    }
  }

  private Node makeSiblings(Node node, Node sibling) {
    int parentLevel = MAX_BITS - Long.numberOfLeadingZeros(node.bits ^ sibling.bits);

    Node parent = createNode(node.bits, parentLevel, 0);

    // the branch is given by the bit at the level one below parent
    long branch = sibling.bits & parent.getBranchMask();
    if (branch == 0) {
      parent.left = sibling;
      parent.right = node;
    } else {
      parent.left = node;
      parent.right = sibling;
    }

    return parent;
  }

  private Node createLeaf(long bits, double weight) {
    return createNode(bits, 0, weight);
  }

  private Node createNode(long bits, int level, double weight) {
    weightedCount += weight;
    ++totalNodeCount;
    if (weight >= ZERO_WEIGHT_THRESHOLD) {
      nonZeroNodeCount++;
    }
    return new Node(bits, level, weight);
  }

  private Node merge(Node node, Node other) {
    if (node == null) {
      return copyRecursive(other);
    } else if (other == null) {
      return node;
    } else if (!inSameSubtree(node.bits, other.bits, Math.max(node.level, other.level))) {
      return makeSiblings(node, copyRecursive(other));
    } else if (node.level > other.level) {
      long branch = other.bits & node.getBranchMask();

      if (branch == 0) {
        node.left = merge(node.left, other);
      } else {
        node.right = merge(node.right, other);
      }
      return node;
    } else if (node.level < other.level) {
      Node result = createNode(other.bits, other.level, other.weightedCount);

      long branch = node.bits & other.getBranchMask();
      if (branch == 0) {
        result.left = merge(node, other.left);
        result.right = copyRecursive(other.right);
      } else {
        result.left = copyRecursive(other.left);
        result.right = merge(node, other.right);
      }

      return result;
    }

    // else, they must be at the same level and on the same path, so just bump the counts
    double oldWeight = node.weightedCount;

    weightedCount += other.weightedCount;
    node.weightedCount = node.weightedCount + other.weightedCount;
    node.left = merge(node.left, other.left);
    node.right = merge(node.right, other.right);

    if (oldWeight < ZERO_WEIGHT_THRESHOLD && node.weightedCount >= ZERO_WEIGHT_THRESHOLD) {
      nonZeroNodeCount++;
    }

    return node;
  }

  private static boolean inSameSubtree(long bitsA, long bitsB, int level) {
    return level == MAX_BITS || (bitsA >>> level) == (bitsB >>> level);
  }

  private Node copyRecursive(Node node) {
    Node result = null;

    if (node != null) {
      result = createNode(node.bits, node.level, node.weightedCount);
      result.left = copyRecursive(node.left);
      result.right = copyRecursive(node.right);
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

    if (node.weightedCount >= ZERO_WEIGHT_THRESHOLD) {
      --nonZeroNodeCount;
    }

    weightedCount -= node.weightedCount;

    Node result = null;
    if (node.isLeaf()) {
      --totalNodeCount;
    } else if (node.hasSingleChild()) {
      result = node.getSingleChild();
      --totalNodeCount;
    } else {
      node.weightedCount = 0;
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
      first = node.left;
      second = node.right;
    } else {
      first = node.right;
      second = node.left;
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
    return computeMaxPathWeight(root) * 1.0 / weightedCount;
  }

  public boolean equivalent(QuantileDigest other) {
    rescaleToCommonLandmark(this, other);

    return (totalNodeCount == other.totalNodeCount && nonZeroNodeCount == other.nonZeroNodeCount && min == other.min
        && max == other.max && weightedCount == other.weightedCount && Objects.equal(root, other.root));
  }

  private void rescaleToCommonLandmark(QuantileDigest one, QuantileDigest two) {
    long nowInSeconds = TimeUnit.NANOSECONDS.toSeconds(ticker.read());

    // 1. rescale this and other to common landmark
    long targetLandmark = Math.max(one.landmarkInSeconds, two.landmarkInSeconds);

    if (nowInSeconds - targetLandmark >= RESCALE_THRESHOLD_SECONDS) {
      targetLandmark = nowInSeconds;
    }

    if (targetLandmark != one.landmarkInSeconds) {
      one.rescale(targetLandmark);
    }

    if (targetLandmark != two.landmarkInSeconds) {
      two.rescale(targetLandmark);
    }
  }

  /**
   * Computes the max "weight" of any path starting at node and ending at a leaf in the
   * hypothetical complete tree. The weight is the sum of counts in the ancestors of a given node
   */
  private double computeMaxPathWeight(Node node) {
    if (node == null || node.level == 0) {
      return 0;
    }

    double leftMaxWeight = computeMaxPathWeight(node.left);
    double rightMaxWeight = computeMaxPathWeight(node.right);

    return Math.max(leftMaxWeight, rightMaxWeight) + node.weightedCount;
  }

  @VisibleForTesting
  void validate() {
    final AtomicDouble sumOfWeights = new AtomicDouble();
    final AtomicInteger actualNodeCount = new AtomicInteger();
    final AtomicInteger actualNonZeroNodeCount = new AtomicInteger();

    if (root != null) {
      validateStructure(root);

      postOrderTraversal(root, new Callback() {
        @Override
        public boolean process(Node node) {
          sumOfWeights.addAndGet(node.weightedCount);
          actualNodeCount.incrementAndGet();

          if (node.weightedCount >= ZERO_WEIGHT_THRESHOLD) {
            actualNonZeroNodeCount.incrementAndGet();
          }

          return true;
        }
      });
    }

    checkState(Math.abs(sumOfWeights.get() - weightedCount) < ZERO_WEIGHT_THRESHOLD,
        "Computed weight (%s) doesn't match summary (%s)", sumOfWeights.get(), weightedCount);

    checkState(actualNodeCount.get() == totalNodeCount, "Actual node count (%s) doesn't match summary (%s)",
        actualNodeCount.get(), totalNodeCount);

    checkState(actualNonZeroNodeCount.get() == nonZeroNodeCount,
        "Actual non-zero node count (%s) doesn't match summary (%s)", actualNonZeroNodeCount.get(), nonZeroNodeCount);
  }

  private void validateStructure(Node node) {
    checkState(node.level >= 0);

    if (node.left != null) {
      validateBranchStructure(node, node.left, node.right, true);
      validateStructure(node.left);
    }

    if (node.right != null) {
      validateBranchStructure(node, node.right, node.left, false);
      validateStructure(node.right);
    }
  }

  private void validateBranchStructure(Node parent, Node child, Node otherChild, boolean isLeft) {
    checkState(child.level < parent.level, "Child level (%s) should be smaller than parent level (%s)", child.level,
        parent.level);

    long branch = child.bits & (1L << (parent.level - 1));
    checkState(branch == 0 && isLeft || branch != 0 && !isLeft, "Value of child node is inconsistent with its branch");

    checkState(parent.weightedCount >= ZERO_WEIGHT_THRESHOLD || child.weightedCount >= ZERO_WEIGHT_THRESHOLD
        || otherChild != null, "Found a linear chain of zero-weight nodes");
  }

  public String toGraphviz() {
    StringBuilder builder = new StringBuilder();

    builder.append("digraph QuantileDigest {\n").append("\tgraph [ordering=\"out\"];");

    final List<Node> nodes = new ArrayList<>();
    postOrderTraversal(root, new Callback() {
      @Override
      public boolean process(Node node) {
        nodes.add(node);
        return true;
      }
    });

    Multimap<Integer, Node> nodesByLevel = Multimaps.index(nodes, new Function<Node, Integer>() {
      @Override
      public Integer apply(Node input) {
        return input.level;
      }
    });

    for (Map.Entry<Integer, Collection<Node>> entry : nodesByLevel.asMap().entrySet()) {
      builder.append("\tsubgraph level_" + entry.getKey() + " {\n").append("\t\trank = same;\n");

      for (Node node : entry.getValue()) {
        builder.append(String
            .format("\t\t%s [label=\"[%s..%s]@%s\\n%s\", shape=rect, style=filled,color=%s];\n", idFor(node),
                node.getLowerBound(), node.getUpperBound(), node.level, node.weightedCount,
                node.weightedCount > 0 ? "salmon2" : "white"));
      }

      builder.append("\t}\n");
    }

    for (Node node : nodes) {
      if (node.left != null) {
        builder.append(format("\t%s -> %s;\n", idFor(node), idFor(node.left)));
      }
      if (node.right != null) {
        builder.append(format("\t%s -> %s;\n", idFor(node), idFor(node.right)));
      }
    }

    builder.append("}\n");

    return builder.toString();
  }

  private static String idFor(Node node) {
    return String.format("node_%x_%x", node.bits, node.level);
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
    private double count;
    private double mean;

    public Bucket(double count, double mean) {
      this.count = count;
      this.mean = mean;
    }

    public double getCount() {
      return count;
    }

    public double getMean() {
      return mean;
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

      if (Double.compare(bucket.count, count) != 0) {
        return false;
      }
      if (Double.compare(bucket.mean, mean) != 0) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result;
      long temp;
      temp = count != +0.0d ? Double.doubleToLongBits(count) : 0L;
      result = (int) (temp ^ (temp >>> 32));
      temp = mean != +0.0d ? Double.doubleToLongBits(mean) : 0L;
      result = 31 * result + (int) (temp ^ (temp >>> 32));
      return result;
    }

    @Override
    public String toString() {
      return String.format("[count: %f, mean: %f]", count, mean);
    }
  }

  private static class Node {
    private double weightedCount;
    private int level;
    private long bits;
    private Node left;
    private Node right;

    private Node(long bits, int level, double weightedCount) {
      this.bits = bits;
      this.level = level;
      this.weightedCount = weightedCount;
    }

    public boolean isLeaf() {
      return left == null && right == null;
    }

    public boolean hasSingleChild() {
      return left == null && right != null || left != null && right == null;
    }

    public Node getSingleChild() {
      checkState(hasSingleChild(), "Node does not have a single child");
      return firstNonNull(left, right);
    }

    public long getUpperBound() {
      // set all lsb below level to 1 (we're looking for the highest value of the range covered by this node)
      long mask = 0;

      if (level > 0) { // need to special case when level == 0 because (value >> 64 really means value >> (64 % 64))
        mask = 0xFFFF_FFFF_FFFF_FFFFL >>> (MAX_BITS - level);
      }
      return bitsToLong(bits | mask);
    }

    public long getBranchMask() {
      return (1L << (level - 1));
    }

    public long getLowerBound() {
      // set all lsb below level to 0 (we're looking for the lowest value of the range covered by this node)
      long mask = 0;

      if (level > 0) { // need to special case when level == 0 because (value >> 64 really means value >> (64 % 64))
        mask = 0xFFFF_FFFF_FFFF_FFFFL >>> (MAX_BITS - level);
      }

      return bitsToLong(bits & (~mask));
    }

    public long getMiddle() {
      return getLowerBound() + (getUpperBound() - getLowerBound()) / 2;
    }

    @Override
    public String toString() {
      return format("%s (level = %d, count = %s, left = %s, right = %s)", bits, level, weightedCount, left != null,
          right != null);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(weightedCount, level, bits, left, right);
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
      return Objects.equal(this.weightedCount, other.weightedCount) && Objects.equal(this.level, other.level) && Objects
          .equal(this.bits, other.bits) && Objects.equal(this.left, other.left) && Objects
          .equal(this.right, other.right);
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
