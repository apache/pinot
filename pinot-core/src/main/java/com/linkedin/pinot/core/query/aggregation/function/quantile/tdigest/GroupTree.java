/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.aggregation.function.quantile.tdigest;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

import com.clearspring.analytics.util.AbstractIterator;
import com.clearspring.analytics.util.Preconditions;

/**
 * We copy the original class here to implement Serializable interface,
 * and resolve multiple access controls (including some bug fixes).
 *
 * // original comments below
 *
 * A tree containing TDigest.Group.  This adds to the normal NavigableSet the
 * ability to sum up the size of elements to the left of a particular group.
 */
public class GroupTree implements Iterable<TDigest.Group>, Serializable {

    private int count;
    private int size;
    private int depth;
    public TDigest.Group leaf;
    public GroupTree left, right;

    public GroupTree() {
        count = size = depth = 0;
        leaf = null;
        left = right = null;
    }

    public GroupTree(TDigest.Group leaf) {
        size = depth = 1;
        this.leaf = leaf;
        count = leaf.count();
        left = right = null;
    }

    public GroupTree(GroupTree left, GroupTree right) {
        this.left = left;
        this.right = right;
        count = left.count + right.count;
        size = left.size + right.size;
        rebalance();
        leaf = this.right.first();
    }

    public void add(TDigest.Group group) {
        if (size == 0) {
            leaf = group;
            depth = 1;
            count = group.count();
            size = 1;
            return;
        } else if (size == 1) {
            int order = group.compareTo(leaf);
            if (order < 0) {
                left = new GroupTree(group);
                right = new GroupTree(leaf);
            } else if (order > 0) {
                left = new GroupTree(leaf);
                right = new GroupTree(group);
                leaf = group;
            }
        } else if (group.compareTo(leaf) < 0) {
            left.add(group);
        } else {
            right.add(group);
        }
        count += group.count();
        size++;
        depth = Math.max(left.depth, right.depth) + 1;

        rebalance();
    }

    private void rebalance() {
        int l = left.depth();
        int r = right.depth();
        if (l > r + 1) {
            if (left.left.depth() > left.right.depth()) {
                rotate(left.left.left, left.left.right, left.right, right);
            } else {
                rotate(left.left, left.right.left, left.right.right, right);
            }
        } else if (r > l + 1) {
            if (right.left.depth() > right.right.depth()) {
                rotate(left, right.left.left, right.left.right, right.right);
            } else {
                rotate(left, right.left, right.right.left, right.right.right);
            }
        } else {
            depth = Math.max(left.depth(), right.depth()) + 1;
        }
    }

    private void rotate(GroupTree a, GroupTree b, GroupTree c, GroupTree d) {
        left = new GroupTree(a, b);
        right = new GroupTree(c, d);
        count = left.count + right.count;
        size = left.size + right.size;
        depth = Math.max(left.depth(), right.depth()) + 1;
        leaf = right.first();
    }

    private int depth() {
        return depth;
    }

    public int size() {
        return size;
    }

    /**
     * @return the number of items strictly before the current element
     */
    public int headCount(TDigest.Group base) {
        if (size == 0) {
            return 0;
        } else if (left == null) {
            return leaf.compareTo(base) < 0 ? 1 : 0;
        } else {
            if (base.compareTo(leaf) < 0) {
                return left.headCount(base);
            } else {
                return left.size + right.headCount(base);
            }
        }
    }

    /**
     * @return the sum of the size() function for all elements strictly before the current element.
     */
    public int headSum(TDigest.Group base) {
        if (size == 0) {
            return 0;
        } else if (left == null) {
            return leaf.compareTo(base) < 0 ? count : 0;
        } else {
            if (base.compareTo(leaf) <= 0) {
                return left.headSum(base);
            } else {
                return left.count + right.headSum(base);
            }
        }
    }

    /**
     * @return the first Group in this set
     */
    public TDigest.Group first() {
        Preconditions.checkState(size > 0, "No first element of empty set");
        if (left == null) {
            return leaf;
        } else {
            return left.first();
        }
    }

    /**
     * Iteratres through all groups in the tree.
     */
    public Iterator<TDigest.Group> iterator() {
        return iterator(null);
    }

    /**
     * Iterates through all of the Groups in this tree in ascending order of means
     *
     * @param start The place to start this subset.  Remember that Groups are ordered by mean *and* id.
     * @return An iterator that goes through the groups in order of mean and id starting at or after the
     * specified Group.
     */
    private Iterator<TDigest.Group> iterator(final TDigest.Group start) {
        return new AbstractIterator<TDigest.Group>() {
            {
                stack = new ArrayDeque<GroupTree>();
                push(GroupTree.this, start);
            }

            Deque<GroupTree> stack;

            // recurses down to the leaf that is >= start
            // pending right hand branches on the way are put on the stack
            private void push(GroupTree z, TDigest.Group start) {
                while (z.left != null) {
                    if (start == null || start.compareTo(z.leaf) < 0) {
                        // remember we will have to process the right hand branch later
                        stack.push(z.right);
                        // note that there is no guarantee that z.left has any good data
                        z = z.left;
                    } else {
                        // if the left hand branch doesn't contain start, then no push
                        z = z.right;
                    }
                }
                // put the leaf value on the stack if it is valid
                if (start == null || z.leaf.compareTo(start) >= 0) {
                    stack.push(z);
                }
            }

            @Override
            protected TDigest.Group computeNext() {
                GroupTree r = stack.poll();
                while (r != null && r.left != null) {
                    // unpack r onto the stack
                    push(r, start);
                    r = stack.poll();
                }

                // at this point, r == null or r.left == null
                // if r == null, stack is empty and we are done
                // if r != null, then r.left != null and we have a result
                if (r != null) {
                    return r.leaf;
                }
                return endOfData();
            }
        };
    }

    public void remove(TDigest.Group base) {
        Preconditions.checkState(size > 0, "Cannot remove from empty set");
        if (size == 1) {
            Preconditions.checkArgument(base.compareTo(leaf) == 0, "Element %s not found", base);
            count = size = 0;
            leaf = null;
        } else {
            if (base.compareTo(leaf) < 0) {
                if (left.size > 1) {
                    left.remove(base);
                    count -= base.count();
                    size--;
                    rebalance();
                } else {
                    size = right.size;
                    count = right.count;
                    depth = right.depth;
                    leaf = right.leaf;
                    left = right.left;
                    right = right.right;
                }
            } else {
                if (right.size > 1) {
                    right.remove(base);
                    leaf = right.first();
                    count -= base.count();
                    size--;
                    rebalance();
                } else {
                    size = left.size;
                    count = left.count;
                    depth = left.depth;
                    leaf = left.leaf;
                    right = left.right;
                    left = left.left;
                }
            }
        }
    }

    /**
     * @return the largest element less than or equal to base
     */
    public TDigest.Group floor(TDigest.Group base) {
        if (size == 0) {
            return null;
        } else {
            if (size == 1) {
                return base.compareTo(leaf) >= 0 ? leaf : null;
            } else {
                if (base.compareTo(leaf) < 0) {
                    return left.floor(base);
                } else {
                    TDigest.Group floor = right.floor(base);
                    if (floor == null) {
                        floor = left.last();
                    }
                    return floor;
                }
            }
        }
    }

    public TDigest.Group last() {
        Preconditions.checkState(size > 0, "Cannot find last element of empty set");
        if (size == 1) {
            return leaf;
        } else {
            return right.last();
        }
    }

    /**
     * @return the smallest element greater than or equal to base.
     */
    public TDigest.Group ceiling(TDigest.Group base) {
        if (size == 0) {
            return null;
        } else if (size == 1) {
            return base.compareTo(leaf) <= 0 ? leaf : null;
        } else {
            if (base.compareTo(leaf) < 0) {
                TDigest.Group r = left.ceiling(base);
                if (r == null) {
                    r = right.first();
                }
                return r;
            } else {
                return right.ceiling(base);
            }
        }
    }

    /**
     * @return the subset of elements equal to or greater than base.
     */
    public Iterable<TDigest.Group> tailSet(final TDigest.Group start) {
        return new Iterable<TDigest.Group>() {
            @Override
            public Iterator<TDigest.Group> iterator() {
                return GroupTree.this.iterator(start);
            }
        };
    }

    public int sum() {
        return count;
    }

    public void checkBalance() {
        if (left != null) {
            Preconditions.checkState(Math.abs(left.depth() - right.depth()) < 2, "Imbalanced");
            int l = left.depth();
            int r = right.depth();
            Preconditions.checkState(depth == Math.max(l, r) + 1, "Depth doesn't match children");
            Preconditions.checkState(size == left.size + right.size, "Sizes don't match children");
            Preconditions.checkState(count == left.count + right.count, "Counts don't match children");
            Preconditions.checkState(leaf.compareTo(right.first()) == 0, "Split is wrong %.5d != %.5d or %d != %d", leaf.mean(), right.first().mean(), leaf.id(), right.first().id());
            left.checkBalance();
            right.checkBalance();
        }
    }

    public void print(int depth) {
        for (int i = 0; i < depth; i++) {
            System.out.printf("| ");
        }
        int imbalance = Math.abs((left != null ? left.depth : 1) - (right != null ? right.depth : 1));
        System.out.printf("%s%s, %d, %d, %d\n", (imbalance > 1 ? "* " : "") + (right != null && leaf.compareTo(right.first()) != 0 ? "+ " : ""), leaf, size, count, this.depth);
        if (left != null) {
            left.print(depth + 1);
            right.print(depth + 1);
        }
    }
}
