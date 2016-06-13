/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.transport.common;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * Taken from R2's LinkedDeque implementation to be used in Pool implementation. The class is copied as the corresponding R2 jar pulls in a lot of unnecessary jars.
 *
 * This class provides the ability to remove an arbitrary interior element (neither head nor tail)
 * from the queue in O(1) time, by invoking #removeNode(com.linkedin.r2.util.LinkedDeque.Node).
 * No class in the Java Collections framework provides this ability.
 *
 * Adding to and removing from head or tail also run in O(1) time, as expected.
 *
 * External synchronization is required!
 *
 * @param <T> Resource to be managed by queue
 */
public class LinkedDequeue<T> extends AbstractQueue<T> implements Deque<T> {
  public static class Node<T> {
    private final T _value;
    private Node<T> _next;
    private Node<T> _prev;

    private Node(T value) {
      _value = value;
    }
  }

  private Node<T> _head;
  private Node<T> _tail;
  private int _size;

  /**
   * Construct a new instance.
   */
  public LinkedDequeue() {
    super();
  }

  /**
   * Construct a new instance, adding all objects in the specified collection.
   *
   * @param collection a {@link Collection} of objects to be added.
   */
  public LinkedDequeue(Collection<? extends T> collection) {
    super();
    addAll(collection);
  }

  /**
   * Add a new item at the head of the queue.
   *
   * @param item the item to be added.
   * @return the {@link Node} of the newly added item.
   */
  public Node<T> addFirstNode(T item) {
    return addBeforeNode(_head, item);
  }

  /**
   * Add a new item at the tail of the queue.
   *
   * @param item the item to be added.
   * @return the {@link Node} of the newly added item.
   */
  public Node<T> addLastNode(T item) {
    return addBeforeNode(null, item);
  }

  /**
   * Add a new item before the specified Node.
   *
   * @param before the {@link Node} before which the item should be added.
   * @param item the item to be added.
   * @return the {@link Node} of the newly added item.
   */
  public Node<T> addBeforeNode(Node<T> before, T item) {
    if (item == null) {
      throw new NullPointerException();
    }
    if (before != null && before != _head && before._next == null && before._prev == null) {
      throw new IllegalStateException("node was already removed");
    }
    Node<T> node = new Node<T>(item);
    if (before == null) {
      // Adding to tail
      node._next = null;
      node._prev = _tail;
      if (_tail != null) {
        _tail._next = node;
      }
      _tail = node;
      if (_head == null) {
        _head = node;
      }
    } else {
      node._next = before;
      node._prev = before._prev;
      before._prev = node;
      if (before == _head) {
        _head = node;
      }
    }
    _size++;
    return node;
  }

  /**
   * Remove the specified Node from the queue.
   *
   * @param node the Node to be removed.
   * @return the item contained in the Node which was removed.
   */
  public T removeNode(Node<T> node) {
    // TODO what if the node is from the wrong list??
    if (node != _head && node._next == null && node._prev == null) {
      // the node has already been removed
      return null;
    }
    if (node == _head) {
      _head = node._next;
    }
    if (node._prev != null) {
      node._prev._next = node._next;
    }
    if (node == _tail) {
      _tail = node._prev;
    }
    if (node._next != null) {
      node._next._prev = node._prev;
    }
    node._next = null;
    node._prev = null;
    _size--;
    return node._value;
  }

  // Remaining methods implement Deque<T>

  @Override
  public boolean offerFirst(T t) {
    addFirstNode(t);
    return true;
  }

  @Override
  public boolean offerLast(T t) {
    addLastNode(t);
    return true;
  }

  @Override
  public T peekFirst() {
    if (_head == null) {
      return null;
    }
    return _head._value;
  }

  @Override
  public T peekLast() {
    if (_tail == null) {
      return null;
    }
    return _tail._value;
  }

  @Override
  public T pollFirst() {
    if (_head == null) {
      return null;
    }
    return removeNode(_head);
  }

  @Override
  public T pollLast() {
    if (_tail == null) {
      return null;
    }
    return removeNode(_tail);
  }

  @Override
  public int size() {
    return _size;
  }

  @Override
  public Iterator<T> iterator() {
    return new LinkedQueueIterator(Direction.ASCENDING);
  }

  @Override
  public Iterator<T> descendingIterator() {
    return new LinkedQueueIterator(Direction.DESCENDING);
  }

  private enum Direction {
    ASCENDING,
    DESCENDING
  }

  private class LinkedQueueIterator implements Iterator<T> {
    private final Direction _dir;
    private Node<T> _index;
    private Node<T> _last;

    private LinkedQueueIterator(Direction dir) {
      _dir = dir;
      _index = _dir == Direction.ASCENDING ? _head : _tail;
    }

    @Override
    public boolean hasNext() {
      return _index != null;
    }

    @Override
    public T next() {
      if (_index == null) {
        throw new NoSuchElementException();
      }
      _last = _index;
      T value = _index._value;
      _index = _dir == Direction.ASCENDING ? _index._next : _index._prev;
      return value;
    }

    @Override
    public void remove() {
      if (_last == null) {
        throw new IllegalStateException();
      }
      removeNode(_last);
      _last = null;
    }
  }

  @Override
  public void addFirst(T e) {
    if (!offerFirst(e)) {
      throw new IllegalStateException("Queue full");
    }
  }

  @Override
  public void addLast(T e) {
    if (!offerLast(e)) {
      throw new IllegalStateException("Queue full");
    }
  }

  @Override
  public T removeFirst() {
    T e = pollFirst();
    if (e == null) {
      throw new NoSuchElementException();
    }
    return e;
  }

  @Override
  public T removeLast() {
    T e = pollLast();
    if (e == null) {
      throw new NoSuchElementException();
    }
    return e;
  }

  @Override
  public T getFirst() {
    T e = peekFirst();
    if (e == null) {
      throw new NoSuchElementException();
    }
    return e;
  }

  @Override
  public T getLast() {
    T e = peekLast();
    if (e == null) {
      throw new NoSuchElementException();
    }
    return e;
  }

  @Override
  public boolean removeFirstOccurrence(Object o) {
    for (Iterator<T> i = iterator(); i.hasNext();) {
      if (i.next().equals(o)) {
        i.remove();
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean removeLastOccurrence(Object o) {
    for (Iterator<T> i = descendingIterator(); i.hasNext();) {
      if (i.next().equals(o)) {
        i.remove();
        return true;
      }
    }
    return false;
  }

  @Override
  public void push(T e) {
    addFirst(e);
  }

  @Override
  public T pop() {
    return removeFirst();
  }

  // java.util.Queue methods
  @Override
  public boolean offer(T e) {
    return offerLast(e);
  }

  @Override
  public T poll() {
    return pollFirst();
  }

  @Override
  public T peek() {
    return peekFirst();
  }
}
