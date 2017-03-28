package com.linkedin.thirdeye.dataframe;

public abstract class TypedSeries<T extends Series> extends Series {
  @Override
  public T head(int n) {
    return (T)super.head(n);
  }

  @Override
  public T tail(int n) {
    return (T)super.tail(n);
  }

  @Override
  public T sliceFrom(int from) {
    return (T)super.sliceFrom(from);
  }

  @Override
  public T sliceTo(int to) {
    return (T)super.sliceTo(to);
  }

  @Override
  public T reverse() {
    return (T)super.reverse();
  }

  @Override
  public T sorted() {
    return (T)super.sorted();
  }

  @Override
  public T unique() {
    return (T)super.unique();
  }

  @Override
  public T shift(int offset) {
    return (T)super.shift(offset);
  }

  @Override
  public T copy() {
    return (T)super.copy();
  }

  @Override
  public T append(Series... other) {
    return (T)super.append(other);
  }

  @Override
  public T fillNullForward() {
    return (T)super.fillNullForward();
  }

  @Override
  public T fillNullBackward() {
    return (T)super.fillNullBackward();
  }

  @Override
  public T dropNull() {
    return (T)super.dropNull();
  }
}
