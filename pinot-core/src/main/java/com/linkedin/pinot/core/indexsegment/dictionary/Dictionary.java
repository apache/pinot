package com.linkedin.pinot.core.indexsegment.dictionary;

public abstract class Dictionary<T> {

  abstract public boolean contains(Object o);

  abstract public int indexOf(Object o);

  abstract public int size();

  abstract public T getRaw(int index);

  abstract public int getInteger(int index);

  abstract public float getFloat(int index);

  abstract public long getLong(int index);

  abstract public double getDouble(int index);

  abstract public String getString(int index);
}
