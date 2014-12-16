package com.linkedin.pinot.transport.pool;

/**
 * 
 * Interface to create/destroy and validate pooled resource
 * The implementation is expected to be thread-safe as concurrent request to create connection for same/different servers
 * is possible.
 * 
 * @param <K>
 * @param <T>
 */
public interface PooledResourceManager<K, T> {

  /**
   * Create a new resource
   * @param key to identify the pool which will host the resource
   * @return future for the resource creation
   */
  public T create(K key);

  /**
   * Destroy the pooled resource
   * @param key Key to identify the pool
   * @param isBad Are we destroying because the connection is bad ?
   * @param resource Resource to be destroyed
   * @return true if successfully destroyed the resource, false otherwise
   */
  public boolean destroy(K key, boolean isBad, T resource);

  /**
   * Validate if the resource is good.
   * 
   * @param key
   * @param resource
   * @return
   */
  public boolean validate(K key, T resource);

}
