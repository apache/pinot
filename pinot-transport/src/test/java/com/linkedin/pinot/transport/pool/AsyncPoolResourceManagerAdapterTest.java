/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.transport.pool;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.util.concurrent.MoreExecutors;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.Callback;


public class AsyncPoolResourceManagerAdapterTest {

  @Test
  public void testCreate() {

    // Success
    {
      ServerInstance key = new ServerInstance("localhost:8080");
      String value = "dummy";
      MyPooledResourceManager rm = new MyPooledResourceManager(true, value);
      AsyncPoolResourceManagerAdapter<String> adapter =
          new AsyncPoolResourceManagerAdapter<>(key, rm, MoreExecutors.sameThreadExecutor(), null);
      MyCallback callback = new MyCallback();

      adapter.create(callback);
      Assert.assertEquals(callback.getResource(), value, "Callback Resource");
      Assert.assertEquals(callback.isOnSuccessCalled(), true, "Callback onSuccess");
      Assert.assertEquals(callback.isOnErrorCalled(), false, "Callback onError");
      Assert.assertNull(callback.getThrowable(), "Callback Error Null");
      Assert.assertEquals(rm.getCreateKey(), key, "Resource Manager create Key");
    }

    //Error
    {
      ServerInstance key = new ServerInstance("localhost:8080");
      MyPooledResourceManager rm = new MyPooledResourceManager(true, null);
      AsyncPoolResourceManagerAdapter<String> adapter =
          new AsyncPoolResourceManagerAdapter<String>(key, rm, MoreExecutors.sameThreadExecutor(), null);
      MyCallback callback = new MyCallback();

      adapter.create(callback);
      Assert.assertNull(callback.getResource(), "Callback Resource");
      Assert.assertEquals(callback.isOnSuccessCalled(), false, "Callback onSuccess");
      Assert.assertEquals(callback.isOnErrorCalled(), true, "Callback onError");
      Assert.assertNotNull(callback.getThrowable(), "Callback Error Null");
      Assert.assertEquals(rm.getCreateKey(), key, "Resource Manager create Key");
    }
  }

  @Test
  public void testValidate() {
    //Success
    {
      ServerInstance key = new ServerInstance("localhost:8080");
      String value = "dummy";
      MyPooledResourceManager rm = new MyPooledResourceManager(true, null);
      AsyncPoolResourceManagerAdapter<String> adapter =
          new AsyncPoolResourceManagerAdapter<>(key, rm, MoreExecutors.sameThreadExecutor(), null);

      boolean ret = adapter.validateGet(value);
      Assert.assertTrue(ret, "Validate Return");
      Assert.assertEquals(rm.getKeyForValidate(), key, "Resource Manager validate Key");
      Assert.assertEquals(rm.getResourceForValidate(), value, "Resource Manager validate Resource");

      String value2 = "dummy2";
      ret = adapter.validatePut(value2);
      Assert.assertTrue(ret, "Validate Return");

      Assert.assertEquals(rm.getKeyForValidate(), key, "Resource Manager validate Key");
      Assert.assertEquals(rm.getResourceForValidate(), value2, "Resource Manager validate Resource");
    }

    //Error
    {
      ServerInstance key = new ServerInstance("localhost:8080");
      String value = "dummy";
      MyPooledResourceManager rm = new MyPooledResourceManager(false, null);
      AsyncPoolResourceManagerAdapter<String> adapter =
          new AsyncPoolResourceManagerAdapter<>(key, rm, MoreExecutors.sameThreadExecutor(), null);

      boolean ret = adapter.validateGet(value);
      Assert.assertFalse(ret, "Validate Return");

      Assert.assertEquals(rm.getKeyForValidate(), key, "Resource Manager validate Key");
      Assert.assertEquals(rm.getResourceForValidate(), value, "Resource Manager validate Resource");

      String value2 = "dummy2";
      ret = adapter.validatePut(value2);
      Assert.assertFalse(ret, "Validate Return");

      Assert.assertEquals(rm.getKeyForValidate(), key, "Resource Manager validate Key");
      Assert.assertEquals(rm.getResourceForValidate(), value2, "Resource Manager validate Resource");
    }
  }

  @Test
  public void testDestroy() {

    // Success
    {
      ServerInstance key = new ServerInstance("localhost:8080");
      String value = "dummy";
      MyPooledResourceManager rm = new MyPooledResourceManager(true, null);
      AsyncPoolResourceManagerAdapter<String> adapter =
          new AsyncPoolResourceManagerAdapter<>(key, rm, MoreExecutors.sameThreadExecutor(), null);
      MyCallback callback = new MyCallback();

      adapter.destroy(value, true, callback);
      Assert.assertEquals(callback.getResource(), value, "Callback Resource");
      Assert.assertEquals(callback.isOnSuccessCalled(), true, "Callback onSuccess");
      Assert.assertEquals(callback.isOnErrorCalled(), false, "Callback onError");
      Assert.assertNull(callback.getThrowable(), "Callback Error Null");
      Assert.assertEquals(rm.getKeyForDestroy(), key, "Resource Manager create Key");
      Assert.assertEquals(rm.getResourceForDestroy(), value, "Resource Manager create Resource");
    }

    // Error
    {
      ServerInstance key = new ServerInstance("localhost:8080");
      String value = "dummy";
      MyPooledResourceManager rm = new MyPooledResourceManager(false, null);
      AsyncPoolResourceManagerAdapter<String> adapter =
          new AsyncPoolResourceManagerAdapter<>(key, rm, MoreExecutors.sameThreadExecutor(), null);
      MyCallback callback = new MyCallback();

      adapter.destroy(value, true, callback);
      Assert.assertNull(callback.getResource(), "Callback Resource");
      Assert.assertEquals(callback.isOnSuccessCalled(), false, "Callback onSuccess");
      Assert.assertEquals(callback.isOnErrorCalled(), true, "Callback onError");
      Assert.assertNotNull(callback.getThrowable(), "Callback Error Null");
      Assert.assertEquals(rm.getKeyForDestroy(), key, "Resource Manager create Key");
      Assert.assertEquals(rm.getResourceForDestroy(), value, "Resource Manager create Resource");
    }
  }

  public static class MyPooledResourceManager implements PooledResourceManager<String> {
    private ServerInstance _createKey;
    private final String _createReturnValue;
    private String _resourceForDestroy;
    private String _resourceForValidate;
    private ServerInstance _keyForDestroy;
    private ServerInstance _keyForValidate;
    private final boolean _boolReturnVal;

    public MyPooledResourceManager(boolean returnVal, String createReturnValue) {
      _boolReturnVal = returnVal;
      _createReturnValue = createReturnValue;
    }

    @Override
    public String create(ServerInstance key) {
      _createKey = key;
      return _createReturnValue;
    }

    @Override
    public boolean destroy(ServerInstance key, boolean isBad, String resource) {
      _keyForDestroy = key;
      _resourceForDestroy = resource;
      return _boolReturnVal;
    }

    @Override
    public boolean validate(ServerInstance key, String resource) {
      _keyForValidate = key;
      _resourceForValidate = resource;
      return _boolReturnVal;
    }

    public ServerInstance getCreateKey() {
      return _createKey;
    }

    public String getResourceForDestroy() {
      return _resourceForDestroy;
    }

    public String getResourceForValidate() {
      return _resourceForValidate;
    }

    public ServerInstance getKeyForDestroy() {
      return _keyForDestroy;
    }

    public ServerInstance getKeyForValidate() {
      return _keyForValidate;
    }
  }

  public static class MyCallback implements Callback<String> {
    private String _resource;
    private Throwable _throwable;
    private boolean _onSuccessCalled;
    private boolean _onErrorCalled;

    @Override
    public void onSuccess(String arg0) {
      _onSuccessCalled = true;
      _resource = arg0;
    }

    @Override
    public void onError(Throwable arg0) {
      _onErrorCalled = true;
      _throwable = arg0;
    }

    public String getResource() {
      return _resource;
    }

    public Throwable getThrowable() {
      return _throwable;
    }

    public boolean isOnSuccessCalled() {
      return _onSuccessCalled;
    }

    public boolean isOnErrorCalled() {
      return _onErrorCalled;
    }
  }
}
