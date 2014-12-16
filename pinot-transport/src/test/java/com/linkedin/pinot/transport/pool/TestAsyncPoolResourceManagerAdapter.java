package com.linkedin.pinot.transport.pool;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.google.common.util.concurrent.MoreExecutors;
import com.linkedin.pinot.transport.common.Callback;


public class TestAsyncPoolResourceManagerAdapter {

  @Test
  public void testCreate() {

    // Success
    {
      String key = "localhost:8080";
      String value = "dummy";
      MyPooledResourceManager rm = new MyPooledResourceManager(true, value);
      AsyncPoolResourceManagerAdapter<String, String> adapter =
          new AsyncPoolResourceManagerAdapter<String, String>(key, rm, MoreExecutors.sameThreadExecutor(), null);
      MyCallback callback = new MyCallback();

      adapter.create(callback);
      AssertJUnit.assertEquals("Callback Resource", value, callback.getResource());
      AssertJUnit.assertEquals("Callback onSuccess", true, callback.isOnSuccessCalled());
      AssertJUnit.assertEquals("Callback onError", false, callback.isOnErrorCalled());
      AssertJUnit.assertNull("Callback Error Null", callback.getThrowable());
      AssertJUnit.assertEquals("Resource Manager create Key", key, rm.getCreateKey());
    }

    //Error
    {
      String key = "localhost:8080";
      MyPooledResourceManager rm = new MyPooledResourceManager(true, null);
      AsyncPoolResourceManagerAdapter<String, String> adapter =
          new AsyncPoolResourceManagerAdapter<String, String>(key, rm, MoreExecutors.sameThreadExecutor(), null);
      MyCallback callback = new MyCallback();

      adapter.create(callback);
      AssertJUnit.assertNull("Callback Resource", callback.getResource());
      AssertJUnit.assertEquals("Callback onSuccess", false, callback.isOnSuccessCalled());
      AssertJUnit.assertEquals("Callback onError", true, callback.isOnErrorCalled());
      AssertJUnit.assertNotNull("Callback Error Null", callback.getThrowable());
      AssertJUnit.assertEquals("Resource Manager create Key", key, rm.getCreateKey());
    }
  }

  @Test
  public void testValidate() {
    //Success
    {
      String key = "localhost:8080";
      String value = "dummy";
      MyPooledResourceManager rm = new MyPooledResourceManager(true, null);
      AsyncPoolResourceManagerAdapter<String, String> adapter =
          new AsyncPoolResourceManagerAdapter<String, String>(key, rm, MoreExecutors.sameThreadExecutor(), null);

      boolean ret = adapter.validateGet(value);
      AssertJUnit.assertTrue("Validate Return", ret);
      AssertJUnit.assertEquals("Resource Manager validate Key", key, rm.getKeyForValidate());
      AssertJUnit.assertEquals("Resource Manager validate Resource", value, rm.getResourceForValidate());

      String value2 = "dummy2";
      ret = adapter.validatePut(value2);
      AssertJUnit.assertTrue("Validate Return", ret);

      AssertJUnit.assertEquals("Resource Manager validate Key", key, rm.getKeyForValidate());
      AssertJUnit.assertEquals("Resource Manager validate Resource", value2, rm.getResourceForValidate());
    }

    //Error
    {
      String key = "localhost:8080";
      String value = "dummy";
      MyPooledResourceManager rm = new MyPooledResourceManager(false, null);
      AsyncPoolResourceManagerAdapter<String, String> adapter =
          new AsyncPoolResourceManagerAdapter<String, String>(key, rm, MoreExecutors.sameThreadExecutor(), null);

      boolean ret = adapter.validateGet(value);
      AssertJUnit.assertFalse("Validate Return", ret);

      AssertJUnit.assertEquals("Resource Manager validate Key", key, rm.getKeyForValidate());
      AssertJUnit.assertEquals("Resource Manager validate Resource", value, rm.getResourceForValidate());

      String value2 = "dummy2";
      ret = adapter.validatePut(value2);
      AssertJUnit.assertFalse("Validate Return", ret);

      AssertJUnit.assertEquals("Resource Manager validate Key", key, rm.getKeyForValidate());
      AssertJUnit.assertEquals("Resource Manager validate Resource", value2, rm.getResourceForValidate());
    }
  }

  @Test
  public void testDestroy() {

    // Success
    {
      String key = "localhost:8080";
      String value = "dummy";
      MyPooledResourceManager rm = new MyPooledResourceManager(true, null);
      AsyncPoolResourceManagerAdapter<String, String> adapter =
          new AsyncPoolResourceManagerAdapter<String, String>(key, rm, MoreExecutors.sameThreadExecutor(), null);
      MyCallback callback = new MyCallback();

      adapter.destroy(value, true, callback);
      AssertJUnit.assertEquals("Callback Resource", value, callback.getResource());
      AssertJUnit.assertEquals("Callback onSuccess", true, callback.isOnSuccessCalled());
      AssertJUnit.assertEquals("Callback onError", false, callback.isOnErrorCalled());
      AssertJUnit.assertNull("Callback Error Null", callback.getThrowable());
      AssertJUnit.assertEquals("Resource Manager create Key", key, rm.getKeyForDestroy());
      AssertJUnit.assertEquals("Resource Manager create Resource", value, rm.getResourceForDestroy());
    }

    // Error
    {
      String key = "localhost:8080";
      String value = "dummy";
      MyPooledResourceManager rm = new MyPooledResourceManager(false, null);
      AsyncPoolResourceManagerAdapter<String, String> adapter =
          new AsyncPoolResourceManagerAdapter<String, String>(key, rm, MoreExecutors.sameThreadExecutor(), null);
      MyCallback callback = new MyCallback();

      adapter.destroy(value, true, callback);
      AssertJUnit.assertNull("Callback Resource", callback.getResource());
      AssertJUnit.assertEquals("Callback onSuccess", false, callback.isOnSuccessCalled());
      AssertJUnit.assertEquals("Callback onError", true, callback.isOnErrorCalled());
      AssertJUnit.assertNotNull("Callback Error Null", callback.getThrowable());
      AssertJUnit.assertEquals("Resource Manager create Key", key, rm.getKeyForDestroy());
      AssertJUnit.assertEquals("Resource Manager create Resource", value, rm.getResourceForDestroy());
    }
  }

  public static class MyPooledResourceManager implements PooledResourceManager<String, String> {
    private String _createKey;
    private final String _createReturnValue;
    private String _resourceForDestroy;
    private String _resourceForValidate;
    private String _keyForDestroy;
    private String _keyForValidate;
    private final boolean _boolReturnVal;

    public MyPooledResourceManager(boolean returnVal, String createReturnValue) {
      _boolReturnVal = returnVal;
      _createReturnValue = createReturnValue;
    }

    @Override
    public String create(String key) {
      _createKey = key;
      return _createReturnValue;
    }

    @Override
    public boolean destroy(String key, boolean isBad, String resource) {
      _keyForDestroy = key;
      _resourceForDestroy = resource;
      return _boolReturnVal;
    }

    @Override
    public boolean validate(String key, String resource) {
      _keyForValidate = key;
      _resourceForValidate = resource;
      return _boolReturnVal;
    }

    public String getCreateKey() {
      return _createKey;
    }

    public String getResourceForDestroy() {
      return _resourceForDestroy;
    }

    public String getResourceForValidate() {
      return _resourceForValidate;
    }

    public String getKeyForDestroy() {
      return _keyForDestroy;
    }

    public String getKeyForValidate() {
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
