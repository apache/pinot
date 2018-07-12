package com.linkedin.pinot.filesystem;

import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.DeviceCodeTokenProvider;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AzurePinotFS extends PinotFS {
  private static final Logger LOGGER = LoggerFactory.getLogger(AzurePinotFS.class);
  private static String _accountFQDN;  // full account FQDN, not just the account name
  private static String _nativeAppId;
  private static AccessTokenProvider _provider;
  public static String _storageConnectionString;
  private static String _container;

  @Override
  public void init(Configuration config) {
//    AdlFileSystem adlFileSystem = new AdlFileSystem();
//    adlFileSystem.initialize(config.getString("uri"), config);
    _accountFQDN = config.getString("accountFQDN");
    _nativeAppId = config.getString("nativeAppId");
    try {
      _provider = new DeviceCodeTokenProvider(_nativeAppId);
    } catch (IOException e) {
      LOGGER.error("Could not instantiate provider");
      throw new RuntimeException(e);
    }
    _storageConnectionString =
        "DefaultEndpointsProtocol=https;" +
            "AccountName=" + config.getString("accountName") + ";" +
            "AccountKey=" + config.getString("accountKey") + ";";
    _container = config.getString("container");
  }

  @Override
  public boolean delete(URI segmentUri) throws IOException {
    ADLStoreClient client = ADLStoreClient.createClient(_accountFQDN, _provider);
    return client.deleteRecursive(segmentUri.getPath());
  }

  @Override
  public boolean move(URI srcUri, URI dstUri) throws IOException {
    ADLStoreClient client = ADLStoreClient.createClient(_accountFQDN, _provider);
    //rename the file
    return client.rename(srcUri.getPath(), dstUri.getPath());
  }

  @Override
  public boolean copy(URI srcUri, URI dstUri) throws IOException {
    try {
      CloudStorageAccount storageAccount = CloudStorageAccount.parse(_storageConnectionString);
      CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
      CloudBlobContainer container = blobClient.getContainerReference(_container);
      CloudBlob blob = container.getBlockBlobReference(dstUri.getPath());
      blob.startCopyFromBlob(srcUri);
    } catch (Exception e) {
      LOGGER.error("Could not copy from {} to {}", srcUri, dstUri);
      throw new RuntimeException(e);
    }
    return true;
  }

  @Override
  public boolean exists(URI fileUri) throws IOException {
    ADLStoreClient client = ADLStoreClient.createClient(_accountFQDN, _provider);
    return client.checkExists(fileUri.getPath());
  }

  @Override
  public long length(URI fileUri) throws IOException {
    ADLStoreClient client = ADLStoreClient.createClient(_accountFQDN, _provider);
    // get file metadata
    DirectoryEntry ent = client.getDirectoryEntry(fileUri.getPath());
    return ent.length;
  }

  @Override
  public String[] listFiles(URI fileUri) throws IOException {
    ADLStoreClient client = ADLStoreClient.createClient(_accountFQDN, _provider);
    // list directory contents
    List<DirectoryEntry> list = client.enumerateDirectory(fileUri.getPath(), Integer.MAX_VALUE);
    List<String> fileNames = new ArrayList<>();
    for (DirectoryEntry directoryEntry : list) {
      fileNames.add(directoryEntry.name);
    }
    return fileNames.toArray(new String[fileNames.size()]);
  }

  @Override
  public void copyToLocalFile(URI srcUri, URI dstUri) throws IOException {
    try {
      CloudStorageAccount storageAccount = CloudStorageAccount.parse(_storageConnectionString);
      CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
      CloudBlobContainer container = blobClient.getContainerReference(_container);
      CloudBlob blob = container.getBlockBlobReference(srcUri.getPath());
      blob.downloadToFile(dstUri.getPath());
    } catch (Exception e) {
      LOGGER.error("Could not copy to local from {} to {}", srcUri, dstUri);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void copyFromLocalFile(URI srcUri, URI dstUri) throws IOException {
//    Writer output = new BufferedWriter(new FileWriter(srcUri.getPath()));
//    output.write("Hello Azure!");
//    output.close();

    //Getting a blob reference
    try {
      CloudStorageAccount storageAccount = CloudStorageAccount.parse(_storageConnectionString);
      CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
      CloudBlobContainer container = blobClient.getContainerReference(_container);
      CloudBlockBlob blob = container.getBlockBlobReference(srcUri.getPath());

      //Creating blob and uploading file to it
      blob.uploadFromFile(srcUri.getPath());

    } catch (Exception e) {
      LOGGER.error("Could not copy from local, src {} and dst {}", srcUri, dstUri);
      throw new RuntimeException(e);
    }

  }
}
