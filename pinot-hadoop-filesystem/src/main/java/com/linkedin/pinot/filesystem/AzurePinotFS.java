package com.linkedin.pinot.filesystem;

import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AzurePinotFS extends PinotFS {
  private static final Logger LOGGER = LoggerFactory.getLogger(AzurePinotFS.class);
  private ADLStoreClient _adlStoreClient;

  private static final String AZURE = "azure";
  private static final String ACCOUNT = "account";
  private static final String AUTHENDPOINT = "authendpoint";
  private static final String ClIENTID = "clientid";
  private static final String CLIENTSECRET = "clientsecret";

  @Override
  public void init(Configuration config) {
    Configuration azureConfigs = config.subset(AZURE);
    // The ADL account id. Example: {@code mystore.azuredatalakestore.net}.
    String account = azureConfigs.getString(ACCOUNT);
    // The endpoint that should be used for authentication.
    // Usually of the form {@code https://login.microsoftonline.com/<tenant-id>/oauth2/token}.
    String authEndpoint = azureConfigs.getString(AUTHENDPOINT);
    // The clientId used to authenticate this application
    String clientId = azureConfigs.getString(ClIENTID);
    // The secret key used to authenticate this application
    String clientSecret = azureConfigs.getString(CLIENTSECRET);

    AccessTokenProvider tokenProvider =
        new ClientCredsTokenProvider(authEndpoint, clientId, clientSecret);
    _adlStoreClient = ADLStoreClient.createClient(account, tokenProvider);
  }

  @Override
  public boolean delete(URI segmentUri) throws IOException {
    return _adlStoreClient.deleteRecursive(segmentUri.getPath());
  }

  @Override
  public boolean move(URI srcUri, URI dstUri) throws IOException {
    //rename the file
    return _adlStoreClient.rename(srcUri.getPath(), dstUri.getPath());
  }

  @Override
  public boolean copy(URI srcUri, URI dstUri) throws IOException {
    // TODO: Add this method. Not needed for now because file will be in final location
    throw new UnsupportedOperationException("Cannot copy from AzureFS to AzureFS");
  }

  @Override
  public boolean exists(URI fileUri) throws IOException {
    return _adlStoreClient.checkExists(fileUri.getPath());
  }

  @Override
  public long length(URI fileUri) throws IOException {
    // get file metadata
    DirectoryEntry ent = _adlStoreClient.getDirectoryEntry(fileUri.getPath());
    return ent.length;
  }

  @Override
  public String[] listFiles(URI fileUri) throws IOException {
    // list directory contents
    List<DirectoryEntry> list = _adlStoreClient.enumerateDirectory(fileUri.getPath(), Integer.MAX_VALUE);
    List<String> fileNames = new ArrayList<>();
    for (DirectoryEntry directoryEntry : list) {
      fileNames.add(directoryEntry.name);
    }
    return fileNames.toArray(new String[fileNames.size()]);
  }

  @Override
  public void copyToLocalFile(URI srcUri, URI dstUri) throws IOException {
    try (InputStream adlStream = _adlStoreClient.getReadStream(srcUri.getPath())) {
      Path dstFilePath = Paths.get(dstUri);

      /* Copy the source file to the destination directory as a file with the same name as the source,
       * replace an existing file with the same name in the destination directory, if any.
       * Set new file permissions on the copied file.
       */
      Files.copy(adlStream, dstFilePath);

      LOGGER.info("Copied file {} from ADL to {}", srcUri, dstUri);
    } catch (Exception ex) {
      throw ex;
    }
  }

  @Override
  public void copyFromLocalFile(URI srcUri, URI dstUri) throws IOException {
    // TODO: Add this method. Not needed for now because of metadata upload
    throw new UnsupportedOperationException("Cannot copy from local to Azure");
  }
}
