package com.linkedin.thirdeye.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.impl.TarUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import java.io.Console;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class DataLoadTool implements Runnable
{
  private static final Logger LOG = LoggerFactory.getLogger(DataLoadTool.class);
  private static final Joiner URI_JOINER = Joiner.on("/");
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String ENCODING = "UTF-8";

  private static String USAGE = "usage: [opts] hdfsUri thirdEyeUri collection";

  private static String LOGIN_CONF = "KrbLogin{\n"
          + "\tcom.sun.security.auth.module.Krb5LoginModule required doNotPrompt=false useTicketCache=false;\n"
          + "};";

  private static String WEB_HDFS_PREFIX = URI_JOINER.join("", "webhdfs", "v1");

  private static String DEFAULT_KRB5_CONF = System.getProperty("user.home") + File.separator + ".krb5.conf";

  private final String user;
  private final String password;
  private final String collection;
  private final URI hdfsUri;
  private final URI thirdEyeUri;
  private final boolean includeStarTree;
  private final boolean includeConfig;
  private final boolean includeDimensions;
  private final TimeRange globalTimeRange;
  private final HttpHost httpHost;
  private final HttpClient httpClient;

  public DataLoadTool(String user,
                      String password,
                      URI hdfsUri,
                      URI thirdEyeUri,
                      String collection,
                      boolean includeStarTree,
                      boolean includeConfig,
                      boolean includeDimensions,
                      TimeRange timeRange)
  {
    this.user = user;
    this.password = password;
    this.hdfsUri = hdfsUri;
    this.thirdEyeUri = thirdEyeUri;
    this.includeStarTree = includeStarTree;
    this.includeConfig = includeConfig;
    this.includeDimensions = includeDimensions;
    this.globalTimeRange = timeRange;
    this.collection = collection;
    this.httpHost = new HttpHost(hdfsUri.getHost(), hdfsUri.getPort(), hdfsUri.getScheme());
    this.httpClient = getHttpClient();
  }

  @Override
  public void run()
  {
    try
    {
      LoginContext loginContext = login();

      if ("file".equals(thirdEyeUri.getScheme()))
      {
        String uri;
        HttpRequest req;
        HttpResponse res;
        File file;
        FileOutputStream fos;

        // Create collection dir
        File collectionDir = new File(thirdEyeUri.getPath(), collection);
        FileUtils.forceMkdir(collectionDir);

        // Get available times
        uri = createListTimeRequest();
        req = new HttpGet(uri);
        res = executePrivileged(loginContext, req);
        JsonNode timeSegmentResult = OBJECT_MAPPER.readTree(res.getEntity().getContent());
        EntityUtils.consume(res.getEntity());

        LOG.info("Target time range {}", globalTimeRange);
        List<TimeRange> timeRanges = new ArrayList<TimeRange>();
        for (JsonNode fileStatus : timeSegmentResult.get("FileStatuses").get("FileStatus"))
        {
          String pathSuffix = fileStatus.get("pathSuffix").asText();
          if (pathSuffix.startsWith("data_"))
          {
            String[] tokens = pathSuffix.substring("data_".length(), pathSuffix.length()).split("-");
            TimeRange timeRange = new TimeRange(Long.valueOf(tokens[0]), Long.valueOf(tokens[1]));
            if (globalTimeRange.contains(timeRange))
            {
              timeRanges.add(timeRange);
              LOG.info("Processing {}", pathSuffix);
            }
            else if (!globalTimeRange.isDisjoint(timeRange))
            {
              throw new IllegalArgumentException(
                      "Global time range " + globalTimeRange
                              + " does not contain time range " + timeRange + " and is not disjoint");
            }
            else
            {
              LOG.info("Skipping {}", pathSuffix);
            }
          }
        }

        if (includeConfig)
        {
          // Get config
          uri = createConfigRequest();
          req = new HttpGet(uri);
          res = executePrivileged(loginContext, req);
          file = new File(collectionDir, StarTreeConstants.CONFIG_FILE_NAME);
          fos = new FileOutputStream(file);
          IOUtils.copy(res.getEntity().getContent(), fos);
          EntityUtils.consume(res.getEntity());
          fos.flush();
          fos.close();
          LOG.info("Copied {} for {} from {} to {}", StarTreeConstants.CONFIG_FILE_NAME, collection, httpHost, file);
        }

        for (TimeRange timeRange : timeRanges)
        {
          if (includeStarTree)
          {
            // Get star tree
            uri = createStarTreeRequest(timeRange);
            req = new HttpGet(uri);
            res = executePrivileged(loginContext, req);
            file = new File(collectionDir, StarTreeConstants.TREE_FILE_NAME);
            fos = new FileOutputStream(file);
            IOUtils.copy(res.getEntity().getContent(), fos);
            EntityUtils.consume(res.getEntity());
            fos.flush();
            fos.close();
            LOG.info("Copied {} for {} from {} to {}", StarTreeConstants.TREE_FILE_NAME, collection, httpHost, file);
          }

          // List data files
          uri = createListDataRequest(timeRange);
          req = new HttpGet(uri);
          res = executePrivileged(loginContext, req);
          JsonNode dataFiles = OBJECT_MAPPER.readTree(res.getEntity().getContent());
          EntityUtils.consume(res.getEntity());

          // Get data files
          Set<String> blacklist = includeDimensions ? null : ImmutableSet.of("dimensionStore");
          file = new File(collectionDir, StarTreeConstants.DATA_DIR_NAME);
          for (JsonNode fileStatus : dataFiles.get("FileStatuses").get("FileStatus"))
          {
            String pathSuffix = fileStatus.get("pathSuffix").asText();
            if (pathSuffix.startsWith("task_"))
            {
              String dataFileUri = createGetDataRequest(timeRange, pathSuffix);
              req = new HttpGet(dataFileUri);
              res = executePrivileged(loginContext, req);
              TarUtils.extractGzippedTarArchive(res.getEntity().getContent(), file, 2, blacklist);
              EntityUtils.consume(res.getEntity());
              LOG.info("Copied data archive {}", pathSuffix);
            }
          }
        }
      }
      else
      {
        throw new IllegalArgumentException("Invalid ThirdEye scheme: " + thirdEyeUri.getScheme());
      }
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
  }

  private String createListTimeRequest() throws IOException
  {
    String encodedCollection = URLEncoder.encode(collection, ENCODING);

    return URI_JOINER.join(WEB_HDFS_PREFIX + hdfsUri.getPath(),
                           encodedCollection + "?op=LISTSTATUS");
  }

  private String createStarTreeRequest(TimeRange timeRange) throws IOException
  {
    String encodedCollection = URLEncoder.encode(collection, ENCODING);

    return URI_JOINER.join(WEB_HDFS_PREFIX + hdfsUri.getPath(),
                           encodedCollection,
                           "data_" + timeRange.getStart() + "-" + timeRange.getEnd(),
                           "startree_generation",
                           "star-tree-" + encodedCollection,
                           encodedCollection + "-tree.bin?op=OPEN");
  }

  private String createConfigRequest() throws IOException
  {
    String encodedCollection = URLEncoder.encode(collection, ENCODING);

    return URI_JOINER.join(WEB_HDFS_PREFIX + hdfsUri.getPath(),
                           encodedCollection,
                           StarTreeConstants.CONFIG_FILE_NAME + "?op=OPEN");
  }

  private String createListDataRequest(TimeRange timeRange) throws IOException
  {
    String encodedCollection = URLEncoder.encode(collection, ENCODING);

    return URI_JOINER.join(WEB_HDFS_PREFIX + hdfsUri.getPath(),
                           encodedCollection,
                           "data_" + timeRange.getStart() + "-" + timeRange.getEnd(),
                           "startree_bootstrap_phase2?op=LISTSTATUS");
  }

  private String createGetDataRequest(TimeRange timeRange, String pathSuffix) throws IOException
  {
    String encodedCollection = URLEncoder.encode(collection, ENCODING);

    return URI_JOINER.join(WEB_HDFS_PREFIX + hdfsUri.getPath(),
                           encodedCollection,
                           "data_" + timeRange.getStart() + "-" + timeRange.getEnd(),
                           "startree_bootstrap_phase2",
                           pathSuffix + "?op=OPEN");
  }

  private LoginContext login() throws Exception
  {
    LoginContext loginContext = new LoginContext("KrbLogin", new CallbackHandler()
    {
      @Override
      public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException
      {
        for (Callback callback : callbacks)
        {
          if (callback instanceof NameCallback)
          {
            NameCallback nc = (NameCallback) callback;
            nc.setName(user);
          } else if (callback instanceof PasswordCallback)
          {
            PasswordCallback pc = (PasswordCallback) callback;
            pc.setPassword(password.toCharArray());
          } else
          {
            throw new UnsupportedCallbackException(callback, "Unknown Callback");
          }
        }
      }
    });

    loginContext.login();

    return loginContext;
  }

  private HttpResponse executePrivileged(final LoginContext loginContext, final HttpRequest httpRequest)
  {
    return Subject.doAs(loginContext.getSubject(), new PrivilegedAction<HttpResponse>()
    {
      @Override
      public HttpResponse run()
      {
        try
        {
          return httpClient.execute(httpHost, httpRequest);
        }
        catch (IOException e)
        {
          throw new IllegalStateException(e);
        }
      }
    });
  }

  private static HttpClient getHttpClient()
  {
    Credentials use_jaas_creds = new Credentials()
    {
      public String getPassword()
      {
        return null;
      }

      public Principal getUserPrincipal()
      {
        return null;
      }
    };

    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(new AuthScope(null, -1, null), use_jaas_creds);

    Registry<AuthSchemeProvider> authSchemeRegistry
            = RegistryBuilder.<AuthSchemeProvider>create()
                             .register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true)).build();

    return HttpClients.custom()
                      .setDefaultAuthSchemeRegistry(authSchemeRegistry)
                      .setDefaultCredentialsProvider(credentialsProvider)
                      .build();
  }

  public static void main(String[] args) throws Exception
  {
    Options options = new Options();
    options.addOption("krb5", true, "Path to krb5.conf file (default: ~/.krb5.conf)");
    options.addOption("debug", false, "Debug logging");
    options.addOption("includeStarTree", false, "Copy star tree binary");
    options.addOption("includeConfig", false, "Copy config file");
    options.addOption("includeDimensions", false, "Copy dimension data");
    options.addOption("help", false, "Prints this help message");
    options.addOption("minTime", true, "Min time to load");
    options.addOption("maxTime", true, "Max time to load");

    CommandLine commandLine = new GnuParser().parse(options, args);

    if (commandLine.getArgs().length != 3 || commandLine.hasOption("help"))
    {
      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.printHelp(USAGE, options);
      System.exit(1);
    }

    // Write login.conf file
    File loginConf = new File(System.getProperty("java.io.tmpdir"), "thirdeye-login.conf." + System.currentTimeMillis());
    loginConf.deleteOnExit();
    OutputStream outputStream = new FileOutputStream(loginConf);
    IOUtils.write(LOGIN_CONF.getBytes(), outputStream);
    outputStream.flush();
    outputStream.close();

    // Set properties
    System.setProperty("java.security.auth.login.config", loginConf.getAbsolutePath());
    System.setProperty("java.security.krb5.conf", commandLine.getOptionValue("krb5", DEFAULT_KRB5_CONF));
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
    System.setProperty("sun.security.krb5.debug", Boolean.toString(options.hasOption("debug")));

    // Get password
    Console console = System.console();
    char[] passwordChars = console.readPassword("password: ");
    String user = System.getProperty("user.name");
    String password = new String(passwordChars);

    // Get time range
    long minTime = commandLine.hasOption("minTime")
            ? Long.valueOf(commandLine.getOptionValue("minTime"))
            : 0;
    long maxTime = commandLine.hasOption("maxTime")
            ? Long.valueOf(commandLine.getOptionValue("maxTime"))
            : Long.MAX_VALUE;

    new DataLoadTool(user,
                     password,
                     URI.create(commandLine.getArgs()[0]),
                     URI.create(commandLine.getArgs()[1]),
                     commandLine.getArgs()[2],
                     commandLine.hasOption("includeStarTree"),
                     commandLine.hasOption("includeConfig"),
                     commandLine.hasOption("includeDimensions"),
                     new TimeRange(minTime, maxTime)).run();
  }
}
