package com.linkedin.thirdeye.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.TimeRange;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
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
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.HashSet;
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

  private enum Mode
  {
    BOOTSTRAP,
    INCREMENT,
    PATCH
  }

  private final String user;
  private final String password;
  private final String collection;
  private final URI hdfsUri;
  private final URI thirdEyeUri;
  private final Mode mode;
  private final TimeRange globalTimeRange;
  private final HttpHost httpHost;
  private final HttpClient httpClient;

  public DataLoadTool(String user,
                      String password,
                      URI hdfsUri,
                      URI thirdEyeUri,
                      String collection,
                      Mode mode,
                      TimeRange timeRange)
  {
    this.user = user;
    this.password = password;
    this.hdfsUri = hdfsUri;
    this.thirdEyeUri = thirdEyeUri;
    this.mode = mode;
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
      String uri;
      HttpRequest hdfsReq;
      HttpResponse hdfsRes;

      LoginContext loginContext = login();

      // Construct loader based on URI scheme
      ThirdEyeLoader thirdEyeLoader;
      if ("http".equals(thirdEyeUri.getScheme()))
      {
        thirdEyeLoader = new ThirdEyeHttpLoader(new HttpHost(thirdEyeUri.getHost(), thirdEyeUri.getPort()), collection);
      }
      else
      {
        throw new IllegalArgumentException("Unsupported URI type " + thirdEyeUri);
      }
      LOG.info("Loading into {}", thirdEyeUri);

      // Get available times
      uri = createListTimeRequest();
      LOG.info("GET {}", uri);
      hdfsReq = new HttpGet(uri);
      hdfsRes = executePrivileged(loginContext, hdfsReq);
      if (hdfsRes.getStatusLine().getStatusCode() != 200)
      {
        throw new IllegalStateException("Request failed " + hdfsReq);
      }
      JsonNode fileStatuses = OBJECT_MAPPER.readTree(hdfsRes.getEntity().getContent());
      EntityUtils.consume(hdfsRes.getEntity());

      // Determine time ranges to load
      Set<TimeRange> timeRanges = new HashSet<TimeRange>();
      for (JsonNode fileStatus : fileStatuses.get("FileStatuses").get("FileStatus"))
      {
        String pathSuffix = fileStatus.get("pathSuffix").asText();

        if (pathSuffix.startsWith("data_"))
        {
          String[] pathTokens = pathSuffix.split("_");
          DateTime minTime = StarTreeConstants.DATE_TIME_FORMATTER.parseDateTime(pathTokens[1]);
          DateTime maxTime = StarTreeConstants.DATE_TIME_FORMATTER.parseDateTime(pathTokens[2]);

          TimeRange timeRange = new TimeRange(minTime.getMillis(), maxTime.getMillis());

          if (globalTimeRange.contains(timeRange))
          {
            timeRanges.add(timeRange);
          }
          else if (!globalTimeRange.isDisjoint(timeRange))
          {
            throw new IllegalArgumentException(
                    "Global time range " + globalTimeRange
                            + " does not contain time range " + timeRange + " and/or is not disjoint");
          }
        }
      }

      // Load config
      uri = createConfigRequest();
      LOG.info("GET {}", uri);
      hdfsReq = new HttpGet(uri);
      hdfsRes = executePrivileged(loginContext, hdfsReq);
      thirdEyeLoader.handleConfig(hdfsRes.getEntity().getContent());
      EntityUtils.consume(hdfsRes.getEntity());

      // Load star tree / data
      for (TimeRange timeRange : timeRanges)
      {
        DateTime minTime = new DateTime(timeRange.getStart());
        DateTime maxTime = new DateTime(timeRange.getEnd());

        // List data files
        uri = createListDataRequest(timeRange);
        LOG.info("GET {}", uri);
        hdfsReq = new HttpGet(uri);
        hdfsRes = executePrivileged(loginContext, hdfsReq);
        fileStatuses = OBJECT_MAPPER.readTree(hdfsRes.getEntity().getContent());
        EntityUtils.consume(hdfsRes.getEntity());

        // Get data files
        for (JsonNode fileStatus : fileStatuses.get("FileStatuses").get("FileStatus"))
        {
          String pathSuffix = fileStatus.get("pathSuffix").asText();

          if (pathSuffix.startsWith("task_"))
          {
            uri = createGetDataRequest(timeRange, pathSuffix);
            LOG.info("GET {}", uri);
            hdfsReq = new HttpGet(uri);
            hdfsRes = executePrivileged(loginContext, hdfsReq);
            thirdEyeLoader.handleData(
                    pathSuffix, hdfsRes.getEntity().getContent(), minTime, maxTime, Mode.BOOTSTRAP.equals(mode));
            EntityUtils.consume(hdfsRes.getEntity());
          }
        }
      }
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
  }

  private interface ThirdEyeLoader
  {
    void handleConfig(InputStream config) throws IOException;
    void handleData(String fileName, InputStream data, DateTime minTime, DateTime maxTime, boolean includeDimensions) throws IOException;
  }

  private class ThirdEyeHttpLoader implements ThirdEyeLoader
  {
    private final HttpHost host;
    private final String collection;

    ThirdEyeHttpLoader(HttpHost host, String collection)
    {
      this.host = host;
      this.collection = collection;
    }

    @Override
    public void handleConfig(InputStream config) throws IOException
    {
      String uri = "/collections/" + URLEncoder.encode(collection, "UTF-8");
      HttpResponse res = execute(uri, config);
      if (res.getStatusLine().getStatusCode() != 200
          && res.getStatusLine().getStatusCode() != 409) // conflict means one already there
      {
        throw new IOException(res.getStatusLine().toString());
      }
      LOG.info("POST {} #=> {}", uri, res.getStatusLine());
    }

    @Override
    public void handleData(String fileName,
                           InputStream data,
                           DateTime minTime,
                           DateTime maxTime,
                           boolean includeDimensions) throws IOException
    {
      String uri = "/collections/" + URLEncoder.encode(collection, "UTF-8") + "/data/"
              + minTime.getMillis() + "/" + maxTime.getMillis();
      if (includeDimensions)
      {
        uri += "?includeDimensions=true";
      }
      HttpResponse res = execute(uri, data);
      if (res.getStatusLine().getStatusCode() != 200)
      {
        throw new IOException(res.getStatusLine().toString());
      }
      LOG.info("POST ({}) {} #=> {}", fileName, uri, res.getStatusLine());
    }

    private HttpResponse execute(String uri, InputStream body) throws IOException
    {
      HttpPost req = new HttpPost(uri);
      req.setEntity(new InputStreamEntity(body));
      HttpResponse res = httpClient.execute(host, req);
      EntityUtils.consume(res.getEntity());
      return res;
    }
  }

  private String createListTimeRequest() throws IOException
  {
    String encodedCollection = URLEncoder.encode(collection, ENCODING);

    return URI_JOINER.join(WEB_HDFS_PREFIX + hdfsUri.getPath(),
            encodedCollection,
            mode + "?op=LISTSTATUS");
  }

  private String createStarTreeRequest(TimeRange timeRange) throws IOException
  {
    String encodedCollection = URLEncoder.encode(collection, ENCODING);
    String encodedStartTime = StarTreeConstants.DATE_TIME_FORMATTER.print(new DateTime(timeRange.getStart()));
    String encodedEndTime = StarTreeConstants.DATE_TIME_FORMATTER.print(new DateTime(timeRange.getEnd()));

    return URI_JOINER.join(WEB_HDFS_PREFIX + hdfsUri.getPath(),
            encodedCollection,
            mode,
            "data_" + encodedStartTime + "_" + encodedEndTime,
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
    String encodedStartTime = StarTreeConstants.DATE_TIME_FORMATTER.print(new DateTime(timeRange.getStart()));
    String encodedEndTime = StarTreeConstants.DATE_TIME_FORMATTER.print(new DateTime(timeRange.getEnd()));

    return URI_JOINER.join(WEB_HDFS_PREFIX + hdfsUri.getPath(),
            encodedCollection,
            mode,
            "data_" + encodedStartTime + "_" + encodedEndTime,
            "startree_bootstrap_phase2?op=LISTSTATUS");
  }

  private String createGetDataRequest(TimeRange timeRange, String pathSuffix) throws IOException
  {
    String encodedCollection = URLEncoder.encode(collection, ENCODING);
    String encodedStartTime = StarTreeConstants.DATE_TIME_FORMATTER.print(new DateTime(timeRange.getStart()));
    String encodedEndTime = StarTreeConstants.DATE_TIME_FORMATTER.print(new DateTime(timeRange.getEnd()));

    return URI_JOINER.join(WEB_HDFS_PREFIX + hdfsUri.getPath(),
            encodedCollection,
            mode,
            "data_" + encodedStartTime + "_" + encodedEndTime,
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
    options.addOption("help", false, "Prints this help message");
    options.addOption("minTime", true, "Min time to load");
    options.addOption("maxTime", true, "Max time to load");
    options.addOption("mode", true, "Data load mode (BOOTSTRAP, INCREMENT, PATCH)");

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
    DateTime minTime = commandLine.hasOption("minTime")
            ? parseDateTime(commandLine.getOptionValue("minTime"))
            : new DateTime(0);
    DateTime maxTime = commandLine.hasOption("maxTime")
            ? parseDateTime(commandLine.getOptionValue("maxTime"))
            : new DateTime(Long.MAX_VALUE);

    // Get mode
    Mode mode = Mode.valueOf(commandLine.getOptionValue("mode", "BOOTSTRAP").toUpperCase());

    new DataLoadTool(user,
            password,
            URI.create(commandLine.getArgs()[0]),
            URI.create(commandLine.getArgs()[1]),
            commandLine.getArgs()[2],
            mode,
            new TimeRange(minTime.getMillis(), maxTime.getMillis())).run();
  }

  private static DateTime parseDateTime(String dateTime) throws IOException
  {
    return ISODateTimeFormat.dateTimeParser().parseDateTime(URLDecoder.decode(dateTime, ENCODING));
  }
}
