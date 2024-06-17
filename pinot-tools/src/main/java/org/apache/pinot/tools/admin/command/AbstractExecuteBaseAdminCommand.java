package org.apache.pinot.tools.admin.command;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.apache.pinot.common.utils.ClientSSLContextGenerator;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.tools.Command;
import picocli.CommandLine;

import javax.net.ssl.SSLContext;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractExecuteBaseAdminCommand extends AbstractBaseAdminCommand implements Command {
    @CommandLine.Option(names = {"-controllerHost"}, required = false, description = "Host name for controller.")
    protected String _controllerHost;
    @CommandLine.Option(names = {"-controllerPort"}, required = false, description = "Port number for controller.")
    protected String _controllerPort = DEFAULT_CONTROLLER_PORT;
    @CommandLine.Option(names = {"-controllerProtocol"}, required = false, description = "Protocol for controller.")
    protected String _controllerProtocol = CommonConstants.HTTP_PROTOCOL;
    @CommandLine.Option(names = {"-user"}, required = false, description = "Username for basic auth.")
    protected String _user;
    @CommandLine.Option(names = {"-password"}, required = false, description = "Password for basic auth.")
    protected String _password;
    @CommandLine.Option(names = {"-authToken"}, required = false, description = "Http auth token.")
    protected String _authToken;
    @CommandLine.Option(names = {"-authTokenUrl"}, required = false, description = "Http auth token url.")
    protected String _authTokenUrl;
    protected AuthProvider _authProvider;
    @CommandLine.Option(names = {"-exec"}, required = false, description = "Execute the command.")
    protected boolean _exec;
    @CommandLine.Option(names = {"-skipControllerCertValidation"}, required = false, description = "Whether to skip"
        + " controller certification validation.")
    private boolean _skipControllerCertValidation = false;
    @CommandLine.Parameters(paramLabel = "-headers", description = "Additional headers to send.")
    protected String[] _headers;

    final public AbstractExecuteBaseAdminCommand setControllerHost(String controllerHost) {
      _controllerHost = controllerHost;
      return this;
    }

    final public AbstractExecuteBaseAdminCommand setControllerPort(String controllerPort) {
      _controllerPort = controllerPort;
      return this;
    }

    final public AbstractExecuteBaseAdminCommand setControllerProtocol(String controllerProtocol) {
      _controllerProtocol = controllerProtocol;
      return this;
    }

    final public AbstractExecuteBaseAdminCommand setUser(String user) {
      _user = user;
      return this;
    }

    final public AbstractExecuteBaseAdminCommand setPassword(String password) {
      _password = password;
      return this;
    }

    final public AbstractExecuteBaseAdminCommand setAuthProvider(AuthProvider authProvider) {
      _authProvider = authProvider;
      return this;
    }

    final public AbstractExecuteBaseAdminCommand setHeaders(List<String> headers) {
        _headers = headers.toArray(new String[headers.size()]);
        return this;
    }

    protected SSLContext makeTrustAllSSLContext() {
      if (_skipControllerCertValidation) {
        PinotConfiguration trustAllSslConfig = new PinotConfiguration();
        return new ClientSSLContextGenerator(trustAllSslConfig).generate();
      } else {
        return null;
      }
    }

    final protected List<Header> getHeaders(List<Header> headers) {
        if (_headers != null) {
            for (String header: _headers) {
                String[] pair = header.split("=");
                if (pair.length == 2) {
                    BasicHeader database = new BasicHeader(pair[0], pair[1]);
                    headers.add(database);
                }
            }
        }
        return headers;
    }

    final protected Map<String, String> getHeadersAsMap(List<Header> headers) {
        return getHeaders(headers).stream().collect(Collectors.toMap(Header::getName, Header::getValue));
    }

    final public AbstractExecuteBaseAdminCommand setExecute(boolean exec) {
      _exec = exec;
      return this;
    }
}
