/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.cli.thrift;

import java.net.InetSocketAddress;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.AbstractService;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.GetInfoType;
import org.apache.hive.service.cli.GetInfoValue;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.SessionManager;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TSocket;


import org.apache.hive.service.auth.RemoteAccessControl;
import org.apache.hive.service.cli.session.SessionManagerDelegator;
/**
 * CLIService.
 *
 */
public class ThriftCLIServiceDelegator extends ThriftCLIService implements TCLIService.Iface, Runnable {

  public static final Log LOG = LogFactory.getLog(ThriftCLIServiceDelegator.class.getName());

  private static HiveAuthFactory hiveAuthFactory;

  private static RemoteAccessControl remoteAccessControl = null;
  private TCLIService.Client delegate;
  private String instanceName = null;
  private String restrictDatabase = null;
  private SessionManagerDelegator sessions;
  
  private int portNum;
  private InetSocketAddress serverAddress;
  private TServer server;

  private boolean isStarted = false;
  protected boolean isEmbedded = false;

  private HiveConf hiveConf;

  private int minWorkerThreads;
  private int maxWorkerThreads;
  private int requestTimeout;

  public ThriftCLIServiceDelegator() {
      super(null); // I only want to derive from ThriftCLIServiceDelegator to be able to use AuthFact
      this.sessions = new SessionManagerDelegator();
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    this.hiveConf = hiveConf;


    try {
        String host = System.getenv("MULTITENANT_DELEGATE_HOST");
        if (host == null) {
            host = hiveConf.get("multitenant.delegate.host");
        }
        String strPort = System.getenv("MULTITENANT_DELEGATE_PORT");
        if(strPort == null) {
            strPort = hiveConf.get("multitenant.delegate.port");
        }
        int port = Integer.parseInt(strPort);
        TSocket transport = new TSocket(host,port);
        transport.setTimeout(60000);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        delegate = new TCLIService.Client(protocol);  
        transport.open();
        LOG.info("ThriftCLIServiceDelegator connected to remote Impala daemon on " + host + ":" + strPort);
    }
    catch(Exception e) {
        LOG.error("Unable to initialize delegate Impala server");
        LOG.error(e.toString());
    }

    this.remoteAccessControl = new RemoteAccessControl(hiveConf);

    instanceName = System.getenv("MULTITENANT_INSTANCE");
    if (instanceName == null) {
        instanceName = hiveConf.get("multitenant.instance");
    }
    LOG.info("Instance name: " + instanceName);

    restrictDatabase = System.getenv("MULTITENANT_DATABASE");
    if(restrictDatabase == null) {
        restrictDatabase = hiveConf.get("multitenant.database");
    }
    LOG.info("Restrict to database: " + restrictDatabase);

    super.init(hiveConf);
  }

  @Override
  public synchronized void start() {
    super.start();
    if (!isStarted && !isEmbedded) {
      new Thread(this).start();
      isStarted = true;
    }
  }

  @Override
  public synchronized void stop() {
    if (isStarted && !isEmbedded) {
      server.stop();
      isStarted = false;
    }
    super.stop();
  }

  private String getIpAddress() {
    if(hiveAuthFactory != null) {
      return hiveAuthFactory.getIpAddress();
    }
    return SessionManager.getIpAddress();
  }
  
  @Override
  public TOpenSessionResp OpenSession(TOpenSessionReq req) throws TException {
      TOpenSessionResp resp = delegate.OpenSession(req);

      String username = req.getUsername();
      String password = req.getPassword();
      String ip = null;

      if (hiveAuthFactory != null && hiveAuthFactory.getRemoteUser() != null) {
        username = hiveAuthFactory.getRemoteUser();
        ip = hiveAuthFactory.getIpAddress();
      } else {
        username = SessionManager.getUserName();
      }
      if(ip==null) {
          ip = getIpAddress();
      }
      if (username == null) {
        username = req.getUsername();
      }
      if (username == null || username.trim().length() == 0) {
          username = instanceName;
      }
      LOG.trace("OpenSession from user " + username + " on ip " + ip);
      sessions.addSession(username,password,ip,resp.getSessionHandle());
      return resp;
  }

  @Override
  public TCloseSessionResp CloseSession(TCloseSessionReq req) throws TException {
      return delegate.CloseSession(req);
  }

  @Override
  public TGetInfoResp GetInfo(TGetInfoReq req) throws TException {
      return delegate.GetInfo(req);
  }
    
  private String overwriteDatabase(String statement) {
      if(restrictDatabase == null) {
          return statement;
      }
      String sql = statement.toLowerCase();
      if (sql.contains("use ")) {
          return "USE " + restrictDatabase;
      }
      if(sql.contains("show ")) {
          if (sql.contains("databases")) {
              return "SHOW DATABASES LIKE `" + restrictDatabase + "`";
          }
          if (sql.contains("schemas")) {
              return "SHOW SCHEMAS LIKE `" + restrictDatabase + "`";
          }
          return sql.replace("default",restrictDatabase);
      }
      return sql.replace("default",restrictDatabase);
  }

  @Override
  public TExecuteStatementResp ExecuteStatement(TExecuteStatementReq req) throws TException {
      TSessionHandle sessionHandle = req.getSessionHandle();
      SessionManagerDelegator.SessionInfo  sessionInfo = sessions.getSession(sessionHandle);
      if(sessionInfo != null) {
          String sql = req.getStatement();
          sql = overwriteDatabase(sql);
          LOG.info("Execute [" + sql + "] for user [" + sessionInfo.username + "] at " + sessionInfo.ip);
          if (remoteAccessControl.hasAccess(sessionInfo.username, sql) == true) {
              req.setStatement(sql);
              return delegate.ExecuteStatement(req);
          }
          else {
              LOG.trace("User " + sessionInfo.username + " has no permission to execute [" + sql + "]");
          }
      }
      TStatus status = new TStatus();
      status.setStatusCode( TStatusCode.ERROR_STATUS );
      status.setErrorMessage("No permission to execute statement");
      status.setSqlState("No permission");
      status.setErrorCode(42000);
      TExecuteStatementResp response = new TExecuteStatementResp();
      response.setStatus(status);
      return response;
  }

  @Override
  public TGetTypeInfoResp GetTypeInfo(TGetTypeInfoReq req) throws TException {
      return delegate.GetTypeInfo(req);
  }

  @Override
  public TGetCatalogsResp GetCatalogs(TGetCatalogsReq req) throws TException {
      return delegate.GetCatalogs(req);
  }

  @Override
  public TGetSchemasResp GetSchemas(TGetSchemasReq req) throws TException {
      return delegate.GetSchemas(req);
  }

  @Override
  public TGetTablesResp GetTables(TGetTablesReq req) throws TException {
      return delegate.GetTables(req);
  }

  @Override
  public TGetTableTypesResp GetTableTypes(TGetTableTypesReq req) throws TException {
      return delegate.GetTableTypes(req);
  }

  @Override
  public TGetColumnsResp GetColumns(TGetColumnsReq req) throws TException {
      return delegate.GetColumns(req);
  }

  @Override
  public TGetFunctionsResp GetFunctions(TGetFunctionsReq req) throws TException {
      return delegate.GetFunctions(req);
  }

  @Override
  public TGetOperationStatusResp GetOperationStatus(TGetOperationStatusReq req) throws TException {
      return delegate.GetOperationStatus(req);
  }

  @Override
  public TCancelOperationResp CancelOperation(TCancelOperationReq req) throws TException {
      return delegate.CancelOperation(req);
  }

  @Override
  public TCloseOperationResp CloseOperation(TCloseOperationReq req) throws TException {
      return delegate.CloseOperation(req);
  }

  @Override
  public TGetResultSetMetadataResp GetResultSetMetadata(TGetResultSetMetadataReq req)
      throws TException {
      return delegate.GetResultSetMetadata(req);
  }

  @Override
  public TFetchResultsResp FetchResults(TFetchResultsReq req) throws TException {
      return delegate.FetchResults(req);
  }

  @Override
  public TGetLogResp GetLog(TGetLogReq req) throws TException {
      return delegate.GetLog(req);
  }

  @Override
  public TGetDelegationTokenResp GetDelegationToken(TGetDelegationTokenReq req)
      throws TException {
      return delegate.GetDelegationToken(req);
  }

  @Override
  public TCancelDelegationTokenResp CancelDelegationToken(TCancelDelegationTokenReq req)
      throws TException {
      return delegate.CancelDelegationToken(req);
  }

  @Override
  public TRenewDelegationTokenResp RenewDelegationToken(TRenewDelegationTokenReq req)
      throws TException {
      return delegate.RenewDelegationToken(req);
  }

  @Override
  public void run() {
    try {
      hiveAuthFactory = new HiveAuthFactory();
      TTransportFactory  transportFactory = hiveAuthFactory.getAuthTransFactory();
      TProcessorFactory processorFactory = hiveAuthFactory.getAuthProcFactory(this);

      String portString = System.getenv("HIVE_SERVER2_THRIFT_PORT");
      if (portString != null) {
        portNum = Integer.valueOf(portString);
      } else {
        portNum = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT);
      }

      String hiveHost = System.getenv("HIVE_SERVER2_THRIFT_BIND_HOST");
      if (hiveHost == null) {
        hiveHost = hiveConf.getVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST);
      }

      if (hiveHost != null && !hiveHost.isEmpty()) {
        serverAddress = new InetSocketAddress(hiveHost, portNum);
      } else {
        serverAddress = new  InetSocketAddress(portNum);
      }


      minWorkerThreads = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_MIN_WORKER_THREADS);
      maxWorkerThreads = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_MAX_WORKER_THREADS);
      requestTimeout = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_LOGIN_TIMEOUT);


      TServerSocket serverSocket = null;
      if (!hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_USE_SSL)) {
        serverSocket = HiveAuthFactory.getServerSocket(hiveHost, portNum);
      } else {
        String keyStorePath = hiveConf.getVar(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH).trim();
        if (keyStorePath.isEmpty()) {
          throw new IllegalArgumentException(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH.varname + " Not configured for SSL connection");
        }
        serverSocket = HiveAuthFactory.getServerSSLSocket(hiveHost, portNum, keyStorePath, hiveConf.getVar(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PASSWORD));
      }
      TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(serverSocket)
      .processorFactory(processorFactory)
      .transportFactory(transportFactory)
      .protocolFactory(new TBinaryProtocol.Factory())
      .minWorkerThreads(minWorkerThreads)
      .maxWorkerThreads(maxWorkerThreads)
      .requestTimeout(requestTimeout);

      server = new TThreadPoolServer(sargs);

      LOG.info("ThriftCLIServiceDelegator listening on " + serverAddress);

      server.serve();
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  /**
   * If the proxy user name is provided then check privileges to substitute the user.
   * @param realUser
   * @param sessionConf
   * @param ipAddress
   * @return
   * @throws HiveSQLException
   */
  private String getProxyUser(String realUser, Map<String, String> sessionConf, String ipAddress)
      throws HiveSQLException {
    if (sessionConf == null || !sessionConf.containsKey(HiveAuthFactory.HS2_PROXY_USER)) {
      return realUser;
    }

    // Extract the proxy user name and check if we are allowed to do the substitution
    String proxyUser = sessionConf.get(HiveAuthFactory.HS2_PROXY_USER);
    if (!hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ALLOW_USER_SUBSTITUTION)) {
      throw new HiveSQLException("Proxy user substitution is not allowed");
    }

    // If there's no authentication, then directly substitute the user
    if (HiveAuthFactory.AuthTypes.NONE.toString().
        equalsIgnoreCase(hiveConf.getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION))) {
      return proxyUser;
    }

    // Verify proxy user privilege of the realUser for the proxyUser
    try {
      UserGroupInformation sessionUgi;
      if (!ShimLoader.getHadoopShims().isSecurityEnabled()) {
        sessionUgi = ShimLoader.getHadoopShims().createProxyUser(realUser);
      } else {
        sessionUgi = ShimLoader.getHadoopShims().createRemoteUser(realUser, null);
      }
      ShimLoader.getHadoopShims().
      authorizeProxyAccess(proxyUser, sessionUgi, ipAddress, hiveConf);
      return proxyUser;
    } catch (IOException e) {
      throw new HiveSQLException("Failed to validate proxy privilage of " + realUser +
          " for " + proxyUser, e);
    }
  }
}
