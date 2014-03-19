package org.apache.hive.service.cli.session;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.log.LogManager;
import org.apache.hive.service.cli.operation.OperationManager;

import org.apache.hive.service.cli.thrift.TSessionHandle;

/**
 * SessionManager.
 *
 */
public class SessionManagerDelegator {

       public class SessionInfo {
              public  String username;  
              public String password;
              public String ip;
              public TSessionHandle remoteHandle;
              public SessionInfo(String username, String password, String ip, TSessionHandle remoteHandle) {
                     this.username = username;
                     this.password = password;  
                     this.ip = ip;
                     this.remoteHandle = remoteHandle;
              }             
       };      

    private static final Object lock = new Object();
      
  private final Map<TSessionHandle, SessionInfo> handleToSession = new HashMap<TSessionHandle, SessionInfo>();

  public SessionManagerDelegator() {
  };

  public void addSession(String username, String password, String ip, TSessionHandle remoteHandle) {
         SessionInfo info = new SessionInfo(username, password, ip, remoteHandle);
         synchronized(lock) {
             handleToSession.put(remoteHandle,info);
         }
  } 

  public SessionInfo getSession(TSessionHandle remoteHandle) {
      SessionInfo info = null;
      synchronized(lock) {
         info = handleToSession.get(remoteHandle);
      }
      return info;
  }

  public void removeSession(TSessionHandle remoteHandle) {
      synchronized(lock) {
         handleToSession.remove(remoteHandle);
      }
  }
}
