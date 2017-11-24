/*
 * Copyright 2017 Manganit.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.manganit.half.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
// Now fails since I switched to Hortonworks packages
//import org.apache.hive.jdbc.Utils;
//import org.apache.hive.jdbc.Utils.JdbcConnectionParams;
import static java.util.stream.Collectors.joining;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.jdbc.ZooKeeperHiveClientException;

/**
 *
 * @author Damien Claveau
 * 
 */

public class HiveConnectionBuilder 
//  extends JdbcConnectionParams 
{
  private Configuration conf;
  // BEGIN SINCE HDP
  private String host = null;
  private int port = 0;
  private String dbName = DEFAULT_DATABASE;
  private Map<String,String> hiveConfs = new LinkedHashMap<String,String>();
  private Map<String,String> hiveVars = new LinkedHashMap<String,String>();
  private Map<String,String> sessionVars = new LinkedHashMap<String,String>();
  private String zooKeeperEnsemble = null;
  // END SINCE HDP
    
  public static final String URL_PREFIX = "jdbc:hive2://";
  public static final String DEFAULT_PORT = "10000";
  public static final String DEFAULT_DATABASE = "default";
  public static final String URI_JDBC_PREFIX = "jdbc:";
  public static final String URI_HIVE_PREFIX = "hive2:";
  public static final String HIVE_SERVER2_RETRY_KEY = "hive.server2.retryserver";
  public static final String HIVE_SERVER2_RETRY_TRUE = "true";
  public static final String HIVE_SERVER2_RETRY_FALSE = "false";
  public static final String AUTH_TYPE = "auth";
  public static final String AUTH_QOP_DEPRECATED = "sasl.qop";
  public static final String AUTH_QOP = "saslQop";
  public static final String AUTH_SIMPLE = "noSasl";
  public static final String AUTH_TOKEN = "delegationToken";
  public static final String AUTH_USER = "user";
  public static final String AUTH_PRINCIPAL = "principal";
  public static final String AUTH_PASSWD = "password";
  public static final String AUTH_KERBEROS_AUTH_TYPE = "kerberosAuthType";
  public static final String AUTH_KERBEROS_AUTH_TYPE_FROM_SUBJECT = "fromSubject";
  public static final String ANONYMOUS_USER = "anonymous";
  public static final String ANONYMOUS_PASSWD = "anonymous";
  public static final String USE_SSL = "ssl";
  public static final String USE_SSL_TRUE = "true";
  public static final String SSL_TRUST_STORE = "sslTrustStore";
  public static final String SSL_TRUST_STORE_PASSWORD = "trustStorePassword";
  public static final String TRANSPORT_MODE = "transportMode";
  public static final String DEFAULT_TRANSPORT_MODE = "http";
  public static final String HTTP_PATH = "httpPath";
  public static final String DEFAULT_HTTP_PATH = "cliservice";
  public static final String SERVICE_DISCOVERY_MODE = "serviceDiscoveryMode";
  public static final String SERVICE_DISCOVERY_MODE_NONE = "none";
  public static final String SERVICE_DISCOVERY_MODE_ZOOKEEPER = "zooKeeper";
  public static final String ZOOKEEPER_NAMESPACE = "zooKeeperNamespace";
  public static final String ZOOKEEPER_DEFAULT_NAMESPACE = "hiveserver2";
  public static final String COOKIE_AUTH = "cookieAuth";
  public static final String COOKIE_AUTH_FALSE = "false";
  public static final String COOKIE_NAME = "cookieName";
  public static final String DEFAULT_COOKIE_NAMES_HS2 = "hive.server2.auth";
  
  // BEGIN SINCE HDP
  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getDbName() {
    return dbName;
  }

  public Map<String, String> getHiveConfs() {
    return hiveConfs;
  }

  public Map<String, String> getHiveVars() {
    return hiveVars;
  }

  public Map<String, String> getSessionVars() {
    return sessionVars;
  }

  public String getZooKeeperEnsemble() {
    return zooKeeperEnsemble;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public void setHiveConfs(Map<String, String> hiveConfs) {
    this.hiveConfs = hiveConfs;
  }

  public void setHiveVars(Map<String, String> hiveVars) {
    this.hiveVars = hiveVars;
  }

  public void setSessionVars(Map<String, String> sessionVars) {
    this.sessionVars = sessionVars;
  }

  public void setZooKeeperEnsemble(String zooKeeperEnsemble) {
    this.zooKeeperEnsemble = zooKeeperEnsemble;
  }
  // END SINCE HDP
  
  
  public HiveConnectionBuilder()  {
    this.conf = null;
  }
    
  public HiveConnectionBuilder(Configuration conf)  {
    this.conf = conf;
  }
  
  public String buildConnectionString() {
    setValuesFromEnvironment();
    if (conf != null) {
      setValuesFromConfiguration();
    }
    setDefaultValues();
    this.setSessionVars(sessionVars); // Path
    this.setHiveConfs(hiveConfs); // Query
    this.setHiveVars(hiveVars); // Fragment
    
    //URI is formed like Scheme://user@host:port/Path?Query#Fragment
    //for Hive JDBC it is jdbc:hive2://Autority/SessionVars?HiveConfs#HiveVars
    String scheme = URI_JDBC_PREFIX + URI_HIVE_PREFIX;
    String authority = (this.getZooKeeperEnsemble()  != null ) ? this.getZooKeeperEnsemble() : this.getHost()+":"+Integer.toString(this.getPort());
    String path = mapToString(this.getSessionVars());
    String query = mapToString(this.getHiveConfs());
    String fragment = mapToString(this.getHiveVars());
    String connection = String.format("%s//%s/%s;%s?%s#%s", scheme, authority, this.getDbName(), path, query, fragment);
// BEGIN SINCE HDP
//    try {
//      JdbcConnectionParams test = Utils.parseURL(connection);
//      return connection;
//    } catch (SQLException ex) {
//      ex.printStackTrace();
//      return "";
//    } catch (ZooKeeperHiveClientException ex) {
//      ex.printStackTrace();
//      return "";
//    }
// END SINCE HDP
    return connection;
  }

  private void setValue(Map<String, String> map, String key, String value) {
    if (!map.containsKey(key) && (value!= null)) {
      map.put(key, value);
    }
  }
  
  private String mapToString(Map<String, String> map) {
    return map.entrySet().stream().map(e -> e.getKey()+"="+e.getValue()).collect(joining(";"));
  }
  
  private void setDefaultValues() {
    if (this.getDbName() == null) { this.setDbName(/*Utils.*/DEFAULT_DATABASE); }
    if (this.getHost()   == null) { this.setHost("localhost"); }
    if (this.getPort()   <= 0)    { this.setPort(Integer.parseInt(/*Utils.*/DEFAULT_PORT)); }
    
    if (this.getZooKeeperEnsemble() != null) {
      setValue(sessionVars, ZOOKEEPER_NAMESPACE, ZOOKEEPER_DEFAULT_NAMESPACE);
      setValue(sessionVars, SERVICE_DISCOVERY_MODE, SERVICE_DISCOVERY_MODE_ZOOKEEPER);
    }
    else {
      setValue(sessionVars, SERVICE_DISCOVERY_MODE, SERVICE_DISCOVERY_MODE_NONE);
    }
    setValue(sessionVars, TRANSPORT_MODE, DEFAULT_TRANSPORT_MODE);
    
    if (sessionVars.get(TRANSPORT_MODE).compareTo(DEFAULT_TRANSPORT_MODE) == 0) {
      setValue(sessionVars, HTTP_PATH, DEFAULT_HTTP_PATH);
    }
  }
  
  private void setValuesFromEnvironment() {
    if (System.getProperty("hive.server.host") != null) {
      this.setHost(System.getProperty("hive.server.host"));
    }
    if (System.getProperty("hive.server.port") != null) {
      this.setPort(Integer.parseInt(System.getProperty("hive.server.port")));
    }
    if (System.getProperty("hive.zookeeper.quorum") != null) {
      this.setZooKeeperEnsemble(System.getProperty("hive.zookeeper.quorum"));
    }
    if (System.getProperty("hive.kerberos.principal") != null) {
      setValue(sessionVars, AUTH_PRINCIPAL, System.getProperty("hive.kerberos.principal"));
    }
      
    if((System.getProperty("javax.net.ssl.trustStore") != null) 
    && (System.getProperty("javax.net.ssl.trustStorePassword") != null)) {
      setValue(sessionVars, USE_SSL, USE_SSL_TRUE);
      setValue(sessionVars, SSL_TRUST_STORE, System.getProperty("javax.net.ssl.trustStore"));
      setValue(sessionVars, SSL_TRUST_STORE_PASSWORD, System.getProperty("javax.net.ssl.trustStorePassword"));
    }
}
  
  private void setValuesFromConfiguration() {
    setValue(sessionVars, USE_SSL, this.conf.get("hive.server2.use.SSL"));
    setValue(sessionVars, AUTH_PRINCIPAL, this.conf.get("hive.metastore.kerberos.principal"));
    setValue(sessionVars, TRANSPORT_MODE, this.conf.get("hive.server2.transport.mode"));
    setValue(sessionVars, ZOOKEEPER_NAMESPACE, this.conf.get("hive.server2.zookeeper.namespace"));
    
    if (this.conf.get("hive.zookeeper.quorum") != null) {
      this.setZooKeeperEnsemble(this.conf.get("hive.zookeeper.quorum"));
    }
    
    if((this.conf.get("hive.server2.support.dynamic.service.discovery")!= null)
    && "true".equals(this.conf.get("hive.server2.support.dynamic.service.discovery"))
    && (getZooKeeperEnsemble() != null)
    && (sessionVars.get(ZOOKEEPER_NAMESPACE) != null)) {
      setValue(sessionVars, SERVICE_DISCOVERY_MODE, SERVICE_DISCOVERY_MODE_ZOOKEEPER);
    }
    
    if((sessionVars.get(TRANSPORT_MODE) != null) 
    && (sessionVars.get(TRANSPORT_MODE).compareTo("http") == 0)) {
      setValue(sessionVars, HTTP_PATH, this.conf.get("hive.server2.thrift.http.path"));
      if ( this.conf.get("hive.server2.thrift.http.port")!= null) {
        this.setPort(Integer.parseInt( this.conf.get("hive.server2.thrift.http.port"))); 
      }
    }
    
    if((sessionVars.get(TRANSPORT_MODE) != null) 
    && (sessionVars.get(TRANSPORT_MODE).compareTo("binary") == 0)) {
      this.setHost(this.conf.get("hive.server2.thrift.bind.host"));
      if ( this.conf.get("hive.server2.thrift.http.port")!= null) {
        this.setPort(Integer.parseInt( this.conf.get("hive.server2.thrift.port"))); 
      } 
    }
  }
 
}
