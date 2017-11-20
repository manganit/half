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

/**
 *
 * @author Damien Claveau
 * 
 */

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.util.Properties;
import org.apache.log4j.Logger;

public class HiveJdbcClient {

  private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
  private static final String defaultDb = "default";
  private static Logger logger = Logger.getLogger(HiveJdbcClient.class);
  
  //!connect jdbc:hive2://host.domain:10000/default;principal=hive/_HOST@MY_REALM
  // Returns a wellformed connection string
  //   jdbcUrl example   : "jdbc:hive2://host.domain:10000"
  //   principal example : "hive/host.domain@MY.REALM"

  public static String getConnectionString(String jdbcUrl, String database, String principal, String queue, boolean ssl, String trustStore, String trustStorePassword, boolean zookeeperDiscovery) throws SQLException {
    String jdbcStr;
    if (database != null) {
      jdbcStr = jdbcUrl + "/" + database;
    } else {
      jdbcStr = jdbcUrl + "/" + defaultDb;
    }

    jdbcStr = jdbcStr + ";transportMode=http;httpPath=cliservice";
    
    if (zookeeperDiscovery) {
      jdbcStr = jdbcStr + ";serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
    }

    if (ssl) {
      jdbcStr = jdbcStr + ";ssl=true";
      if (trustStore != null) {
        jdbcStr = jdbcStr + ";sslTrustStore=" + trustStore;
      }
      if (trustStorePassword != null) {
        jdbcStr = jdbcStr + ";trustStorePassword=" + trustStorePassword;
      }
    }
    
    if (principal != null) {
      jdbcStr = jdbcStr + ";principal=" + principal;
    }

    if (queue != null) {
      jdbcStr = jdbcStr + String.format("?mapreduce.job.queuename=%s;tez.queue.name=%s", queue, queue);
    }

    return jdbcStr;
  }

  public static Connection getConnection(String jdbcStr, Properties info) throws SQLException {
    try {
      logger.debug("Check the Jdbc drivers.");
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      return null;
    }
    logger.info("Hive connection string : " + jdbcStr);
    Connection connection = DriverManager.getConnection(jdbcStr, info);
    return connection;
  }
  
  public static Connection getConnection(HiveConnectionBuilder builder, Properties info) throws SQLException {
    return getConnection(builder.buildConnectionString(), info);
  }
  
  public static Connection getConnection(HiveConnectionBuilder builder) throws SQLException {
    Properties info = new Properties();
    return getConnection(builder, info);
  }
  
  
  // Create a connection on a secured Hive database
  //   jdbcUrl example   : "jdbc:hive2://host.domain:10000"
  //                    or "jdbc:hive2://host1.domain:2181,host2.domain:2181,host3.domain:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;"
  //   principal example : "hive/host.domain@MY.REALM"
  public static Connection getConnection(String jdbcUrl, String database, String principal, String queue, boolean ssl, String trustStore, String trustStorePassword, boolean zookeeperDiscovery) throws SQLException {
    try {
      logger.debug("Check the Jdbc drivers.");
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      return null;
    }

    String jdbcStr = getConnectionString(jdbcUrl, database, principal, queue, ssl, trustStore, trustStorePassword, zookeeperDiscovery);
    logger.debug("Hive connection string : " + jdbcStr);

    Connection connection = DriverManager.getConnection(jdbcStr);
    return connection;
  }

  // Create a connection on a secured Hive database
  public static Connection getConnection(String jdbcUrl, String database, String principal) throws SQLException {
    return getConnection(jdbcUrl, database, principal, null, false, null, null, false);
  }

  // Create a connection on a non-secured Hive database
  public static Connection getConnection(String jdbcUrl, String database) throws SQLException {
    return getConnection(jdbcUrl, database, null, null, false, null, null, false);
  }

  public static void closeConnection(Connection connection) {
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (Exception e) {
      logger.error("closeConnection error : " + e.getMessage());
    }
  }
  
}
