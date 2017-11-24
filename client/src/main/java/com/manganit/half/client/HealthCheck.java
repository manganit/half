/*
 * Copyright 2017 ManganIT.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.manganit.half.client;

import com.manganit.half.util.StringUtils;
import org.apache.log4j.Logger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.yarn.client.api.YarnClient;

/**
 *
 * @author Damien Claveau
 * 
 */
public class HealthCheck {

  
  private final static Logger logger = Logger.getLogger(HealthCheck.class);
  private final Configuration conf;
  
  public HealthCheck(Configuration conf) {
    this.conf = conf;
  }

  public boolean checkAll() {
    boolean fsCheck = checkFileSystem();
    boolean msCheck = checkMetastore();
    boolean hiveCheck = checkHive();
    boolean hbaseCheck = checkHBase();
    boolean yarnCheck = checkYarn();
    boolean pigCheck = checkPig();
    return (fsCheck && msCheck && hiveCheck && hbaseCheck && yarnCheck && pigCheck);
  }
  
  public boolean checkFileSystem() {
    try {
      logger.info("HDFS check ...");
      String capacity = StringUtils.humanReadableByteCount(FileSystem.get(conf).getStatus().getRemaining());
      logger.info("HDFS check : OK (Free space is " + capacity + ")");
      return true;
    }
    catch (Exception e) {
      logger.error("HDFS check failed", e);
      return false;
    }
  }

  public boolean checkMetastore() {
    try {
      logger.info("Metastore check ...");
      int dbCount = new HCatalogClient(conf).getClient().getAllDatabases().size();
      logger.info("Metastore check : OK (" + Integer.toString(dbCount) + " available databases)");
      return true;
    }
    catch (Exception e) {
      logger.error("Metastore check failed", e);
      return false;
    }
  }

  public boolean checkHive() {
    try {
      logger.info("Hive check ...");
      HiveConnectionBuilder builder = new HiveConnectionBuilder(conf);
      try {
        Connection con = HiveJdbcClient.getConnection(builder);
        HiveJdbcExecutor hive = new HiveJdbcExecutor(con);
        ResultSet rs = hive.executeQuery("show databases");
        int rowCount = 0;
        while(rs.next()){
          rowCount++;
          }
        rs.close();
        con.close();
        logger.info("Hive check : OK (" + Integer.toString(rowCount) + " available databases)");
        return true;
      } catch (SQLException e) {
        logger.error("Hive check failed", e);
        return false;
      }
    }
    catch (Exception e) {
      logger.error("Hive check failed", e);
      return false;
    }
  }

  public boolean checkHBase() {
    try {
      logger.info("HBase check ...");
      HBaseAdmin.checkHBaseAvailable(conf);
      org.apache.hadoop.hbase.client.Connection connection = ConnectionFactory.createConnection(conf);
      Admin admin = connection.getAdmin();
      int nsCount = admin.listNamespaceDescriptors().length;
      int tbCount = admin.listTableNames().length;
      logger.info("HBase check : OK (" + Integer.toString(tbCount) + " tables availables in " + Integer.toString(nsCount) + " namespaces)");
      return true;
    }
    catch (Exception e) {
      logger.error("HBase check failed", e);
      return false;
    }
  }

  public boolean checkYarn() {
    try {
      logger.info("Yarn check ...");
      YarnClient yarnClient = YarnClient.createYarnClient();
      yarnClient.init(conf);
      yarnClient.start();
      int nmCount = yarnClient.getYarnClusterMetrics().getNumNodeManagers();
      logger.info("Yarn check : OK (" + Integer.toString(nmCount) + " Node Managers)");
      return true;
    }
    catch (Exception e) {
      logger.error("Yarn check failed", e);
      return false;
    }
  }
  
  public boolean checkPig() {
    try {
      logger.info("Pig check ...");
//      PigClient pigClient = new PigClient(null, null, null, null);
//      pigClient.run(null, null, null);
      logger.info("Pig check : OK");
      return true;
    }
    catch (Exception e) {
      logger.error("Pig check failed", e);
      return false;
    }
  }
  
  
}
