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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 *
 * @author Damien Claveau
 * 
 */

public class HBaseAdminClient {

  private Configuration conf = null;

  /**
   * Default Initialization
   */
  public HBaseAdminClient() {
    this(HBaseConfiguration.create());
  }

  /**
   *
   * @param conf Configuration
   */
  public HBaseAdminClient(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Print all tables
   *
   * @param regex filter
   * @throws java.lang.Exception Exception
   */
  public void printTables(String regex)
          throws Exception {
    HBaseAdmin admin = new HBaseAdmin(conf);
    // Getting all the list of tables using HBaseAdmin object
    HTableDescriptor[] tableDescriptor = admin.listTables(regex);
    // printing all the table names.
    for (HTableDescriptor tableDescriptor1 : tableDescriptor) {
      System.out.println(tableDescriptor1.getNameAsString());
    }
  }

  /**
   * Create a table
   *
   * @param tableName  table Name
   * @param familys column familys
   * @throws java.lang.Exception Exception
   */
  public void creatTable(String tableName, String[] familys)
          throws Exception {
    HBaseAdmin admin = new HBaseAdmin(conf);
    if (admin.tableExists(tableName)) {
      System.out.println("table already exists!");
    } else {
      HTableDescriptor tableDesc = new HTableDescriptor(tableName);
      for (String family : familys) {
        tableDesc.addFamily(new HColumnDescriptor(family));
      }
      admin.createTable(tableDesc);
      System.out.println("create table " + tableName + " ok.");
    }
  }

  /**
   * Delete a table
   *
   * @param tableName table Name
   * @throws java.lang.Exception Exception
   */
  public void deleteTable(String tableName) throws Exception {
    try {
      HBaseAdmin admin = new HBaseAdmin(conf);
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
      System.out.println("delete table " + tableName + " ok.");
    } catch (MasterNotRunningException e) {
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      e.printStackTrace();
    }
  }

}
