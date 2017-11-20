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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author Damien Claveau
 * 
 */

public class HBaseClient {

  private Configuration conf = null;

  /**
   * Default Initialization
   */
  public HBaseClient() {
    this(HBaseConfiguration.create());
  }

  public HBaseClient(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Put (or insert) a row
   *
   * @param tableName tableName
   * @param rowKey rowKey
   * @param family family
   * @param qualifier qualifier
   * @param value value
   * @throws Exception Exception
   */
  public void addRecord(String tableName, String rowKey,
          String family, String qualifier, String value) throws Exception {
    try {
      HTable table = new HTable(conf, tableName);
      Put put = new Put(Bytes.toBytes(rowKey));
      put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
              .toBytes(value));
      table.put(put);
      System.out.println("insert record " + rowKey + " to table "
              + tableName + " ok.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Delete a row
   *
   * @param tableName tableName
   * @param rowKey rowKey
   * @throws IOException IOException
   */
  public void delRecord(String tableName, String rowKey)
          throws IOException {
    HTable table = new HTable(conf, tableName);
    List<Delete> list = new ArrayList<Delete>();
    Delete del = new Delete(rowKey.getBytes());
    list.add(del);
    table.delete(list);
    System.out.println("del record " + rowKey + " ok.");
  }

  /**
   * Get a row
   *
   * @param tableName tableName
   * @param rowKey rowKey
   * @throws IOException IOException
   */
  public void printOneRecord(String tableName, String rowKey) throws IOException {
    HTable table = new HTable(conf, tableName);
    Get get = new Get(rowKey.getBytes());
    Result rs = table.get(get);
    for (KeyValue kv : rs.raw()) {
      System.out.print(new String(kv.getRow()) + " ");
      System.out.print(new String(kv.getFamily()) + ":");
      System.out.print(new String(kv.getQualifier()) + " ");
      System.out.print(kv.getTimestamp() + " ");
      System.out.println(new String(kv.getValue()));
    }
  }

  /**
   * Scan (or list) a table
   *
   * @param tableName tableName
   */
  public void printAllRecords(String tableName) {
    try {
      HTable table = new HTable(conf, tableName);
      Scan s = new Scan();
      ResultScanner ss = table.getScanner(s);
      for (Result r : ss) {
        for (KeyValue kv : r.raw()) {
          System.out.print(new String(kv.getRow()) + " ");
          System.out.print(new String(kv.getFamily()) + ":");
          System.out.print(new String(kv.getQualifier()) + " ");
          System.out.print(kv.getTimestamp() + " ");
          System.out.println(new String(kv.getValue()));
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
