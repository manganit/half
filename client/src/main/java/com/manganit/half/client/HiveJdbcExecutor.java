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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import org.apache.hive.jdbc.HiveStatement;
import org.apache.log4j.Logger;

/**
 *
 * @author Damien Claveau
 * 
 */

public class HiveJdbcExecutor {

  private final static Logger logger = Logger.getLogger(HiveJdbcExecutor.class);
  private final Connection connection;
  private Object incrementalLogs;
  private static final int DEFAULT_QUERY_PROGRESS_INTERVAL = 1000;
  private static final int DEFAULT_QUERY_PROGRESS_THREAD_TIMEOUT = 10 * 1000;
  private boolean LoggingEnabled = false;

  public HiveJdbcExecutor(Connection connection) {
    this.connection = connection;
  }

  /**
   * Get the value of LoggingEnabled
   *
   * @return the value of LoggingEnabled
   */
  public boolean isLoggingEnabled() {
    return LoggingEnabled;
  }

  /**
   * Set the value of LoggingEnabled
   *
   * @param LoggingEnabled new value of LoggingEnabled
   */
  public void setLoggingEnabled(boolean LoggingEnabled) {
    if (this.connection != null && this.LoggingEnabled != LoggingEnabled) {
      try {
        Statement setStmt = connection.createStatement();
        if (LoggingEnabled) {
          setStmt.execute("set hive.server2.logging.operation.enabled = true");
        } else {
          setStmt.execute("set hive.server2.logging.operation.enabled = false");
        }
        setStmt.close();
        this.LoggingEnabled = LoggingEnabled;
      } catch (SQLException ex) {
        logger.error(ex);
      }
    }    
  }

  public ResultSet executeQuery(String sql) {
    long startTime = System.currentTimeMillis();
    logger.info("HiveUtil.execute:sql =" + sql);
    ResultSet res = null;
    try {
      HiveStatement stmt = (HiveStatement) connection.createStatement();
      //Thread async = startLoggingThread(stmt);
      res = stmt.executeQuery(sql);
      //stopLoggingThread(async, stmt);
      if (LoggingEnabled) {
        //List<String> logs = stmt.getQueryLog(false, 10000);
        for (String log : stmt.getQueryLog(false, 10000)) {
          logger.info(log);
        }
      }
    } catch (Exception e) {
      logger.error("HiveUtil.executeQuery:error = " + e.getMessage());
    }
    logger.debug("HiveUtil.executeQuery:all time =" + (System.currentTimeMillis() - startTime));
    return res;
  }

  private Thread startLoggingThread(HiveStatement statement) {
    final List<String> incrementalLogs = new ArrayList<String>();
    Runnable logThread = new Runnable() {
      @Override
      public void run() {
        while (true /* statement.hasMoreLogs() */) {
          try {
            //incrementalLogs.addAll(statement.getQueryLog());
            Thread.sleep(500);
          //} catch (SQLException e) {
            //  logger.error("Failed getQueryLog. Error message: " + e.getMessage());
          } catch (InterruptedException e) {
            logger.error("Getting log thread is interrupted. Error message: " + e.getMessage());
          }
        }
      }
    };
    Thread thread = new Thread(logThread);
    thread.setDaemon(true);
    thread.start();
    return thread;
  }

  private void stopLoggingThread(Thread thread, HiveStatement statement) {
    thread.interrupt();
    try {
      thread.join(10000);
    } catch (InterruptedException ex) {
      java.util.logging.Logger.getLogger(HiveJdbcExecutor.class.getName()).log(Level.SEVERE, null, ex);
    }
    // fetch remaining logs
    List<String> remainingLogs = null;
    do {
      //remainingLogs = statement.getQueryLog();
      //incrementalLogs.addAll(remainingLogs);
    } while (remainingLogs.size() > 0);
  }

  private Runnable createLogRunnable(Statement statement) {
    if (statement instanceof HiveStatement) {
      final HiveStatement hiveStatement = (HiveStatement) statement;
      Runnable runnable = new Runnable() {
        @Override
        public void run() {
          while (hiveStatement.hasMoreLogs()) {
            try {
              // fetch the log periodically and output to beeline console
              for (String log : hiveStatement.getQueryLog()) {
                logger.info(log);
              }
              Thread.sleep(DEFAULT_QUERY_PROGRESS_INTERVAL);
            } catch (SQLException e) {
              logger.error(e);
              return;
            } catch (InterruptedException e) {
              logger.debug("Getting log thread is interrupted, since query is done!");
              showRemainingLogsIfAny(hiveStatement);
              return;
            }
          }
        }
      };
      return runnable;
    } else {
      logger.debug("The statement instance is not HiveStatement type: " + statement.getClass());
      return new Runnable() {
        @Override
        public void run() {
        // do nothing.
        }
      };
    }
  }

  private void showRemainingLogsIfAny(Statement statement) {
    if (statement instanceof HiveStatement) {
      HiveStatement hiveStatement = (HiveStatement) statement;
      List<String> logs;
      do {
        try {
          logs = hiveStatement.getQueryLog();
        } catch (SQLException e) {
          logger.error(e);
          return;
        }
        for (String log : logs) {
          logger.info(log);
        }
      } while (logs.size() > 0);
    } else {
      logger.debug("The statement instance is not HiveStatement type: " + statement.getClass());
    }
  }

  public int executeUpdate(String sql) {
    long startTime = System.currentTimeMillis();
    logger.info("HiveUtil.execute:sql =" + sql);
    int res = 0;
    Connection conn = null;
    try {
      Statement stmt = connection.createStatement();
      res = stmt.executeUpdate(sql);
    } catch (Exception e) {
      logger.error("HiveUtil.executeUpdate:error = " + e.getMessage());
    }
    logger.debug("HiveUtil.executeUpdate:all time =" + (System.currentTimeMillis() - startTime));
    return res;
  }

  public void execute(String sql) throws Exception {
    long startTime = System.currentTimeMillis();
    logger.info("HiveUtil.execute:sql =" + sql);
    Connection conn = null;
    try {
      Statement stmt = connection.createStatement();
      stmt.execute(sql);
    } catch (Exception e) {
      logger.error("HiveUtil.execute:error = " + e.getMessage());
      e.printStackTrace();
      throw new Exception(e);
    }
    logger.info("HiveUtil.execute:all time =" + (System.currentTimeMillis() - startTime));
  }
  
  public void testGetQueryLog(String sql) throws Exception {
    // Verify the fetched log (from the beginning of log file)
    HiveStatement stmt = (HiveStatement)connection.createStatement();
    stmt.executeQuery(sql);
    List<String> logs = stmt.getQueryLog(false, 10000);
    stmt.close();
  
    // Verify the fetched log (incrementally)
    final HiveStatement statement = (HiveStatement)connection.createStatement();
    statement.setFetchSize(10000);
    final List<String> incrementalLogs = new ArrayList<String>();

    Runnable logThread = new Runnable() {
      @Override
      public void run() {
        while (statement.hasMoreLogs()) {
          try {
            incrementalLogs.addAll(statement.getQueryLog());
            Thread.sleep(500);
          } catch (SQLException e) {
            logger.error("Failed getQueryLog. Error message: " + e.getMessage());
          } catch (InterruptedException e) {
            logger.error("Getting log thread is interrupted. Error message: " + e.getMessage());
          }
        }
      }
    };

    Thread thread = new Thread(logThread);
    thread.setDaemon(true);
    thread.start();
    statement.executeQuery(sql);
    thread.interrupt();
    thread.join(10000);
    // fetch remaining logs
    List<String> remainingLogs;
    do {
      remainingLogs = statement.getQueryLog();
      incrementalLogs.addAll(remainingLogs);
    } while (remainingLogs.size() > 0);
    statement.close();
  }
}
