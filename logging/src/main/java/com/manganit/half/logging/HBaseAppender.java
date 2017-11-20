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

package com.manganit.half.logging;

import com.manganit.half.util.NamedThreadFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

/**
 *
 * @author Damien Claveau
 * 
 */

public class HBaseAppender extends AppenderSkeleton implements Runnable {

    private int batchSize = 10;
    private int period = 1000;
    private String hbLogName = "test";
    private String hbLogFamily = "bg";
    private Queue<LoggingEvent> loggingEvents;
    private ScheduledExecutorService executor;
    private ScheduledFuture<?> task;
    private Configuration conf;
    private HConnection hconnection;
    private HTableInterface htable;

    /**
     * activateOptions
     */
    @Override
    public void activateOptions() {
        try {
            super.activateOptions();
            //time-based thread that flushes events to HBase
            executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("HBaseAppender"));
            //internal queue of events
            loggingEvents = new ConcurrentLinkedQueue<>();
            //run every [period] time-interval
            task = executor.scheduleWithFixedDelay(this, period, period, TimeUnit.MILLISECONDS);
            System.out.println("ActivateOptions ok!");
        } catch (Exception e) {
            System.err.println("Error during activateOptions: " + e);
        }
    }

    /**
     * @return success or failure
     *
     */
    private boolean initHbase() {
        try {
            if (conf == null) {
                //read classpath hbase-site.xml
                conf = HBaseConfiguration.create();
                //htable connection
                hconnection = HConnectionManager.createConnection(conf);
                htable = hconnection.getTable(hbLogName);
                System.out.println("Init Hbase OK!");
            }
            return true;
        } catch (Exception e) {
            task.cancel(false);
            executor.shutdown();
            System.err.println("Init Hbase fail !");
            return false;
        }
    }

    @Override
    public void run() {
        if (conf == null || htable == null) {
            initHbase();
        }
        try {
            //
            if (batchSize <= loggingEvents.size()) {
                LoggingEvent event;
                List<Put> logs = new ArrayList<>();
                // Consume events from queue
                while ((event = loggingEvents.poll()) != null) {
                    try {
                        // Row key balanced distribution among Region Servers
                        Put log = new Put((event.getThreadName() + event.getLevel().toString() + System.currentTimeMillis()).getBytes());
                        //
                        log.add(hbLogFamily.getBytes(), "log".getBytes(), layout.format(event).getBytes());
                        logs.add(log);
                    } catch (Exception e) {
                        System.err.println("Error logging put " + e);
                    }
                }
                // Flush and insert batch of events
                if (logs.size() > 0) htable.put(logs);
            }
        } catch (Exception e) {
            System.err.println("Error run " + e);
        }
    }

    /**
     * 
     *
     * @param loggingEvent Log4J event
     */
    @Override
    protected void append(LoggingEvent loggingEvent) {
        try {
            populateEvent(loggingEvent);
            //
            loggingEvents.add(loggingEvent);
        } catch (Exception e) {
            System.err.println("Error populating event and adding to queue" + e);
        }
    }

    /**
     * 
     *
     * @param event Log4J event
     */
    protected void populateEvent(LoggingEvent event) {
        event.getThreadName();
        event.getRenderedMessage();
        event.getNDC();
        event.getMDCCopy();
        event.getThrowableStrRep();
        event.getLocationInformation();
    }

    /**
     * Close and free resources
     */
    @Override
    public void close() {
        try {
            task.cancel(false);
            executor.shutdown();
            htable.close();
            hconnection.close();
        } catch (IOException e) {
            System.err.println("Error close " + e);
        }
    }

    /**
     * requiresLayout
     * @return default true
     */
    @Override
    public boolean requiresLayout() {
        return true;
    }

    /**
     * 
     *
     * @param batchSize Number of events before flush
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    /**
     * 
     *
     * @param period Number of seconds ellapsed before flush
     */
    public void setPeriod(int period) {
        this.period = period;
    }

    /**
     * 
     *
     * @param hbLogName HBase table name
     */
    public void setHbLogName(String hbLogName) {
        this.hbLogName = hbLogName;
    }

    /**
     * 
     * @param hbLogFamily HBase column family name
     */
    public void setHbLogFamily(String hbLogFamily) {
        this.hbLogFamily = hbLogFamily;
    }
}