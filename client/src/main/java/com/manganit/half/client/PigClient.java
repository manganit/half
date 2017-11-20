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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.tools.pigstats.InputStats;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;

import org.apache.log4j.Logger;
import java.net.URL;
import java.net.URLClassLoader;

/**
 *
 * @author Damien Claveau
 *
 */

public class PigClient {

  private final Logger logger;
  private final static String libSeparator = ";";

  private PigServer pigServer;
  private final String scriptName;
  private Map<String, String> propertyFileParamaters;
  private List<String> propertyFiles;

  private List<PigStats> pigStatistics;

  public PigClient(Map<String, String> propertyFileParamaters,
                   String scriptName, String processId, String worflowId) {
      this.logger = Logger.getLogger(PigClient.class);

      this.scriptName = scriptName;
      this.propertyFileParamaters = propertyFileParamaters;

  }

  public PigClient(List<String> propertyFiles,
                   String scriptName, String processId, String worflowId) {
      this.logger = Logger.getLogger(PigClient.class);

      this.scriptName = scriptName;
      this.propertyFiles = propertyFiles;

  }

  private List<String> getJars(String jarsNames) {
      List<String> jars = Arrays.asList(jarsNames.split(libSeparator));
      return jars;
  }

  // =========================================================================================================
  // Method that actually runs the script

  /*
  prop.setProperty("mapreduce.job.queuename", queue);
  prop.setProperty("hive.metastore.uris", hiveMetastore);
  */


  public void run(Properties pigServerProps, String jobName, String jarPath) throws IOException, Exception {
      logger.debug("ENTER>>run");
      // Start of execution
      Date dateBegin = new Date();

      // Remote Pig instance, Create the MapReduce job
      start(pigServerProps, jobName, jarPath);
      executeScript();
      stop();

      // Total process time
      Date dateEnd = new Date();
      long execution_time = dateEnd.getTime() - dateBegin.getTime();
      logger.debug("Pig Execution time (s): " + execution_time / 1000);
      logger.debug("EXIT>>run");

  }

  private void start(Properties prop, String jobName, String jarPath) {

      try {
          pigServer = createPig(prop, jobName, jarPath);
          pigServer.setBatchOn();

      } catch (ExecException ex) {
          throw new RuntimeException("Cannot create pig server", ex);
      } catch (IOException ex) {
          throw new RuntimeException("Cannot create pig server", ex);
      }

  }

  private void executeScript() throws IOException, Exception {

      logger.debug("Loading the script " + scriptName);

      // Load the script
      if(this.propertyFileParamaters == null){
          pigServer.registerScript(scriptName, this.propertyFileParamaters);
      }else{
          pigServer.registerScript(scriptName, this.propertyFiles);
      }


      logger.debug("Script loaded");

      try {
          logger.debug("Excuting the script");
          List<ExecJob> jobs = pigServer.executeBatch();
          logger.debug("script executed");
          logger.debug("End of executeBatch");

          // Showing the stats
          pigStatistics = new ArrayList<PigStats>();
          logger.debug("Number of executed jobs : " + jobs.size());
          int count = 0;
          for (ExecJob execJob : jobs) {

              PigStats pigStats = execJob.getStatistics();

              pigStatistics.add(pigStats);

              logAllStats(pigStats, count++);
              logger.debug(System.getProperty("line.separator"));
              logger.debug("End of statistics");

              if (execJob.getStatus() == ExecJob.JOB_STATUS.FAILED) {
                  throw new Exception("Pig execution failed");
              } else {
                  logger.debug(execJob.getStatus().toString());
              }

          }
      } finally {
          pigServer.discardBatch();
      }
  }

  public void stop() {
      // close pig
      if (null != pigServer) {
          pigServer.shutdown();
          pigServer = null;
      }
  }

  private PigServer createPig(Properties prop, String jobName, String jarPath
  ) throws ExecException, IOException {

      logger.debug("Executing Pig in cluster mode");

      prop.setProperty("stop.on.failure", "true");
      prop.setProperty("pig.noSplitCombination", "true");

      pigServer = new PigServer(ExecType.MAPREDUCE, prop);

      pigServer.setJobName(jobName);

      registerJar(jarPath);
      logger.debug("PigServer successfully instantiated");

      return pigServer;
  }


  // Registers all the jars needed for the execution
  private void registerJar(String jarPath) throws IOException {

      // Hive jars, needed for the slave nodes
      File dir = new File(".");
      File[] filesList = dir.listFiles();
      for (File file : filesList) {
          if (file.isFile()) {
              String filename = file.getName();
              for (String jarPrefix : getJars(jarPath)) {
                  if (filename.startsWith(jarPrefix)) {
                      pigServer.registerJar(filename);
                      logger.debug("Registered Jar : " + filename);
                      System.out.println("Registered Jar : " + filename);
                  }
              }
          }
      }
      // From classpath
      ClassLoader sysClassLoader = ClassLoader.getSystemClassLoader();
      URL[] urls = ((URLClassLoader) sysClassLoader).getURLs();
      String filename;
      for (URL url : urls) {
          filename = new File(url.getFile()).getName();
          for (String jarPrefix : getJars(jarPath)) {
              if (filename.startsWith(jarPrefix)) {
                  pigServer.registerJar(url.getFile());
                  logger.debug("Registered Jar : " + url.getFile());
                  System.out.println("Registered Jar : " + url.getFile());
              }
          }
      }
  }

  // Printing all the stats
  private void logAllStats(PigStats pigStats, int count) {
      logger.debug("Job number " + count);
      logger.debug("This job contains " + pigStats.getNumberJobs()
              + " smaller jobs");

      logger.debug("--------------- EXECUTION PLAN ---------------");
      JobGraph graph = pigStats.getJobGraph();
      logger.debug(graph.toString());

      logger.debug("");
      logger.debug("---------------- GLOBAL STATS ----------------");
      logAllPigStats(pigStats);

      logger.debug("");
      logger.debug("----------------- JOB STATS ------------------");
      for (JobStats jobStat : graph.getJobList()) {
          printSingleJobStats(jobStat);
      }

  }

  // Overall stats
  private void logAllPigStats(PigStats pigStats) {
      logger.debug("BYTES_WRITTEN: "
              + Long.toString(pigStats.getBytesWritten()));
      logger.debug("DURATION (ms): " + Long.toString(pigStats.getDuration()));
      logger.debug("ERROR_CODE: " + Long.toString(pigStats.getErrorCode()));
      logger.debug("ERROR_MESSAGE: " + pigStats.getErrorMessage());
      logger.debug("FEATURES: " + pigStats.getFeatures());
      logger.debug("HADOOP_VERSION: " + pigStats.getHadoopVersion());
      logger.debug("NUMBER_JOBS: " + Long.toString(pigStats.getNumberJobs()));
      logger.debug("PIG_VERSION: " + pigStats.getPigVersion());
      logger.debug("PROACTIVE_SPILL_COUNT_OBJECTS: "
              + Long.toString(pigStats.getProactiveSpillCountObjects()));
      logger.debug("PROACTIVE_SPILL_COUNT_RECORDS: "
              + Long.toString(pigStats.getProactiveSpillCountRecords()));
      logger.debug("RECORD_WRITTEN: "
              + Long.toString(pigStats.getRecordWritten()));
      logger.debug("RETURN_CODE: " + Long.toString(pigStats.getReturnCode()));
      logger.debug("SCRIPT_ID: " + pigStats.getScriptId());
      logger.debug("SMM_SPILL_COUNT: "
              + Long.toString(pigStats.getSMMSpillCount()));
      logger.info("INPUT STATS: ");
      int number = 0;
      for (InputStats inputStats : pigStats.getInputStats()) {
          number++;
          logger.info("------------------");
          logger.info("INPUT NAME " + number + ": " + inputStats.getName());
          logger.debug("INPUT LOCATION " + number + ": " + inputStats.getLocation());
          logger.info("INPUT RECORDS " + number + ": " + inputStats.getNumberRecords());
          logger.info("INPUT BYTES " + number + ": " + Long.toString(inputStats.getBytes()));
      }
      number = 0;
      logger.info("OUTPUT STATS: ");
      for (OutputStats outputStats : pigStats.getOutputStats()) {
          number++;
          logger.info("------------------");
          logger.info("OUTPUT NAME " + number + ": " + outputStats.getName());
          logger.debug("OUTPUT LOCATION " + number + ": " + outputStats.getLocation());
          logger.info("OUTPUT RECORDS " + number + ": " + outputStats.getNumberRecords());
          logger.info("OUTPUT BYTES " + number + ": " + Long.toString(outputStats.getBytes()));
      }
  }

  // =========================================================================================================
  // Stats for a single job
  private void printSingleJobStats(JobStats jobStats) {
      logger.debug(System.getProperty("line.separator"));
      logger.debug("Alias: " + jobStats.getAlias());

      Exception e = jobStats.getException();
      if (e != null) {
          logger.debug("Exception stack: " + e.getStackTrace());
      }

      String err = jobStats.getErrorMessage();
      if (err != null && !err.isEmpty()) {
          logger.debug("Error message: " + err);
      }

      logger.debug("HDFS_BYTES_WRITTEN: " + jobStats.getHdfsBytesWritten());
      logger.debug("MAP_INPUT_RECORDS: " + jobStats.getMapInputRecords());
      logger.debug("MAP_OUTPUT_RECORDS: " + jobStats.getMapOutputRecords());
      logger.debug("REDUCE_INPUT_RECORDS: "
              + jobStats.getReduceInputRecords());
      logger.debug("REDUCE_OUTPUT_RECORDS: "
              + jobStats.getReduceOutputRecords());
      logger.debug("AVG_MAP_TIME (ms): " + jobStats.getAvgMapTime());
      logger.debug("AVG_REDUCE_TIME (ms): " + jobStats.getAvgREduceTime());
      logger.debug("BYTES_WRITTEN: " + jobStats.getBytesWritten());
      logger.debug("ERROR_MESSAGE: " + jobStats.getErrorMessage());
      logger.debug("FEATURE: " + jobStats.getFeature());
      logger.debug("JOB_ID: " + jobStats.getJobId());
      logger.debug("MAX_MAP_TIME (ms): "
              + Long.toString(jobStats.getMaxMapTime()));
      logger.debug("MIN_MAP_TIME (ms): "
              + Long.toString(jobStats.getMinMapTime()));
      logger.debug("MAX_REDUCE_TIME (ms): "
              + Long.toString(jobStats.getMaxReduceTime()));
      logger.debug("MIN_REDUCE_TIME (ms): "
              + Long.toString(jobStats.getMinReduceTime()));
      logger.debug("NUMBER_MAPS: " + Long.toString(jobStats.getNumberMaps()));
      logger.debug("NUMBER_REDUCESS: "
              + Long.toString(jobStats.getNumberReduces()));
      logger.debug("PROACTIVE_SPILL_COUNT_OBJECTS: "
              + Long.toString(jobStats.getProactiveSpillCountObjects()));
      logger.debug("PROACTIVE_SPILL_COUNT_RECSS: "
              + Long.toString(jobStats.getProactiveSpillCountRecs()));
      logger.debug("RECORD_WRITTENS: "
              + Long.toString(jobStats.getRecordWrittern()));
      logger.debug("SMMS_SPILL_COUNTS: "
              + Long.toString(jobStats.getSMMSpillCount()));

      jobStats.getErrorMessage();

      logger.debug(System.getProperty("line.separator"));
  }

  public List<PigStats> getStatistics() {
      return pigStatistics;
  }

  }
