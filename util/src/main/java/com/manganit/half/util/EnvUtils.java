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

package com.manganit.half.util;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.crypto.Cipher;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * Print useful information : 
 *  Environment settings
 *  Java properties
 *  Container root directory contents
 *  Container execution classpath
 * 
 * @author Damien Claveau
 */
public class EnvUtils {
  
  private final static Logger logger = Logger.getLogger(EnvUtils.class);
   
  /**
   * Print or Log one line of text
   */
  private static void outputLine(String line) {
    if (line != null)
      //System.out.println(line);
      logger.info(line);    
  }

  /**
   * Print JCE settings
   * @param header
   */  
  public static void printJCESettings(String header) {
    outputLine(header);
    try {
      outputLine("JCE AES Key Length " + Integer.toString(Cipher.getMaxAllowedKeyLength("AES")));
    } catch (NoSuchAlgorithmException ex) {
      logger.warn("Failed to retrieve JCE AES Key Length", ex);
    }
  }
  
  /**
   * Print Environment variables
   * @param header
   */  
  public static void printEnv(String header) {
    outputLine(header);
    SortedMap<String, String> sortedEnv = new TreeMap<>(System.getenv());
    sortedEnv.keySet().stream().forEach((envName) -> {
      outputLine(String.format("%s=%s%n", envName, sortedEnv.get(envName)));
    });
  }

  /**
   * Print Java runtime properties
   * @param header
   */
  public static void printJavaProperties(String header) {
    outputLine(header);
    //System.getProperties().list(System.out);
    Map<String, String> sortedProps = new TreeMap(System.getProperties());
    sortedProps.keySet().stream().forEach((key) -> {
      outputLine(key + "=" + sortedProps.get(key));
    });
  }

  /**
   * Print Hadoop Environment Configuration
   * @param conf HadoopConfiguration
   * @param header
   */
  public static void printConfiguration(Configuration conf, String header) {
    outputLine(header);
    if (conf != null) {
      //Configuration.dumpConfiguration(conf, new PrintWriter(System.out, true));
      //ConfigUtils.getProperties(conf).store(System.out, null);
      Map<String, String> sortedConf = new TreeMap(ConfigUtils.getProperties(conf));
      sortedConf.keySet().stream().forEach((key) -> {
        outputLine(key + "=" + sortedConf.get(key));
      });
    }
    else logger.warn("Configuration object not found");
  }
    
  /**
   * Print Container root directory
   * @param header
   */
  public static void printCurrentDirContents(String header) {
    outputLine(header);
    File dir;
    try {
      dir = new File(".").getCanonicalFile();
      File[] filesList = dir.listFiles();
      for (File file : filesList) {
        if (file.isFile()) {
          String filename = file.getName();
          outputLine("     " + filename);
        }
      }
    }
    catch (IOException ex) {
      logger.warn("Failed to print current local directory", ex);
    }
  }

  /**
   * Print Classpath with short filenames
   */
  public static void printClassPath(String header) {
    printClassPath(false, header);
  }
  
  /**
   * Print Classpath : Oozie sharedlib jars should appear here
   * @param fullPath set to true in order to print the absolute full path
   */
  public static void printClassPath(boolean fullPath, String header) {
    outputLine(header);
    ClassLoader sysClassLoader = ClassLoader.getSystemClassLoader();
    URL[] urls = ((URLClassLoader) sysClassLoader).getURLs();
    Arrays.sort(urls, fullPath ? UrlUtils.urlPathComparator : UrlUtils.urlFileComparator);
    for (URL url : urls) {
      outputLine(fullPath ? new File(url.getFile()).getAbsolutePath() : new File(url.getFile()).getName());
    }
  }
  
}
