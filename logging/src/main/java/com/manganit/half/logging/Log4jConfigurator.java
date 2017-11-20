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



import java.util.Enumeration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

/**
 *
 * @author Damien Claveau
 *
 */
public class Log4jConfigurator {
    
  /**
   * Log4J debug information
   */
  public static void showLog4jImplementation (){
     Package p = Layout.class.getPackage();
        System.out.println(p);
        System.out.println("Implementation title:   " + p.getImplementationTitle());
        System.out.println("Implementation vendor:  " + p.getImplementationVendor());
        System.out.println("Implementation version: " + p.getImplementationVersion());
    }
    
  /**
   *
   * @param key Tag name
   * @param o Tag value
   */
  public static void putMDC(String key, Object o){
        if ((key != null) && (o != null))
            MDC.put(key, o);
    }
    
    /**
        @return Returns true if it appears that log4j have been previously configured. This code
        checks to see if there are any appenders defined for log4j which is the
        definitive way to tell if log4j is already initialized
    */
    private static boolean isConfigured() {
        Enumeration appenders = LogManager.getRootLogger().getAllAppenders();
        if (appenders.hasMoreElements()) {
            return true;
        }
        else {
            Enumeration loggers = LogManager.getCurrentLoggers();
            while (loggers.hasMoreElements()) {
                Logger c = (Logger) loggers.nextElement();
                if (c.getAllAppenders().hasMoreElements())
                    return true;
            }
        }
        return false;
    }
    
  /**
   * Configure the appenders
   */
  public synchronized static void configure() {
        if (!isConfigured()) {
            BasicConfigurator.configure();
        }
    }

  /**
   * Shutdown the appenders
   */
  public synchronized static void shutdown() {
        if (isConfigured()) {
            LogManager.shutdown();
        }
    }

}