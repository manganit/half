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

package com.manganit.half.action;

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 *
 * @author Damien Claveau
 *
 */

public class JavaActionConfiguration {
  
    public static Path getOozieConfigurationPath() {
        String actionXml = System.getProperty("oozie.action.conf.xml");
        if (actionXml == null) {
            //throw new RuntimeException("Missing Java System Property [oozie.action.conf.xml]");
            System.out.println("Missing Java System Property [oozie.action.conf.xml]");
            return null;
        } else if (!new File(actionXml).exists()) {
            //throw new RuntimeException("Action Configuration XML file [" + actionXml + "] does not exist");
            System.out.println("Action Configuration XML file [" + actionXml + "] does not exist");
            return null;
        } else {
            System.out.println("Using action configuration file " + actionXml);
            return new Path("file:///", actionXml);
        }
    }

    // Loads action conf prepared by Oozie
    public static Configuration getOozieConfiguration() {
        Configuration actionConf = null;
        Path confPath = getOozieConfigurationPath();
        if (confPath != null) {
            actionConf = new Configuration(false);
            actionConf.addResource(confPath);
            System.out.println("Action configuration file added : " + confPath.toString());
        }
        return actionConf;
    }

    // Loads config files from a local directory
    public static Configuration getConfDirConfiguration(String hadoopConfDir) {
        final String[] confFiles = {"core-site.xml","hdfs-site.xml","yarn-site.xml","mapred-site.xml","hbase-site.xml","hive-site.xml"};
        Configuration dirConf = null;
        if (hadoopConfDir != null) {
            dirConf = new Configuration(false);
            for (String s: confFiles) {           
                File f = new File (hadoopConfDir, s);
                if (f.exists() && f.canRead()) {
                    Path p = new Path(f.getAbsolutePath());
                    dirConf.addResource(p);
                    System.out.println("Configuration file added : " + p.toString());
                }
            }
        }        
        return dirConf;
    }
    
    // Adds ressources from the classpath to the configuration initiated with "hadoop jar"
    public static Configuration getRessourceConfiguration(Configuration configuredConf) {
        // Duplicate curent config prepared by Hadoop Jar through the ToolRunner mechanism (basically core-site.xml)
        Configuration ressourceConf = new Configuration(configuredConf);
        // Additional Hdfs configuration if hdfs-site can be loaded by the classloader
        ressourceConf.addResource("hdfs-site.xml");
        // Additional HBase configuration if hbase-site can be loaded by the classloader
        HBaseConfiguration.merge(ressourceConf, HBaseConfiguration.create(ressourceConf));
        // Additionnal Hive configuration if hive-site can be loaded by the classloader
        HBaseConfiguration.merge(ressourceConf, new HiveConf());
        return ressourceConf;
    }
    
    public static Configuration getConfiguration(Configuration configured, String hadoopConfDir) {
        // Override configuration found in ressource/classpath/$HADOOP_CONF_DIR with a customized conf dir
        Configuration dirConf = getConfDirConfiguration(hadoopConfDir);//getConfDirConfiguration(hadoopConfDir);
        // Configuration inherited from Oozie Job configuration
        Configuration actionConf = getOozieConfiguration();
        
        if (dirConf != null) {
          return dirConf;
        }
        else if (actionConf != null) {
          return actionConf;
        }
        else {        
          // Merge configured object with additional resources or classpath
          return getRessourceConfiguration(configured);
        }
    }

}
