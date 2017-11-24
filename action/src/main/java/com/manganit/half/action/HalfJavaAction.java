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

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import javax.crypto.Cipher;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;

import org.apache.log4j.Logger;

import com.manganit.half.security.CredentialsManager;
import com.manganit.half.security.LoginManager;
import com.manganit.half.logging.Log4jConfigurator;
import com.manganit.half.util.EnvUtils;

/**
 * <p>
 * HalfJavaAction is a Base class for any Oozie Java action that may be
 * configured with a {@link org.apache.hadoop.conf.Configuration}. and supports
 * handling of generic command-line options. A tool interface that supports
 * handling of generic command-line options.</p>
 *
 *
 * <p>
 * Here is how a typical <code>HalfAction</code> is implemented:</p>
 * <pre>
 * public class DirectoryLister extends HalfJavaAction  {@code
 *
 *     public static void main(String[] args) throws Exception {
 *        int exitCode = ToolRunner.run(new DirectoryLister(), args);
 *     }
 *
 *     //Override
 *     public void doRun(String[] args) throws Exception {
 *        printDirectoryContent(getFileSystem(), args[0]);
 *     }
 *
 *     private static void printDirectoryContent(FileSystem fs, String directory) throws IOException {
 *        Path path = new Path(directory);
 *        if (fs.exists(path)) {
 *            FileStatus[] files = fs.listStatus(path);
 *            for (FileStatus file : files) {
 *                System.out.println(file.getOwner() + "\t" + file.getGroup() + "\t" + file.getPath().getName());
 *            }
 *        }
 *    }
 * } </pre>
 *
 * @see org.apache.hadoop.util.Tool
 * @see org.apache.hadoop.util.ToolRunner
 *
 * To compile with this library please add the following repository in your
 * maven settings : http://repo.hortonworks.com/content/repositories/releases/
 *
 * @author Damien Claveau
 *
 */

public abstract class HalfJavaAction extends Configured implements Tool {

  /**
   *
   */
  public Logger logger;
  private JavaActionProperties outputs;
  private FileSystem fs;
  private LoginManager loginManager;

  private final static String WORKFLOW_ID_LOG_FIELD = "oozie.job.id";
  private final static String WORKFLOW_NAME_LOG_FIELD = "oozie.job.name";
  private final static String ACTION_ID_LOG_FIELD = "oozie.action.id";
  private final static String ACTION_NAME_LOG_FIELD = "oozie.action.name";
  private final static String USER_NAME_LOG_FIELD = "user.name";

  /**
   * AUTHENTICATION_METHOD_LOGIN
   */
  public final static String AUTHENTICATION_METHOD_IMPERSONATION = "IMPERSONATION";

  /**
   * AUTHENTICATION_METHOD_LOGIN
   */
  public final static String AUTHENTICATION_METHOD_LOGIN = "LOGIN";

  /**
   * Get configured FileSystem
   * @return FileSystem
   * @throws IOException IOException
   * @throws InterruptedException InterruptedException
   */
  public FileSystem getFileSystem() throws IOException, InterruptedException {
    if (fs == null) {
      fs = FileSystem.get(getConf());
      //fs = FileSystem.get(FileSystem.getDefaultUri(getConf()), getConf(), getKerberosUserName());
    }
    return fs;
  }

  /**
   * Output values to the Oozie context
   * @param key name of the property to output
   * @param value value of the property to output
   */
  public void setOutputProperty(String key, String value) {
    if (outputs == null) {
      outputs = new JavaActionProperties();
    }
    outputs.setProperty(key, value);
  }

  /**
   * getAuthenticationMethod
   * @return AuthenticationMethod
   */
  public String getAuthenticationMethod() {
    return StringUtils.defaultIfEmpty(System.getProperty("authentication.method"), AUTHENTICATION_METHOD_IMPERSONATION);
  }

  /**
   * getHadoopConfDir
   * @return HadoopConfDir
   */
  public String getHadoopConfDir() {
    return StringUtils.defaultIfEmpty(System.getProperty("hadoop.conf.dir"), null);
  }
 
  /**
   * see also OOZIE-1794 for java options retrieval through java-opt
   * @param key variable name to retrieve
   * @return a value
   */  
  public String getConfigOrPropertyValue(String key) {
    // in case you want to pass it on command line e.g. -Duser.kerberos.keytab
    String u1 = System.getProperty(key);
    // else here is the Oozie workflow config param
    String u2 = this.getConf().get(key);
    return StringUtils.defaultIfEmpty(u1, u2);
  }

  /**
   * 
   * @return KerberosPrincipal
   */
  public String getKerberosPrincipal() {
    return getConfigOrPropertyValue("user.kerberos.principal");
  }

  /**
   *
   * @return KerberosPrincipal
   */
  public String getKerberosKeytab() {
    return getConfigOrPropertyValue("user.kerberos.keytab");
  }

  // Utiliser peut-etre ici la key de config standard
  //public static final String CommonConfigurationKeys.KERBEROS_TICKET_CACHE_PATH = "hadoop.security.kerberos.ticket.cache.path";
  /**
   *
   * @return KerberosTokenCache
   */
  public String getKerberosTokenCache() {
    return System.getProperty("user.kerberos.token.cache");
    //return getConf().get("mapreduce.job.credentials.binary");
  }

    // 
  /**
   * Override this to disable the debug traces
   * @return true by default
   */
  protected boolean getKerberosDebugEnabled() {
    return true;
  }

  /**
   *
   * @return KerberosConfig
   */
  public String getKerberosConfig() {
    return System.getProperty("java.security.krb5.conf");
  }

  /**
   *
   * @return tWorkflowId
   */
  public String getWorkflowId() {
    return System.getProperty("oozie.job.id");
  }

  /**
   *
   * @return ActionId
   */
  public String getActionId() {
    return System.getProperty("oozie.action.id");
  }

  /**
   *
   * @return WorkflowName
   */
  public String getWorkflowName() {
    return this.getConf().get("oozie.job.name");
  }

  /**
   *
   * @return ActionName
   */
  public String getActionName() {
    if ((getActionId() != null) && (getWorkflowId() != null)) {
      return getActionId().substring(getWorkflowId().length() + 1);
    } else {
      return null;
    }
  }

  /**
   *
   * @return Namenode
   */
  public String getNamenode() {
    return this
            .getConf()
            .get(org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
  }

  /**
   *
   * @return HiveJdbcUrl
   */
  public String getHiveJdbcUrl() {
    return getConfigOrPropertyValue("hive2.jdbc.url");
  }

  /**
   *
   * @return HivePrincipal
   */
  public String getHivePrincipal() {
    return getConfigOrPropertyValue("hcat.metastore.principal");
  }

  /**
   *
   * @return QueueName
   */
  public String getQueueName() {
    return getConfigOrPropertyValue("mapred.job.queue.name");
  }

  /**
   * Initialize the action context before the impersonation
   * @throws IOException IOException
   */
  private void doBeforeRun() throws IOException {

    // Prepare a full configuration object
    this.setConf(JavaActionConfiguration.getConfiguration(this.getConf(), getHadoopConfDir()));
    
    // Set Delegation Tokens location
    CredentialsManager.setCredentialsLocation(getConf());

    // Set Kerberos UGI
    loginManager = new LoginManager();

    // Set Kerberos Debug Mode
    LoginManager.setKerberosDebugMode(getKerberosDebugEnabled());
  }

  /**
   * Initialize Log4J loggers with additional tags
   * 
   */
  private void initLogger() {
    System.out.println("Logger initialization...");
    logger = Logger.getLogger(HalfJavaAction.class);
    Log4jConfigurator.showLog4jImplementation();
    Log4jConfigurator.configure();
    Log4jConfigurator.putMDC(WORKFLOW_ID_LOG_FIELD, getWorkflowId());
    Log4jConfigurator.putMDC(WORKFLOW_NAME_LOG_FIELD, getWorkflowName());
    Log4jConfigurator.putMDC(ACTION_ID_LOG_FIELD, getActionId());
    Log4jConfigurator.putMDC(ACTION_NAME_LOG_FIELD, getActionName());
    Log4jConfigurator.putMDC(USER_NAME_LOG_FIELD, LoginManager.getCurrentUserName());
    System.out.println("Logger initialized");
  }

  /**
   * This method will be executed under impersonation
   * @param args command-line arguments
   * @throws Exception exception
   */
  private void doPrivilegedRun(String[] args) throws Exception {
    try {
      // Initialize HBase/SolR or any other secured Logger
      initLogger();
      // Run effectively
      doRun(args);
    } catch (Exception e) {
      if (logger != null) {
        logger.error(e);
      }
      throw e;
    }
  }

  /**
   * Your code goes here !
   * @param args command-line arguments
   * @throws Exception exception
   */
  protected abstract void doRun(String[] args) throws Exception;

  /**
   * Finalize the Oozie action
   */
  private void doAfterRun() {
    // Output values to the Oozie context for the next action
    if (outputs != null) {
      outputs.output();
    }
  }

  /**
   * @param args command-line arguments
   * @throws Exception exception
   */
  public final int run(String[] args) throws Exception {
    UserGroupInformation ugi;
    final String[] runArgs = args;

    try {
      doBeforeRun();
      // Run with impersonation
      if (getAuthenticationMethod().equals(AUTHENTICATION_METHOD_IMPERSONATION)) {
        ugi = LoginManager.getSecuredLogin(
                getKerberosPrincipal(),
                getKerberosTokenCache(),
                getKerberosKeytab(),
                this.getConf());
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
          public Void run() throws Exception {
            doPrivilegedRun(runArgs);
            return null;
          }
        });
      } // Run after a modification of the current user
      else {
        loginManager.login(
                getKerberosPrincipal(),
                getKerberosTokenCache(),
                getKerberosKeytab(),
                this.getConf());
        try {
          doPrivilegedRun(runArgs);
        } finally {
          // Restore initial logged user
          loginManager.logout();
          System.out.println("Logged out");
        }
      }

    } finally {
      doAfterRun();
    }

    return 0;
  }

  /**
   * Print useful information : Java properties, System properties, Container root directory and Workflow parameters
   */
  public void printContext() {
    logger.info("######################################################################");
    logger.info("WorkflowId " + getWorkflowId());
    logger.info("ActionId " + getActionId());
    logger.info("WorkflowName " + getWorkflowName());
    logger.info("ActionName " + getActionName());
    logger.info("KerberosPrincipal " + getKerberosPrincipal());
    logger.info("KerberosKeytab " + getKerberosKeytab());
    EnvUtils.printJCESettings(null);
    EnvUtils.printEnv("###################### Env variables #######################");
    EnvUtils.printJavaProperties("###################### Java properties ###############################");
    EnvUtils.printConfiguration(JavaActionConfiguration.getOozieConfiguration(),"###################### Oozie inherited Configuration #################################");
    EnvUtils.printConfiguration(getConf(),"###################### Final Hadoop Configuration #################################");
    EnvUtils.printClassPath(true, "###################### Classpath #######################");
    EnvUtils.printCurrentDirContents("###################### Current directory files #######################");
    logger.info("######################################################################");
  }
}
