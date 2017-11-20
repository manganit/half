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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.io.Text;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

/**
 * An implementation of CatalogService that uses Hive Meta Store (HCatalog) as
 * the backing Catalog registry.
 *
 * @author Damien Claveau
 * 
 */

public class HCatalogClient {

    private static final Logger LOG = LoggerFactory.getLogger(HCatalogClient.class);
    private HiveMetaStoreClient hms;
    private Configuration conf;
    private HiveConf hCatConf;

    public HCatalogClient(Configuration conf) {
      this.conf = conf;
    }


    private static HiveConf createHiveConf(Configuration conf) throws IOException, Exception {
        HiveConf hcatConf = new HiveConf(conf, HiveConf.class);
        hcatConf.set("hive.metastore.local", "false");
        if (hcatConf.get(HiveConf.ConfVars.METASTOREURIS.varname) == null)
            throw new Exception("Exception creating HiveMetaStoreClient: No metastore uri in the configuration.");
        //hcatConf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreUrl);
        hcatConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
        hcatConf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
                HCatSemanticAnalyzer.class.getName());
        hcatConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
        hcatConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hcatConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        return hcatConf;
    }

    /**
     * This is used from with in an oozie job.
     *     
     * @param conf conf object
     * @return hive metastore client handle
     * @throws Exception Exception
     */
    private HiveMetaStoreClient createClient(Configuration conf) throws Exception {
        try {
            LOG.info("Creating HCatalog client object for metastore using conf {}", conf.toString());
            final Credentials credentials = getCredentials(conf);
            Configuration jobConf = credentials != null ? copyCredentialsToConf(conf, credentials) : conf;
            hCatConf = createHiveConf(jobConf);
            if (UserGroupInformation.isSecurityEnabled()) {
                hCatConf.set(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname,
                    conf.get(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname));
                hCatConf.set(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname, "true");
                UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
                if (credentials != null)
                    ugi.addCredentials(credentials); // credentials cannot be null
            }

            return new HiveMetaStoreClient(hCatConf);
        } catch (Exception e) {
            throw new Exception("Exception creating HiveMetaStoreClient: " + e.getMessage(), e);
        }
    }

    private static JobConf copyCredentialsToConf(Configuration conf, Credentials credentials) {
        JobConf jobConf = new JobConf(conf);
        jobConf.setCredentials(credentials);
        return jobConf;
    }

    private static Credentials getCredentials(Configuration conf) throws IOException {
        final String tokenFile = System.getenv("HADOOP_TOKEN_FILE_LOCATION");
        if (tokenFile == null) {
            return null;
        }
        try {
            LOG.info("Adding credentials/delegation tokens from token file={} to conf", tokenFile);
            Credentials credentials = Credentials.readTokenStorageFile(new File(tokenFile), conf);
            LOG.info("credentials numberOfTokens={}, numberOfSecretKeys={}",
                    credentials.numberOfTokens(), credentials.numberOfSecretKeys());
            return credentials;
        } catch (IOException e) {
            LOG.warn("error while fetching credentials from {}", tokenFile);
        }
        return null;
    }

    public HiveConf getHCatConf() {
       if (this.hCatConf == null ) {
         try {
           this.hCatConf = createHiveConf(this.conf);
         } catch (Exception ex) {
           LOG.error("Error when initializing Hive Configuration", ex);
         }
      }
      return this.hCatConf;
    }
    
    public HiveMetaStoreClient getClient() {
      if (this.hms == null ) {
        try {
          this.hms = createClient(this.conf);
        } catch (Exception ex) {
          LOG.error("Error when initializing Metastore Client", ex);
        }
      }
      return this.hms;
    }
    
  /**
   *
   * @return is Alive
   * @throws Exception Exception
   */
  public boolean isAlive() throws Exception {
        LOG.info("Checking if the service is alive");
        try {
            List<String> databases = getClient().getAllDatabases();
            return !databases.isEmpty();
        } catch (Exception e) {
            throw new Exception("Exception checking if the service is alive:" + e.getMessage(), e);
        }
    }

  /**
   *
   * @param database database name
   * @param tableName table name
   * @return exists or not
   * @throws Exception Exception
   */
  public boolean tableExists(final String database, final String tableName) throws Exception {
        LOG.info("Checking if the table exists: {}", tableName);
        try {
            Table table = getClient().getTable(database, tableName);
            return table != null;
        } catch (Exception e) {
            throw new Exception("Exception checking if the table exists:" + e.getMessage(), e);
        }
    }

  /**
   *
   * @param database database name
   * @param tableName table name
   * @return external or not
   * @throws Exception Exception
   */
  public boolean isTableExternal(String database, String tableName) throws Exception {
        LOG.info("Checking if the table is external: {}", tableName);
        try {
            Table table = getClient().getTable(database, tableName);
            return table.getTableType().equals(TableType.EXTERNAL_TABLE.name());
        } catch (Exception e) {
            throw new Exception("Exception checking if the table is external:" + e.getMessage(), e);
        }
    }

  /**
   *
   * @param database database name
   * @param tableName table name
   * @return table description
   * @throws Exception Exception
   */
  public String getTableDescription(String database, String tableName) throws Exception {
            Table table = getClient().getTable(database, tableName);
            return table.getViewOriginalText();
    }    
    
  /**
   *
   * @param database database name
   * @param tableName table name
   * @param filter filter
   * @return list of partitions
   * @throws Exception Exception
   */
  public List<HCatalogPartition> listPartitionsByFilter(String database, String tableName,
            String filter) throws Exception {
        LOG.info("List partitions for: {}, partition filter: {}", tableName, filter);
        try {
            List<HCatalogPartition> catalogPartitionList = new ArrayList<HCatalogPartition>();
            List<Partition> hCatPartitions = getClient().listPartitionsByFilter(database, tableName, filter, (short) -1);
            for (Partition hCatPartition : hCatPartitions) {
                LOG.info("Partition: " + hCatPartition.getValues());
                HCatalogPartition partition = createCatalogPartition(hCatPartition);
                catalogPartitionList.add(partition);
            }
            return catalogPartitionList;
        } catch (Exception e) {
            throw new Exception("Exception listing partitions:" + e.getMessage(), e);
        }
    }

    private HCatalogPartition createCatalogPartition(Partition hCatPartition) {
        final HCatalogPartition catalogPartition = new HCatalogPartition();
        catalogPartition.setDatabaseName(hCatPartition.getDbName());
        catalogPartition.setTableName(hCatPartition.getTableName());
        catalogPartition.setValues(hCatPartition.getValues());
        catalogPartition.setInputFormat(hCatPartition.getSd().getInputFormat());
        catalogPartition.setOutputFormat(hCatPartition.getSd().getOutputFormat());
        catalogPartition.setLocation(hCatPartition.getSd().getLocation());
        catalogPartition.setSerdeInfo(hCatPartition.getSd().getSerdeInfo().getSerializationLib());
        catalogPartition.setCreateTime(hCatPartition.getCreateTime());
        catalogPartition.setLastAccessTime(hCatPartition.getLastAccessTime());
        return catalogPartition;
    }

  /**
   *
   * @param database database name
   * @param tableName table name
   * @param partitionValues partitions
   * @param deleteData delete data
   * @return dropped
   * @throws Exception Exception
   */
  public boolean dropPartition(String database, String tableName,
            List<String> partitionValues, boolean deleteData) throws Exception {
        LOG.info("Dropping partition for: {}, partition: {}", tableName, partitionValues);
        try {
            return getClient().dropPartition(database, tableName, partitionValues, deleteData);
        } catch (Exception e) {
            throw new Exception("Exception dropping partitions:" + e.getMessage(), e);
        }
    }

  /**
   *
   * @param database database name
   * @param tableName table name
   * @param partitionValues partitions
   * @param deleteData delete data
   * @throws Exception Exception
   */
  public void dropPartitions(String database, String tableName,
            List<String> partitionValues, boolean deleteData) throws Exception {
        LOG.info("Dropping partitions for: {}, partitions: {}", tableName, partitionValues);
        try {
            List<Partition> partitions = getClient().listPartitions(database, tableName, partitionValues, (short) -1);
            for (Partition part : partitions) {
                LOG.info("Dropping partition for: {}, partition: {}", tableName, part.getValues());
                getClient().dropPartition(database, tableName, part.getValues(), deleteData);
            }
        } catch (Exception e) {
            throw new Exception("Exception dropping partitions:" + e.getMessage(), e);
        }
    }

  /**
   *
   * @param database database name
   * @param tableName table name
   * @param partitionValues partitions
   * @return partition descriptor
   * @throws Exception Exception
   */
  public HCatalogPartition getPartition(String database, String tableName,
            List<String> partitionValues) throws Exception {
        LOG.info("Fetch partition for: {}, partition spec: {}", tableName, partitionValues);
        try {
            Partition hCatPartition = getClient().getPartition(database, tableName, partitionValues);
            return createCatalogPartition(hCatPartition);
        } catch (Exception e) {
            throw new Exception("Exception fetching partition:" + e.getMessage(), e);
        }
    }
  
  public HCatalogPartition addPartition(String database, String tableName,
            List<String> partitionValues) throws Exception {
        LOG.info("Fetch partition for: {}, partition spec: {}", tableName, partitionValues);
        try {
            Partition hCatPartition = getClient().getPartition(database, tableName, partitionValues);
            return createCatalogPartition(hCatPartition);
        } catch (Exception e) {
            throw new Exception("Exception fetching partition:" + e.getMessage(), e);
        }
    }  
  
  public void alterPartition(String database, String tableName,
            List<String> partitionValues) throws Exception {
        LOG.info("Fetch partition for: {}, partition spec: {}", tableName, partitionValues);
        try {
            Partition hCatPartition = getClient().getPartition(database, tableName, partitionValues);
            getClient().alter_partition(tableName, tableName, hCatPartition);
        } catch (Exception e) {
            throw new Exception("Exception fetching partition:" + e.getMessage(), e);
        }
    }    
  
}
