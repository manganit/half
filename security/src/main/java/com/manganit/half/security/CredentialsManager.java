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

package com.manganit.half.security;

import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.mapred.JobConf;

/**
 *
 * @author Damien Claveau
 *
 */
public class CredentialsManager {

    // Propagate delegation related props from Ooozie launcher job to submitted M/R jobs
    public static void setCredentialsLocation(Properties properties, Configuration configuration) {
        String tokenFile = System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
        if (tokenFile != null) {
            if (properties != null) {
                properties.setProperty("mapreduce.job.credentials.binary", tokenFile);
                properties.setProperty("tez.credentials.path", tokenFile);
            }
            if (configuration != null) {
                configuration.set("mapreduce.job.credentials.binary", tokenFile);
                configuration.set("tez.credentials.path", tokenFile);
            }
            System.out.println("------------------------");
            System.out.println("Setting env property for mapreduce.job.credentials.binary to :" + tokenFile);
            System.out.println("------------------------");
            System.setProperty("mapreduce.job.credentials.binary", tokenFile);
            System.setProperty("tez.credentials.path", tokenFile);
        } else {
            System.out.println("Non-kerberos execution");
        }
    }

    // Propagate delegation related props from Ooozie launcher job to submitted M/R jobs
    public static void setCredentialsLocation(Properties properties) {
        setCredentialsLocation(properties, null);
    }

    public static void setCredentialsLocation(Configuration configuration) {
        setCredentialsLocation(null, configuration);
    }

    public static void setCredentialsLocation() {
        setCredentialsLocation(null, null);
    }

    // Security framework already loaded the tokens into current ugi
    public Credentials getCurrentUserCredentials() throws IOException {
        return UserGroupInformation.getCurrentUser().getCredentials();
    }

    //
    public void printCurrentUserCredentials() throws IOException {
        Credentials credentials = getCurrentUserCredentials();
        System.out.println("Executing with tokens :");
        for (Token token : credentials.getAllTokens()) {
            System.out.println(token);
        }
    }

    //
    public void transferCredentials(UserGroupInformation source, UserGroupInformation dest) {
        dest.addCredentials(source.getCredentials());
    }

    // Add any user credentials to the job conf which are necessary 
    // for running on a secure Hadoop cluster
    public void addCredentials(JobConf conf) throws IOException {
        Credentials jobCreds = conf.getCredentials();
        jobCreds.mergeAll(getCurrentUserCredentials());
    }

}
