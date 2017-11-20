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
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.security.auth.Subject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.commons.lang.StringUtils;

/**
 *
 * Helper class to manage Kerberos Authentication
 * 
 * @author Damien Claveau
 * 
 */
public class LoginManager {

    public static String getCurrentUserName() {
        try {
            return UserGroupInformation.getCurrentUser().getUserName();
        } catch (IOException ex) {
            Logger.getLogger(LoginManager.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }

    public boolean kerberosParamModified = false;
    private UserGroupInformation originalUser;
    
    /**Environment variable pointing to the token cache file*/
    public static final String WINDOWS_TOKEN_FILE_LOCATION = "WINDOWS_TOKEN_FILE_LOCATION";
    /**Environment variable pointing to the token cache file*/
    public static final String WINDOWS_KERBEROS_USER = "WINDOWS_KERBEROS_USER";
  
    public LoginManager() throws IOException {
        // Save the original User
        try {
            originalUser = UserGroupInformation.getCurrentUser();
        }
        catch (Exception e) {
            System.out.println(e.toString());
        }
            
    }

    // Login from Delegation Tokens    
    private static UserGroupInformation getLoginFromCache(String user, Configuration conf) throws IOException {
        String ticketCache = System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
        if (StringUtils.isEmpty(ticketCache)) {
            throw new IOException("Cannot login from token cache. HADOOP_TOKEN_FILE_LOCATION variable is empty.");
        }
        return getLoginFromCache(user, ticketCache, conf);
    }
    
    // Enable/disable Debug Mode
    public static void setKerberosDebugMode(boolean value) {
        System.setProperty("sun.security.krb5.debug", String.valueOf(value));
    }

    // Force Kerberos authentication method
    public static void checkHadoopSecurityAuthentication(Configuration conf) throws IOException {
        String currentAuthenticationMethod = conf.get("hadoop.security.authentication", "");
        String kerberosAuthenticationMethod = "KERBEROS";
        //UserGroupInformation.setConfiguration(conf);
        //see UserGroupInformation.isSecurityEnabled()
        if (StringUtils.isEmpty(currentAuthenticationMethod) || !currentAuthenticationMethod.equalsIgnoreCase(kerberosAuthenticationMethod) ) {
            //throw new IOException("hadoop.security.authentication configuration Not Found !");
            System.out.println("hadoop.security.authentication was : " + currentAuthenticationMethod);
            conf.set("hadoop.security.authentication", kerberosAuthenticationMethod);
            System.out.println("hadoop.security.authentication overriden to : " + kerberosAuthenticationMethod);
        } else {
            System.out.println("hadoop.security.authentication : " + conf.get("hadoop.security.authentication"));
        }
   }
    
    
    // Login from local ticket cache on Windows client platform
    private static UserGroupInformation getLoginFromCache(String user, String ticketCache, Configuration conf) throws IOException {
        UserGroupInformation ugi;

        if (StringUtils.isEmpty(System.getProperty("java.security.krb5.conf"))) 
        //Caused by: org.apache.hadoop.security.authentication.util.KerberosName$NoMatchingRule: No rules applied to u_tsbm4911_dev@INTBDFDATAFRANCE
        //at org.apache.hadoop.security.authentication.util.KerberosName.getShortName(KerberosName.java:378)
        {
            throw new IOException("Cannot login from token cache. System Property java.security.krb5.conf is empty.");
        } else {
            System.out.println("java.security.krb5.conf : " + System.getProperty("java.security.krb5.conf"));
        }

        if (StringUtils.isEmpty(ticketCache)) {
            throw new IOException("Cannot login from token cache. No ticketCache file provided.");
        } else {
            System.out.println("Ticket cache : " + ticketCache);
        }

        if (StringUtils.isEmpty(conf.get("dfs.namenode.kerberos.principal"))) {
            throw new IOException("dfs.namenode.kerberos.principal configuration Not Found !");
        } else {
            System.out.println("dfs.namenode.kerberos.principal : " + conf.get("dfs.namenode.kerberos.principal"));
        }

        if (StringUtils.isEmpty(conf.get("fs.defaultFS"))) {
            throw new IOException("fs.defaultFS configuration Not Found !");
        } else {
            System.out.println("fs.defaultFS : " + conf.get("fs.defaultFS"));
        }
        checkHadoopSecurityAuthentication(conf);
        //conf.set("java.security.krb5.conf", krb5Conf);
        conf.set("com.sun.security.auth.module.Krb5LoginModule", "required");
        conf.set("ticketCache", ticketCache);
        UserGroupInformation.setConfiguration(conf);
        ugi = UserGroupInformation.getUGIFromTicketCache(ticketCache, user);
        System.out.println("Token autentication successful !");
        return ugi;
    }

    // Login from Keytab
    private synchronized static UserGroupInformation getLoginFromKeytab(String user, String path, Configuration conf) throws IOException {
        checkHadoopSecurityAuthentication(conf);
        UserGroupInformation.setConfiguration(conf);
        return UserGroupInformation.loginUserFromKeytabAndReturnUGI(user, path);
    }

    public synchronized static UserGroupInformation getSecuredLogin(String user, String ticketCache, String keytabFile, Configuration conf)throws IOException {
            UserGroupInformation securedUser = null;
            System.out.println("getSecuredLogin : ticketCache = " + ticketCache);
            System.out.println("getSecuredLogin : keytabFile = " + keytabFile);
            
            if (StringUtils.isNotEmpty(ticketCache) && (conf != null)) {
                securedUser = getLoginFromCache(user, ticketCache, conf);
                System.out.println("Login with Ticket cache " + ticketCache);
            }
            else if (StringUtils.isNotEmpty(keytabFile) && StringUtils.isNotEmpty(user)) {
               securedUser =  getLoginFromKeytab(user, keytabFile, conf);
               System.out.println("Login with Keytab " + keytabFile);
            }
            else {
                System.out.println("Ticket cache or Keytab file must be provided. Fallback to current user.");
                securedUser = UserGroupInformation.getCurrentUser();
            }
            return securedUser;
    }

    
    
    
    public void login(String user, String ticketCache, String keytabFile, Configuration conf) throws IOException {
            UserGroupInformation securedUser = getSecuredLogin(user, ticketCache, keytabFile, conf);
            UserGroupInformation.setLoginUser(securedUser);
            kerberosParamModified = true;
    }
    
    public void logout() {
        if (originalUser != null && kerberosParamModified) {
            UserGroupInformation.setLoginUser(originalUser);
            kerberosParamModified = false;
        }
    }

}
