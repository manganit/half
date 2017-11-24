/*
 * Copyright 2017 ManganIT.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.manganit.half.util;

import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Damien Claveau
 */
public class ConfigUtils {
  
    /**
     * Convert Configuration to Properties
     * @param conf Configuration
     * @return Configuration
     */
    public static Properties getProperties(Configuration conf) {
      Properties jobprops = new Properties();
      for (Map.Entry<String, String> entry : conf) {
        jobprops.setProperty(entry.getKey(), entry.getValue());
      }
      return jobprops;
    }  
}
