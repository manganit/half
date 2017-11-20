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
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Properties;

/**
 *
 * Helper class to forward output properties to the Oozie host workflow
 * 
 * @author Damien Claveau
 * 
 */
public class JavaActionProperties extends Properties {
    
    public void output() {
      try{
         File file = new File(System.getProperty("oozie.action.output.properties"));
         System.out.println("Saving properties to " + file.getAbsolutePath());
         OutputStream os = new FileOutputStream(file);
         this.store(os, "");
         os.close();
         System.out.println("Properties saved");
      }
      catch (Exception e) {
         e.printStackTrace();
      }        
    }
    
}
