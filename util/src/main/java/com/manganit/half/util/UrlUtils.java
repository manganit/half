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

import java.io.File;
import java.net.URL;
import java.util.Comparator;

/**
 *
 * @author Damien Claveau
 * 
 */

public class UrlUtils {
  /**
   * URL array sorting Helpers
   */
  public static final Comparator<URL> urlPathComparator;
  public static final Comparator<URL> urlFileComparator;

  static {
    urlPathComparator = (URL b1, URL b2) -> {
      String f1 = new File(b1.getFile()).getAbsolutePath();
      String f2 = new File(b2.getFile()).getAbsolutePath();
      return f1.compareTo(f2);
    };
    };

  static {
    urlFileComparator = (URL b1, URL b2) -> {
      String f1 = new File(b1.getFile()).getName();
      String f2 = new File(b2.getFile()).getName();
      return f1.compareTo(f2);
    };
    };
}
