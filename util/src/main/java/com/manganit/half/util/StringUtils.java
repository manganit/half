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

import java.text.DecimalFormat;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author Damien Claveau
 * 
 */

public class StringUtils {
  
  /**
     * Returns a human-readable representation of the file size (number of bytes).
     * 
     * @param bytes
     *            the file size in bytes
     * @return a human-readable representation of the file size (number of bytes)
     */
    public static String humanReadableByteCount(long bytes) {
        return humanReadableByteCount(bytes, false);
    }

    /**
     * Returns a human-readable representation of the file size (number of bytes).
     * 
     * @param bytes
     *            the file size in bytes
     * @param withDetails
     *            if true the full display view is used, which also includes the byte count
     * @return a human-readable representation of the file size (number of bytes)
     */
    public static String humanReadableByteCount(long bytes, boolean withDetails) {
        if (bytes < FileUtils.ONE_KB) {
            return bytes + " bytes";
        }

        StringBuilder display = new StringBuilder();

        long divider = FileUtils.ONE_KB;
        if (bytes / FileUtils.ONE_GB > 0) {
            divider = FileUtils.ONE_GB;
            display.append(" GB");
        } else if (bytes / FileUtils.ONE_MB > 0) {
            divider = FileUtils.ONE_MB;
            display.append(" MB");
        } else {
            display.append(" KB");
        }

        display.insert(0, new DecimalFormat("###,###,###,###,###,###,###.##").format((double) bytes / (double) divider));
        if (withDetails) {
            display.append(" (").append(new DecimalFormat("###,###,###,###,###,###,###").format(bytes)).append(" bytes)");
        }

        return display.toString();
    }  
}
