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

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author Damien Claveau
 * 
 */

public class NamedThreadFactory implements ThreadFactory {
    private final String prefix;
    private final ThreadFactory threadFactory;
    private final AtomicInteger atomicInteger = new AtomicInteger();

    public NamedThreadFactory(final String prefix){
        this(prefix, Executors.defaultThreadFactory());
    }

    public NamedThreadFactory(final String prefix, final ThreadFactory threadFactory){
        this.prefix = prefix;
        this.threadFactory = threadFactory;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = this.threadFactory.newThread(r);
        t.setName(this.prefix + this.atomicInteger.incrementAndGet());
        return t;
    }
}