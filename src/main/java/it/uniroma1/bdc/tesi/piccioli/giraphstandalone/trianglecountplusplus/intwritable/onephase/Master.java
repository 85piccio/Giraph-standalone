/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.trianglecountplusplus.intwritable.onephase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Level;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

/**
 * A dumb implementation of {@link MasterCompute}. This is the default
 * implementation when no MasterCompute is defined by the user. It does nothing.
 */
public class Master extends MasterCompute {

    /**
     * Class logger
     */
    private static final Logger LOG = Logger.getLogger(Master.class);
    /**
     * Somma aggregator name
     */
    private static String SOMMA = "somma";

    @Override
    public void readFields(DataInput in) throws IOException {
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }

    @Override
    public void compute() {

        //all'inizio del secondo superstep vario la classe computation per dimezzare lo spazio dei messaggi
        if (this.getSuperstep() == 3) {
//            try {
//                registerPersistentAggregator(SOMMA + getSuperstep(), LongSumAggregator.class);
//            } catch (InstantiationException | IllegalAccessException ex) {
//                java.util.logging.Logger.getLogger(Master.class.getName()).log(Level.SEVERE, null, ex);
//            }        
        }
        LongWritable a = this.getAggregatedValue(SOMMA + "3");//superstep precedente
        System.out.println("DEBUG\t " + a);
    }

    @Override
    public void initialize() throws InstantiationException,
            IllegalAccessException {
        registerPersistentAggregator(SOMMA + "3", LongSumAggregator.class);
        registerPersistentAggregator(SOMMA + "2", LongSumAggregator.class);
    }

}
