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
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.direct.intwritable.testadd;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

/**
 * The values of the aggregators are broadcast to the workers before
 * vertex.compute() is called and collected by the master before
 * master.compute() is called. This means aggregator values used by the workers
 * are consistent with aggregator values from the master from the same superstep
 * and aggregator used by the master are consistent with aggregator values from
 * the workers from the previous superstep.
 */
/**
 * Densest Subgraph in Streaming and MapReduce Bahman Bahmani, Ravi Kumar,
 * Sergei, Vassilvitskii
 */
public class Master extends MasterCompute {

    /**
     * Class logger
     */
    private static final Logger LOG = Logger.getLogger(Master.class);
    /**
     * Somma aggregator name
     */
    private static final String REMOVEDVERTICIESINS = "removedVerticiesFromS";
    private static final String REMOVEDVERTICIESINT = "removedVerticiesFromT";
    private static final String REMOVEDEDGES = "removedEdges";
    /**
     * variabili globali
     */
    private static final String OPTIMALSUPERSTEP = "optimalSuperstep";

    private static long bestDensitySuperstep = -1;
    private static Double bestlDensity = Double.NEGATIVE_INFINITY;
    private static final Double c = 1.0;

    @Override
    public void compute() {

        long superstep = getSuperstep();

        if (superstep > 2) {

            Log.info("inizio superste: " + superstep);

            if (isEven(superstep)) {//2,4....
                LongWritable removedEdges = this.getAggregatedValue(REMOVEDEDGES);//superstep precedente
                LongWritable removedVertexInS = this.getAggregatedValue(REMOVEDVERTICIESINS);//superstep precedente	    
                LongWritable removedVertexInT = this.getAggregatedValue(REMOVEDVERTICIESINT);//superstep precedente

                Long totVertices = this.getTotalNumVertices();
                Long verticesInS = totVertices - removedVertexInS.get();
                Long verticesInT = totVertices - removedVertexInT.get();

                //EpSpSPp --> |E(S, T )| = |E ∩ (S×T)|
                Long totEdge = this.getTotalNumEdges() / 2;
                Long EpSTp = totEdge - removedEdges.get();
                LOG.info("EpSTp\t" + totEdge + "\t" + removedEdges.get());

                //con rimozione effettiva dei vertici ci vogliono 2 step per startup                
//                Boolean SamePreviuosStepPartition = IsNextPartitionS.equals(isPreviousPartitionS);
//                Boolean noChangePreviousStep = prevStepRemovedEdges.equals(removedEdges.get()) && SamePreviuosStepPartition;
//                if ((noChangePreviousStep && superstep > 2) || edges == 0) {
                if ((verticesInS == 0 && verticesInT == 0)) {
                    LOG.info("edge rimasti\t" + EpSTp);
                    LOG.info("vertici in partizione S\t" + verticesInS);
                    LOG.info("vertici in partizione T\t" + verticesInT);
                    LOG.info("BEST DENSITY\t" + bestlDensity + " at " + bestDensitySuperstep);
                    this.haltComputation();
                    return;
                }

                //Aggiorno variabile vertici rimossi per check nel step successivo
//                prevStepRemovedEdges = removedEdges.get();
//                isPreviousPartitionS = IsNextPartitionS;
                //DENSITY DIRECT  ρ(S, T ) =  |E(S, T )| /  sqrt (|S||T |)
                Double currDensity = EpSTp.doubleValue() / Math.sqrt(verticesInS.doubleValue() * verticesInT.doubleValue());
                LOG.info("currDesity" + "\t" + verticesInS.doubleValue() + "\t" + verticesInT.doubleValue());

                if (currDensity > bestlDensity) {
                    bestlDensity = currDensity;
                    bestDensitySuperstep = superstep - 2; //Densità calcolata sul supertep pari precedente
                    this.getConf().setLong(OPTIMALSUPERSTEP, superstep);
                }

                //calcolo prossima partizione su cui operare in base alla cardinalità dei due insiemi
                Boolean IsNextPartitionS = (verticesInS.doubleValue() / verticesInT.doubleValue()) >= c;

                if (IsNextPartitionS) {
                    //S                
                    LOG.info("partizione S");
                    this.setComputation(VertexComputePartitionS.class);

                } else {
                    //T
                    LOG.info("partizione T");
                    this.setComputation(VertexComputePartitionT.class);

                }

                LOG.info("superstep\t" + superstep + "\t\tdensity\t" + currDensity);

            }
//	     else {//3,5...
//
//        }
        }
    }

    @Override
    public void initialize() throws InstantiationException,
            IllegalAccessException {
        registerPersistentAggregator(REMOVEDEDGES, LongSumAggregator.class);
        registerPersistentAggregator(REMOVEDVERTICIESINS, LongSumAggregator.class);
        registerPersistentAggregator(REMOVEDVERTICIESINT, LongSumAggregator.class);
    }

    private boolean isEven(long a) {
        return (a % 2 == 0);
    }

    @Override
    public void write(DataOutput d) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
