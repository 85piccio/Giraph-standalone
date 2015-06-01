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
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.undirect.intwritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

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
    private static String REMOVEDVERTICIES = "removedVerticies";
    private static String REMOVEDEDGES = "removedEdges";
    /**
     * variabili globali
     */
    private static final String OPTIMALSUPERSTEP = "optimalSuperstep";
    private static final String SOGLIA = "soglia";

//    private static final String PREVSTEPREMOVEDEDVERTEX = "prevStepRemovedVertex";
    private static Long prevStepRemovedVertex = Long.MIN_VALUE;
    private static long bestDensitySuperstep = 0;
    private static Double bestlDensity = Double.NEGATIVE_INFINITY;
    private static final Double epsilon = 0.001;

    long counterRemovedEdges = 0, counterRemovedVertecies = 0;

    @Override
    public void readFields(DataInput in) throws IOException {
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }

    @Override
    public void compute() {

        long superstep = getSuperstep();
        LongWritable removedEdges = this.getAggregatedValue(REMOVEDEDGES);//superstep precedente
        LongWritable removedVertex = this.getAggregatedValue(REMOVEDVERTICIES);//superstep precedente        

        //DeBug valori aggregators
        System.out.println("Vertex: \t"+removedVertex + "\tEdges: \t" + removedEdges);

        counterRemovedEdges += removedEdges.get();
        counterRemovedVertecies += removedVertex.get();

        System.out.println("DEBUG " + counterRemovedEdges + "\t" + counterRemovedVertecies);

        if (isEven(superstep)) {//0,2,4....

//            LOG.info("confronto \t" + prevStepRemovedVertex + "\t" + removedEdges);
            LOG.info("confronto \t" + prevStepRemovedVertex + "\t" + counterRemovedEdges);

//            Long vertices = this.getTotalNumVertices() - removedVertex.get();
            Long vertices = this.getTotalNumVertices() - counterRemovedVertecies;
            //con rimozione effettiva dei vertici ci vogliono 2 step per startup
            if (vertices == 0 && superstep > 2) {
                LOG.info("NO CHANGES - HALT COMPUTATION");
                LOG.info("BEST DENSITY\t" + bestlDensity + " at " + bestDensitySuperstep);
                this.haltComputation();
                return;
            }

            //Aggiorno variabile vertici rimossi per step successivo
//            prevStepRemovedVertex = removedEdges.get();
            prevStepRemovedVertex = counterRemovedEdges;

            //DENSITY UNDIRECT ρ(S) = (|E(S)| / 2 ) / |S|
            // |E(S)| / 2 perchè giraph rappresenta edge non diretto con 2 edge diretti
//            Long edges = this.getTotalNumEdges() - removedEdges.get();
            Long edges = this.getTotalNumEdges() - counterRemovedEdges;

            Double currDensity = (edges.doubleValue() / 2) / vertices.doubleValue();

            LOG.info("superstep\t" + superstep + "\tedge\t" + edges + "\tvertices\t" + vertices + "\tdensity\t" + currDensity);

            if (currDensity > bestlDensity) {
                bestlDensity = currDensity;
                bestDensitySuperstep = superstep - 2;//Densità calcolata sul supertep pari precedente
                this.getConf().setLong(OPTIMALSUPERSTEP, superstep);
            }

            //soglia = 2(1 + epsilon) ρ(S)
            Double soglia = 2 * (1 + epsilon) * currDensity;
            LOG.info("soglia = " + soglia);

            this.getContext().getConfiguration().setDouble(SOGLIA, soglia);

        }
//        else {//1,3,5...
//
//        }

    }

    @Override
    public void initialize() throws InstantiationException,
            IllegalAccessException {
        registerPersistentAggregator(REMOVEDEDGES, LongSumAggregator.class);
        registerPersistentAggregator(REMOVEDVERTICIES, LongSumAggregator.class);
    }

    private boolean isEven(long a) {
        return (a % 2 == 0);
    }

}
