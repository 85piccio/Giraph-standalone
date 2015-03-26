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
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.direct;

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
public class DenseSubgraphDirectMasterCompute extends MasterCompute {

    /**
     * Class logger
     */
    private static final Logger LOG = Logger.getLogger(DenseSubgraphDirectMasterCompute.class);
    /**
     * Somma aggregator name
     */
    private static final String REMOVEDVERTICIESINS = "removedVerticiesFromS";
    private static final String REMOVEDEDGESINS = "removedEdgesFromS";
    private static final String REMOVEDVERTICIESINT = "removedVerticiesFromT";
    private static final String REMOVEDEDGESINT = "removedEdgesFromT";
    /**
     * variabili globali
     */
    private static final String OPTIMALSUPERSTEP = "optimalSuperstep";
    private static final String SOGLIA = "soglia";
    private static final String PARTITIONTOPROCESS = "partitionToProcess";

//    private static final String PREVSTEPREMOVEDEDVERTEX = "prevStepRemovedVertex";
    private static Long prevStepRemovedVertexFromS = Long.MIN_VALUE;
    private static Long prevStepRemovedVertexFromT = Long.MIN_VALUE;
    private static long bestDensitySuperstep = 0;
    private static Double bestlDensity = Double.NEGATIVE_INFINITY;
    private static final Double epsilon = 0.001;
    private static final Double c = 1.0;

    @Override
    public void readFields(DataInput in) throws IOException {
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }

    @Override
    public void compute() {

        long superstep = getSuperstep();

        LongWritable removedEdgesInS = this.getAggregatedValue(REMOVEDEDGESINS);//superstep precedente
        LongWritable removedVertexInS = this.getAggregatedValue(REMOVEDVERTICIESINS);//superstep precedente
        LongWritable removedEdgesInT = this.getAggregatedValue(REMOVEDEDGESINT);//superstep precedente
        LongWritable removedVertexInT = this.getAggregatedValue(REMOVEDVERTICIESINT);//superstep precedente

        if (isEven(superstep)) {//0,2,4....

//            LOG.info("confronto \t" + prevStepRemovedVertex + "\t" + removedEdges);
            //con rimozione effettiva dei vertici ci vogliono 2 step per startup
            if (((prevStepRemovedVertexFromS.equals(removedEdgesInS.get())) || (prevStepRemovedVertexFromT.equals(removedEdgesInT.get())))
                    && superstep > 2) {
                LOG.info("NO CHANGES - HALT COMPUTATION");
                LOG.info("BEST DENSITY\t" + bestlDensity + " at " + bestDensitySuperstep);
                this.haltComputation();
                return;
            }

            //Aggiorno variabile vertici rimossi per step successivo
//            this.getContext().getConfiguration().setLong(PREVSTEPREMOVEDEDVERTEX, removedEdges.get());
            prevStepRemovedVertexFromS = removedEdgesInS.get();
            prevStepRemovedVertexFromT = removedEdgesInT.get();

            //DENSITY UNDIRECT ρ(S) = (|E(S)| / 2 ) / |S|
            Long edgesInS = this.getTotalNumEdges() - removedEdgesInS.get();
            Long verticesInS = this.getTotalNumVertices() - removedVertexInS.get();
            Long edgesInT = this.getTotalNumEdges() - removedEdgesInT.get();
            Long verticesInT = this.getTotalNumVertices() - removedVertexInT.get();

            Double currDensity = (edgesInS.doubleValue() * edgesInT.doubleValue()) / Math.sqrt(verticesInS.doubleValue() * verticesInT.doubleValue());

//            LOG.info("superstep\t" + superstep + "\tedge\t" + edges + "\tvertices\t" + vertices + "\tdensity\t" + currDensity);
            if (currDensity > bestlDensity) {
                bestlDensity = currDensity;
                bestDensitySuperstep = superstep;
                this.getConf().setLong(OPTIMALSUPERSTEP, superstep);
            }
            Double soglia;
            if ((verticesInS.doubleValue() / verticesInT.doubleValue()) >= c) {
                //S                
                this.getContext().getConfiguration().setStrings(PARTITIONTOPROCESS, "S");
                soglia = (1 + epsilon) * ((edgesInS.doubleValue() * edgesInT.doubleValue()) / verticesInS.doubleValue());
            } else {
                //T
                this.getContext().getConfiguration().setStrings(PARTITIONTOPROCESS, "T");
                soglia = (1 + epsilon) * ((edgesInS.doubleValue() * edgesInT.doubleValue()) / verticesInT.doubleValue());
            }

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
        registerPersistentAggregator(REMOVEDEDGESINT, LongSumAggregator.class);
        registerPersistentAggregator(REMOVEDVERTICIESINT, LongSumAggregator.class);
        registerPersistentAggregator(REMOVEDEDGESINS, LongSumAggregator.class);
        registerPersistentAggregator(REMOVEDVERTICIESINS, LongSumAggregator.class);
    }

    private boolean isEven(long a) {
        return (a % 2 == 0);
    }

}
