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
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.trash;

import it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.undirect.longwritable.Master;
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
public class DenseSubgraphMasterCompute_old extends MasterCompute {

    /**
     * Class logger
     */
    private static final Logger LOG = Logger.getLogger(Master.class);
    /**
     * Somma aggregator name
     */
    private static final String VERTECIES = "vertecies";
    private static final String EDGES = "edges";

    private static final String OPTIMALSUPERSTEP = "optimalSuperstep";
    private static final String SOGLIA = "soglia";
    private static final String REMOVEDVERTICIES = "removedVerticies";
    private static final String REMOVEDEDGES = "removedEdges";
    private static final String PREVSTEPREMOVEDEDVERTEX = "prevStepRemovedVertex";

    private static long optimalDensitySuperstep = 0;
    private static Double optimalDensity = Double.NEGATIVE_INFINITY;
    private static final Double epsilon = 0.001;

    @Override
    public void readFields(DataInput in) throws IOException {
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }

    @Override
    public void compute() {

        long superstep = getSuperstep();
//        System.out.println("master si trova superstep " + superstep);

        LongWritable removedEdges = this.getAggregatedValue(REMOVEDEDGES);//superstep precedente
        LongWritable removedVertex = this.getAggregatedValue(REMOVEDVERTICIES);//superstep precedente
        
        Long prevStepRemovedVertex = this.getContext().getConfiguration().getLong(PREVSTEPREMOVEDEDVERTEX, Long.MIN_VALUE);

        LOG.info("confronto \t" + prevStepRemovedVertex + "\t" + removedEdges);

        //con rimozione effettiva dei vertici ci vogliono 2 step per startup
        if ((prevStepRemovedVertex.equals(removedEdges.get())) && superstep > 1) {
            LOG.info("NO CHANGES - HALT COMPUTATION");
            LOG.info("BEST DENSITY\t" + optimalDensity + " at " + optimalDensitySuperstep);
            this.haltComputation();
            return;
        }

        //Aggiorno variabile vertici rimossi per step successivo
        this.getContext().getConfiguration().setLong(PREVSTEPREMOVEDEDVERTEX, removedEdges.get());

        //current density (relativo al supertstep precedente)
        
        //DENSITY ρ(S) = |E(S)| / |S|
        Long edges = this.getTotalNumEdges() -  removedEdges.get();
        Long vertices = this.getTotalNumVertices() - removedVertex.get();
        Double currDensity = (edges.doubleValue() / 2) / vertices.doubleValue();

        LOG.info("superstep\t" + superstep + "\tedge\t" + edges + "\tvertices\t" + vertices + "\tdensity\t" + currDensity);

        if (currDensity > optimalDensity) {
            optimalDensity = currDensity;
            optimalDensitySuperstep = superstep - 1;
            this.getConf().setLong(OPTIMALSUPERSTEP, superstep - 1);
        }

        //soglia = 2(1 + epsilon) ρ(S)
        Double soglia = 2 * (1 + epsilon) * currDensity;
        LOG.info("soglia = " + soglia);

        this.getContext().getConfiguration().setDouble(SOGLIA, soglia);

        //reset aggregators per calcolo densita superstep successivo
//        this.setAggregatedValue(VERTECIES, new DoubleWritable(0));
//        this.setAggregatedValue(EDGES, new DoubleWritable(0));
    }

    @Override
    public void initialize() throws InstantiationException,
            IllegalAccessException {
        registerPersistentAggregator(REMOVEDEDGES, LongSumAggregator.class);
        registerPersistentAggregator(REMOVEDVERTICIES, LongSumAggregator.class);
    }

}
