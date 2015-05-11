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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

/**
 * The values of the aggregators are broadcast to the workers before vertex.compute() is called and collected by the master before master.compute() is called. This means aggregator values used by the
 * workers are consistent with aggregator values from the master from the same superstep and aggregator used by the master are consistent with aggregator values from the workers from the previous
 * superstep.
 */
public class DenseSubgraphDirectMasterCompute extends MasterCompute {

    /**
     * Class logger
     */
    private static final Logger LOG = Logger.getLogger(DenseSubgraphDirectMasterCompute.class);
    /**
     * Somma aggregator name
     */    
    private static final String REMOVEDEDGES = "removedEdges";    
    private static final String REMOVEDVERTICIESINS = "removedVerticiesFromS";
    private static final String REMOVEDVERTICIESINT = "removedVerticiesFromT";
    /**
     * variabili globali
     */
    private static final String OPTIMALSUPERSTEP = "optimalSuperstep";
    private static final String SOGLIA = "soglia";
    private static final String PARTITIONTOPROCESS = "partitionToProcess";

    private static Long prevStepRemovedEdges = Long.MIN_VALUE;
    private static Boolean isPreviousPartitionS = Boolean.FALSE;
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

	if (superstep > 1) {

	    LongWritable removedEdges = this.getAggregatedValue(REMOVEDEDGES);//superstep precedente

	    LongWritable removedVertexInS = this.getAggregatedValue(REMOVEDVERTICIESINS);//superstep precedente	    
	    LongWritable removedVertexInT = this.getAggregatedValue(REMOVEDVERTICIESINT);//superstep precedente	    

	    if (isEven(superstep)) {//2,4....

		Long edges = this.getTotalNumEdges() - removedEdges.get();
		Long verticesInS = this.getTotalNumVertices() - removedVertexInS.get();
		Long verticesInT = this.getTotalNumVertices() - removedVertexInT.get();

		Boolean IsNextPartitionS = (verticesInS.doubleValue() / verticesInT.doubleValue()) >= c;
		Boolean SamePreviuosStepPartition = IsNextPartitionS.equals(isPreviousPartitionS);
		

		//con rimozione effettiva dei vertici ci vogliono 2 step per startup
		if ((prevStepRemovedEdges.equals(removedEdges.get())) && (superstep > 2) && SamePreviuosStepPartition) {
		    LOG.info("NO CHANGES - HALT COMPUTATION");
		    LOG.info("BEST DENSITY\t" + bestlDensity + " at " + bestDensitySuperstep);
		    this.haltComputation();
		    return;
		}

		//Aggiorno variabile vertici rimossi per check in step successivo
		prevStepRemovedEdges = removedEdges.get();
		isPreviousPartitionS = IsNextPartitionS;

		//EpSpSPp --> |E(S, T )| = |E ∩ (S×T)|
		Long EpSTp = this.getTotalNumEdges() - removedEdges.get();
		LOG.info("EpSTp\t" + this.getTotalNumEdges() + "\t" + removedEdges.get());

//		DENSITY DIRECT  ρ(S, T ) =  |E(S, T )| /  sqrt (|S||T |)
		Double currDensity = EpSTp.doubleValue() / Math.sqrt(verticesInS.doubleValue() * verticesInT.doubleValue());
		LOG.info("currDesity" + "\t" + verticesInS.doubleValue() + "\t" + verticesInT.doubleValue());

		if (currDensity > bestlDensity) {
		    bestlDensity = currDensity;
		    bestDensitySuperstep = superstep - 2; //Densità calcolata sul supertep pari precedente
		    this.getConf().setLong(OPTIMALSUPERSTEP, superstep);
		}
		
		//soglia a seconda della partizione
		Double soglia;
		if (IsNextPartitionS) {
		    //S                
		    LOG.info("partizione S");
		    LOG.info(edges + "\t" + verticesInS);
		    this.getContext().getConfiguration().setStrings(PARTITIONTOPROCESS, "S");
		    
		    // soglia = (1 + epsilon) * (|E(S, T)| / |S| )
		    soglia = (1 + epsilon) * ((EpSTp.doubleValue()) / verticesInS.doubleValue());

		} else {
		    //T
		    LOG.info("partizione T");
		    LOG.info(edges + "\t" + verticesInT);
		    this.getContext().getConfiguration().setStrings(PARTITIONTOPROCESS, "T");
		    
		    // soglia = (1 + epsilon) * (|E(S, T)| / |T| )
		    soglia = (1 + epsilon) * ((EpSTp.doubleValue()) / verticesInT.doubleValue());;
		}

		LOG.info("superstep\t" + superstep + "\t\tdensity\t" + currDensity);
		LOG.info("soglia = " + soglia);

		this.getContext().getConfiguration().setDouble(SOGLIA, soglia);

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

}
