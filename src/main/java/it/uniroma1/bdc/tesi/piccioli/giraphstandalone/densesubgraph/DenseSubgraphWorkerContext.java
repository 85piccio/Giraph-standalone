/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph;

import org.apache.giraph.worker.DefaultWorkerContext;
import org.apache.log4j.Logger;

/**
 *
 * @author piccio
 */
public class DenseSubgraphWorkerContext extends DefaultWorkerContext {

    /**
     * Class logger
     */
    private static final Logger LOG = Logger.getLogger(DenseSubgraphWorkerContext.class);
    /**
     * Somma aggregator name
     */
    private static final String VERTECIES = "vertecies";
    private static final String EDGES = "edges";

    private static final String SOGLIA = "soglia";
    private static final String LASTDENSITY = "lastDensity";
    private static final Double epsilon = Double.MIN_NORMAL;

    @Override
    public void preSuperstep() {
        super.preSuperstep();

//        long superstep = getSuperstep();
//
//        //compute density
//        LOG.info("compute density superstep " + superstep);
//
//        Double edges = this.getAggregatedValue(VERTECIES);
//        Double vertices = this.getAggregatedValue(EDGES);
//        Double density = (edges / 2) / vertices;
//
//        LOG.info("compute " + edges + "\\" + vertices);
//        //compute density
//        LOG.info("density = " + density);
//
//        Double soglia = 2 * (1 + epsilon) * density;
//        LOG.info("soglia = " + soglia);
//
//        this.getContext().getConfiguration().setDouble(SOGLIA, soglia);
        
        
        
    }
    

    
}
