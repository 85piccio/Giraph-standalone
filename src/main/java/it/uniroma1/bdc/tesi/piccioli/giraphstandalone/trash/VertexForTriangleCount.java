/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.trash;

import org.apache.giraph.graph.DefaultVertex;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author piccio
 */
public class VertexForTriangleCount extends DefaultVertex {

    @Override
    public void removeEdges(WritableComparable targetVertexId) {
         try {
        super.removeEdges(targetVertexId); //To change body of generated methods, choose Tools | Templates.
         }catch(Exception e){
//             System.out.println("edge non trovato");
         }
    }
    
    
    
}
