compute(Vertex vertex, Messages messages){
	if (Superstep % 2 == 0) {

		rho = |E(G)| / |V(G)| ;
		soglia = 2 * (1 + epsilon) * rho;

		if(deg(v) <= soglia ){
			elimina vertex;
			for each edge in vertex.getEdegs(){
				elimina edge;
				sendMessage(edge.ID, vertex.ID);
			}
		}
	}

	} else if (Superstep % 2 == 1)  {

	    for each m in messages{
		for each edge in vertex.getEdegs(){
			if(edge.ID  == m)
		    		elimina edge;
		}
	    }
	}
}
