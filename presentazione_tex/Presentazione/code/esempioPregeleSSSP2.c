compute(Vertex vertex, Messages messages){
        if (Superstep == 0) {
        	vertex.value = MAX_VALUE;
		if(vertex = sorgente){
			vertex.value = 0;
		}	
        }
	distanza_min = vertex.value;
	for each m in messages{
	    minDist = min(minDist, m);
	}

	if (minDist < vertex.value) {
		//Valore distanza aggiornato
		vertex.value = minDist;
		for each edge in vertex.getEdegs(){
			distance = minDist + 1;
			sendMessage(edge.ID, minDist + 1);
		}
	}
	vertex.voteToHalt();	
    }
}

