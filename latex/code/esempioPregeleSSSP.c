compute(Vertex vertex, Messages messages){
	dist_min = vertex.value;
	for each msg in messages{
		if(msg + 1 < dist_min):
			dist_min= msg + 1;
	}
	if(dist_min == vertex.value){
		//Valore distanza non aggiornato
		vertex.vote_to_halt();
	}
	else{
		//Valore distanza aggiornato
		vertex.value = dist_min;
	}
}
