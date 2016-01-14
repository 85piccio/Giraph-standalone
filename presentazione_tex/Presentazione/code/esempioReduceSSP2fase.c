reduce(String v, Iterator vicini){
	// v: ID Vertice 
	// vicini: Lista vertici vicini 
	int dist_min = v.distanza;
	for each w in vicini{		
		if(w.$ == S){
			if(dist_min  > w.distanza){
				dist_min = w.distanza;
			}
		}	
	}
	for each w in vicini{
		if(w.$ == D){
			emit(v:dist_min,w)
		}
	}
}
