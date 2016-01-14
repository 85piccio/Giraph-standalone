reduce(String v, Iterator vicini){
	// v: ID Vertice
	// vicini: Lista vertici vicini
	int dist_min = v.distanza;
	for each w in vicini{
		if(dist_min  > w.distanza + 1 ){
			dist_min = w.distanza + 1;
		}	
	}
	for each w in values{
		emit(v, w:dist_min+1)
	}
}
