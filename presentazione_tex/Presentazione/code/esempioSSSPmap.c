reduce(String v, Iterator vicini){
	// v: ID Vertice
	// vicini: Lista vertici vicini
	int dist_min = v.distanza;
	for each w in vicini{
		if(dist_min + 1 < v.distanza ){
			//Nodo bi nuova distanza mimima dal nodo sorgente
			emit(v, w:dist_min + 1);
		}else{
			//Nodo bi mantiene distanza minimia	
			emit(v, w);
		}
	}
}
