reduce(String v, Iterator vicini){
	// v: ID Vertice
	// vicini: Lista vertici vicini
	if($ in vicini){
		for each u in vicini{
			emit(v,u);
		}
	}
}
