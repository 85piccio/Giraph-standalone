reduce(String key, Iterator values){
	// key: ID Vertice
	// values: Lista vertici vicini
	for each u in values{
		for each w in values{
			if(u prec w){
				emit(v, (u,w) )
			}
		}	
	}
}
