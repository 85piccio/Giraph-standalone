reduce(String key, Iterator values){
	// key: ID Vertice
	// values: Lista vertici vicini
	int dist_min = key.distanza;
	for each v in values{
		if(dist_min  > v.distanza + 1 ){
			dist_min = v.distanza + 1;
		}	
	}
	for each v in values{
		emit(v,k:dist_min+1)
	}
}
