reduce(String v, Iterator vicini){
	// v: ID Vertice v
	// vicini: Lista vertici vicini
	int degree= 0;
	foreach u in vicini{
		degree += 1;
	}
	emit(v,degree);
}
