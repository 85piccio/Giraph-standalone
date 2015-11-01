compute(Vertex vertex, Messages messages){
	degree = 0;
	for each u in vertex.getEdegs(){
		degree += 1;
	}
	vertex.value = degree;
	vertex.voteTohalt();
}

