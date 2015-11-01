compute(Vertex vertex, Messages messages){
	if (getSuperstep() == 0) {
            degree = vertex.getNumEdges();
            
            for each edge in vertex.getEdegs(){
                sendMessage(edge.ID, degree);
            }

        } else if (getSuperstep() == 1) {

	    for each m in messages{
                if (vertex.ID prec m.ID) {
                    this.removeEdge(m.ID, vertexId);
                }
            }
        } else if (getSuperstep() == 2) {
            for each1 edge in vertex.getEdegs(){
		for each2 edge in vertex.getEdegs(){
                	sendMessage(edge1.ID, edge2.ID);
            }
        } else if (getSuperstep() == 3) {
            for each m in messages{
                if (m is Edge) {
                    T++;
                }
            }
            aggregate(T);
            vertex.voteToHalt();
        }

    }
