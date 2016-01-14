map(String v, String u){
	// key: vertice di partenza
	// value: vertice destinazione
	if(input <v, deg(v) >){
		if( deg(v) > soglia){
			Emit(v, $);
		}
	}
	if(input <v, u>){
		Emit(v, u);
	}
}
