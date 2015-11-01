map(String v, String u){
	// v: vertice di partenza
	// u: vertice destinazione
	if(input <v, deg(v) >){
		if( deg(v) > soglia){
			Emit(v, $);
		}
	}
	if(input <v, u>){
		Emit(v, u);
	}
}
