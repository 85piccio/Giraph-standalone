map(String v, String u){
	// v: vertice sorgente
	// u: vertice destinazione
	Emit(v.ID, u : v.distanza : S);
	Emit(u.ID, v : u.distanza : D);
}
