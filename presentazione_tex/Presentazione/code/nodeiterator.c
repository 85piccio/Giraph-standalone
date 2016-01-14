T = 0;
for each v in V{
	for each u in vicinato(v) && u prec  v{
		for each w in in vicinato(v) && w prec u{
			if((u, w) is Edge){
				T = T + 1;
			}
		}
	}
}
