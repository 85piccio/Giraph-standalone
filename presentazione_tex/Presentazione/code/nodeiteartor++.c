T = 0
for each v in V{
	for each u in vicinato(v) && u prec  v{
		for each w in vicinato(v) && w prec u{
			if((u, w) è un Edge){
				T = T + 1
			}
		}
	}		
}
