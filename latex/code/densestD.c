G = (V, E)
epsilon > 0
c > 0
S = V
T = V
while (S not EMPTY AND T not EMPTY){
	if( |S|/|T| > c){
		for each v in S{
			soglia_S = (1 + epsilon) * |E(S,T)| / |S| 
			if ( deg_u(v) <= soglia_S ) {
				S = S - {v}
			}
		}
	}else{
		for each u in T{
			soglia_T = (1 + epsilon) * |E(S,T)| / |T|
			if ( deg_e(u) <= soglia_T ){
				T = T - {v}
			}
		}
	}

	
	if ( rho(S,T) > rho*(S,T) ){
		S* = S
		T* = T
	}
}
return S*, T*;


