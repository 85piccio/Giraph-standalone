G = (V, E)
epsilon > 0
S  = V
while (S not EMPTY){
	for each v in S{
		if ( deg(v) <= 2 * (1 + epsilon) * rho(S) ){
			S = S - {v}
		}
	}
	if ( rho(S) > rho*(S) ){
		S* = S
	}		
}
return S*;

