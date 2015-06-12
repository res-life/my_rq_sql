package com.yeahmobi.datasystem.query.utils;

public class Triple<T1, T2, T3> {

	public T1 t1;
	public T2 t2;
	public T3 t3;

	public static <T1, T2, T3> Triple<T1, T2, T3> of(T1 t1, T2 t2, T3 t3) {
		return new Triple<>(t1, t2, t3);
	}

	public Triple(T1 t1, T2 t2, T3 t3) {
		this.t1 = t1;
		this.t2 = t2;
		this.t3 = t3;
	}
}
