package com.yeahmobi.datasystem.query.utils;

import java.util.UUID;

public class DbUtils {

	public static String createUniqueTableName(){
		return "t_"+ UUID.randomUUID().toString().replaceAll("-", "_");
	}
}
