package com.yeahmobi.datasystem.query.landingpage;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

import com.yeahmobi.datasystem.query.akka.cache.db.DbFieldType;

public class Main {

	public static void main(String[] args) {

//		H2InMemoryDbUtil
//				.executeDbStatement(
//						"create table if not exist T(F1 varchar, F2 varchar, F3 varchar, F4 varchar, F5 varchar, F6 varchar, F7 varchar, F8 varchar, F9 varchar, F10 varchar, F11 varchar, F12 varchar, F13 varchar, F14 varchar, F15 varchar, F16 varchar, F17 varchar, F18 varchar, F19 varchar, F20 varchar,F21 varchar, F22 varchar, F23 varchar, F24 varchar, F25 varchar, F26 varchar, F27 varchar, F28 varchar, F29 varchar, F30 varchar,F31 varchar, F32 varchar, F33 varchar, F34 varchar, F35 varchar, F36 varchar, F37 varchar, F38 varchar, F39 varchar, F40 varchar)",
//						"e");
		LinkedHashMap<String, DbFieldType> linkMap = new LinkedHashMap<>();
		for (int i = 1; i <= 40; ++i) {
			linkMap.put("F" + i, DbFieldType.VARCHAR);
		}

		System.out.println(System.currentTimeMillis() / 1000);
		List<Object[]> rows = new LinkedList<>();
		for (int i = 1; i <= 1000000; ++i) {
			Object[] row = new Object[40];
			for (int j = 0; j < 40; ++j) {
				row[j] = "Row_" + i + "_COL_" + j + "_TEST_TEST_TEST";
			}
			rows.add(row);
		}

		System.out.println(System.currentTimeMillis() / 1000);
		
		H2InMemoryDbUtil.insertData("T", linkMap, rows);
		
		System.out.println(System.currentTimeMillis() / 1000);

	}

}
