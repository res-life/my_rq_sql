package com.yeahmobi.datasystem.query.akka.http;

import static org.junit.Assert.assertEquals;
import io.druid.data.input.MapBasedRow;

import java.util.ArrayList;
import java.util.List;

import jersey.repackaged.com.google.common.collect.Lists;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.primitives.Bytes;

public class ByteStreamJsonParserTest {

	static final String input1 = "{\"version\":\"v1\",\"timestamp\":\"2015-01-01T00:00:00.000Z\",\"event\":{\"clicks\":1}}";
	static final String input2 = "{\"version\":\"v1\",\"timestamp\":\"2015-01-02T00:00:00.000Z\",\"event\":{\"clicks\":2}}";

	@Test
	public void test() throws Exception {

		MapBasedRow row1 = parseRow(input1);
		MapBasedRow row2 = parseRow(input2);
		List<MapBasedRow> expectRows1 = Lists.newArrayList(row1);
		List<MapBasedRow> expectRows2 = Lists.newArrayList(row2);

		final String input = "[" + input1 + "," + input2 + "]";
		JsonFactory jsonFactory = new JsonFactory();
		ObjectMapper mapper = new ObjectMapper().registerModule(new JodaModule());
		TypeReference<?> type = new TypeReference<MapBasedRow>() {
		};

		ByteStreamJsonParser parser = new ByteStreamJsonParser(jsonFactory, mapper, type);
		byte[] bytes = input.getBytes("UTF-8");

		int row1DelimiterIndex = input1.getBytes("UTF-8").length;
		int row2DelimiterIndex = input1.getBytes("UTF-8").length + input2.getBytes("UTF-8").length + 1;

		for (int i = 0; i < bytes.length; ++i) {
			List<Byte> blist = Lists.newArrayList();
			blist.add(bytes[i]);
			List<Object> rows = parser.tryParse(blist);

			if (i < row1DelimiterIndex) {
				assertEquals(new ArrayList<Object>(), rows);
			} else if (i == row1DelimiterIndex) {
				assertEquals(expectRows1, rows);
			} else if (i > row1DelimiterIndex && i < row2DelimiterIndex) {
				assertEquals(new ArrayList<Object>(), rows);
			} else if (i == row2DelimiterIndex) {
				assertEquals(expectRows2, rows);
			} else {
				assertEquals(new ArrayList<Object>(), rows);
			}
		}
	}

	static final String input3 = "{\"version\":\"v1\",\"timestamp\":\"2015-01-03T00:00:00.000Z\",\"event\":{\"clicks\":3}}";
	static final String input4 = "{\"version\":\"v1\",\"timestamp\":\"2015-01-04T00:00:00.000Z\",\"event\":{\"clicks\":4}}";
	static final String input5 = "{\"version\":\"v1\",\"timestamp\":\"2015-01-05T00:00:00.000Z\",\"event\":{\"clicks\":5}}";

	@Test
	public void test2() throws Exception {

		MapBasedRow row1 = parseRow(input1);
		MapBasedRow row2 = parseRow(input2);
		MapBasedRow row3 = parseRow(input3);
		MapBasedRow row4 = parseRow(input4);
		MapBasedRow row5 = parseRow(input5);

		final String input123 = "[" + input1 + "," + input2 + "," + input3;
		String input45 = "," + input4 + "," + input5;
		JsonFactory jsonFactory = new JsonFactory();
		ObjectMapper mapper = new ObjectMapper().registerModule(new JodaModule());
		TypeReference<?> type = new TypeReference<MapBasedRow>() {
		};

		ByteStreamJsonParser parser = new ByteStreamJsonParser(jsonFactory, mapper, type);
		byte[] bytes = input123.getBytes("UTF-8");
		byte[] bytes2 = input45.getBytes("UTF-8");

		assertEquals(Lists.newArrayList(row1, row2, row3), parser.tryParse(Bytes.asList(bytes)));
		assertEquals(Lists.newArrayList(row4, row5), parser.tryParse(Bytes.asList(bytes2)));
	}

	private static MapBasedRow parseRow(String str) throws Exception {
		final JsonFactory jsonFactory = new JsonFactory();
		final ObjectMapper mapper = new ObjectMapper().registerModule(new JodaModule());

		JsonParser jsonParser = jsonFactory.createParser(str.getBytes("UTF-8"));

		TypeReference<MapBasedRow> type = new TypeReference<MapBasedRow>() {
		};
		MapBasedRow row = mapper.readValue(jsonParser, type);
		return row;
	}

}
