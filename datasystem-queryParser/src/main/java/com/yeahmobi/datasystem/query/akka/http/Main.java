package com.yeahmobi.datasystem.query.akka.http;

import io.druid.data.input.MapBasedRow;

import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.List;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;

public class Main {

	public static void main(String[] args) throws Exception {

		final AyncInputStream stream = new AyncInputStream();

		final String input = "[{\"version\":\"v1\",\"timestamp\":\"2015-01-06T05:37:00.000Z\",\"event\":{\"clicks\":28}},{\"version\":\"v1\",\"timestamp\":\"2015-01-06T05:37:00.000Z\",\"event\":{\"clicks\":3,\"sub5\":\"BB1\"}}]";
		final String input1 = "[{\"version\":\"v1\",\"timestamp\":\"2015-01-06T05:37:00.000Z\",\"event\":{\"clicks\":28}},{sdf";
		final String input2 = "[{\"version\":\"v1\",\"timestamp\":\"2015-01-06T05:37:00.000Z\",\"event\":{\"clicks\":28}},{\"version\":\"v1\",\"timestamp\":\"2015-01-06T05:37:00.000Z\",\"event\":{\"clicks\":3,\"sub5\":\"BB1\"}}]";
		

		final JsonFactory jsonFactory = new JsonFactory();
		final ObjectMapper mapper = new ObjectMapper().registerModule(new JodaModule());

		JsonParser jsonParser = jsonFactory.createParser(input1.getBytes("UTF-8"));

		TypeReference<MapBasedRow> type = new TypeReference<MapBasedRow>() {
		};

		List<Object> rows = new LinkedList<>();
		
		// [
		
		jsonParser.nextToken();
		while (true) {
			try {
				JsonToken token = jsonParser.nextToken();
				if (token == JsonToken.START_OBJECT) {
					Object row = mapper.readValue(jsonParser, type);

					rows.add(row);
					System.out.println("abc");
				} else {
					System.out.println("abc");
					break;
				}
			} catch (JsonProcessingException e) { // may occur when
													// parsing,
				// just ignore
				System.out.println("abc");
				break;
			}
		}

	}
}
