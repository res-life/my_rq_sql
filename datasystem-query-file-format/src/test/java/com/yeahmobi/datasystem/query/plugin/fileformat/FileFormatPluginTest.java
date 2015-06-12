/**
 * 
 */
package com.yeahmobi.datasystem.query.plugin.fileformat;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.*;
import org.junit.rules.ExpectedException;

import com.yeahmobi.datasystem.query.extensions.FormatterGenerator;
import com.yeahmobi.datasystem.query.meta.ReportResult;
import com.yeahmobi.datasystem.query.meta.ReportResult.Entity;
import com.yeahmobi.datasystem.query.plugin.fileformat.FileFormatPlugin.HtmlFormatter;
import com.yeahmobi.datasystem.query.plugin.fileformat.FileFormatPlugin.XlsFormatter;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

/**
 * @author jeff.yu
 * @date 2014年9月23日
 */
public class FileFormatPluginTest {
	
	
	public ReportResult get_reportResult() {
		
		ReportResult reportResult = new ReportResult();
		reportResult.setFlag("ok");
		reportResult.setMsg("ok");
		Entity data = new Entity();
		List<Object[]> l = new ArrayList<Object[]>();
		l.add(new Object[] { "offer_id", "click" });
		l.add(new Object[] { "13044", "7865" });
		data.setData(l);
		reportResult.setData(data);
		return reportResult;
	}
	
	public Boolean containsString(String actualString, String findString) {
		if (actualString.contains(findString)){
			return true;
		}
		return false;
	}
	
	@Test
	public void JsonFormatTest() {
		FormatterGenerator formattor = new FileFormatPlugin.JsonFormatter();

		String s = formattor.asString(get_reportResult());
		try {
			new JsonParser().parse(s);
			Assert.assertTrue(true);
		} catch (JsonParseException e ) {
			Assert.assertTrue(false);
		}
	}
	
	
	@Test
	public void JsonFormatGetName() {
		FormatterGenerator formattor = new FileFormatPlugin.JsonFormatter();
		Assert.assertEquals("json", formattor.getName());
	}
	
	@Test
	public void JsonFormatWriter() {
		File file = null;
		FormatterGenerator formattor = new FileFormatPlugin.JsonFormatter();
		try {
			file = File.createTempFile("prefix", ".json");
		} catch (IOException e) {
			e.printStackTrace();
		}
		try (OutputStream out = new FileOutputStream(file)) {
			formattor.write(get_reportResult(), out);
		}catch (Exception e) {
			e.printStackTrace();
		}
		Assert.assertTrue(containsString(file.getAbsolutePath(), ".json"));
	}
	
	
	@Test
	public void CsvFormatterGetName() {
		FormatterGenerator formattor = new FileFormatPlugin.CsvFormatter();
		Assert.assertEquals("csv", formattor.getName());
	}
	
	@Test
	public void CsvFormatterWriter() {
		File file = null;
		FormatterGenerator formattor = new FileFormatPlugin.CsvFormatter();
		String s = formattor.asString(get_reportResult());
		try {
			file = File.createTempFile("prefix", ".csv");
		} catch (IOException e) {
			e.printStackTrace();
		}
		try (OutputStream out = new FileOutputStream(file)) {
			formattor.write(get_reportResult(), out);
		}catch (Exception e) {
			e.printStackTrace();
		}
		Assert.assertTrue(containsString(file.getAbsolutePath(), ".csv"));
	}
	
	@Test
	public void CsvFormatterAsString() {
		File file = null;
		FormatterGenerator formattor = new FileFormatPlugin.CsvFormatter();
		String actual = formattor.asString(get_reportResult());
		String expected = "13044";
		Assert.assertTrue(containsString(actual, expected));
	}
	
	
	@Test
	public void HtmlFormatterGetName() {
		FormatterGenerator formattor = new FileFormatPlugin.HtmlFormatter();
		Assert.assertEquals("html", formattor.getName());
	}
	
	@Test
	public void HtmlFormatterWriter() {
		File file = null;
		FormatterGenerator formattor = new FileFormatPlugin.HtmlFormatter();
		String s = formattor.asString(get_reportResult());
		try {
			file = File.createTempFile("prefix", ".html");
		} catch (IOException e) {
			e.printStackTrace();
		}
		try (OutputStream out = new FileOutputStream(file)) {
			formattor.write(get_reportResult(), out);
		}catch (Exception e) {
			e.printStackTrace();
		}
		Assert.assertTrue(containsString(file.getAbsolutePath(), ".html"));
	}
	
	@Test
	public void HtmlFormatterAsString() {
		File file = null;
		FormatterGenerator formattor = new FileFormatPlugin.HtmlFormatter();
		String expected = "<table border=1 width=100%><tr>null</tr><tr><td>offer_id</td><td>click</td></tr><tr><td>13044</td><td>7865</td></tr><tr><td></tr></table>";
		String actual = formattor.asString(get_reportResult());
		Assert.assertEquals(expected, actual);
	}
	
	
	@Test
	public void XlsFormatterGetName() {
		FormatterGenerator formattor = new FileFormatPlugin.XlsFormatter();
		Assert.assertEquals("xls", formattor.getName());
	}
	
	@Test
	public void XlsFormatterWriter() {
		File file = null;
		FormatterGenerator formattor = new FileFormatPlugin.XlsFormatter();
		try {
			file = File.createTempFile("prefix", ".xls");
		} catch (IOException e) {
			e.printStackTrace();
		}
		try (OutputStream out = new FileOutputStream(file)) {
			formattor.write(get_reportResult(), out);
		}catch (Exception e) {
			e.printStackTrace();
		}
		Assert.assertTrue(containsString(file.getAbsolutePath(), ".xls"));
	}
	
	@Test
	public void XlsFormatterAsString() {
		File file = null;
		FormatterGenerator formattor = new FileFormatPlugin.XlsFormatter();
		try {
			String s = formattor.asString(get_reportResult());
		} catch (UnsupportedOperationException e){
			Assert.assertTrue(true);
		}	
	}

}
