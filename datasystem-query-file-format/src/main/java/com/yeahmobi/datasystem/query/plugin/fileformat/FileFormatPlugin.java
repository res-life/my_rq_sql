package com.yeahmobi.datasystem.query.plugin.fileformat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import ro.fortsoft.pf4j.Extension;
import ro.fortsoft.pf4j.Plugin;
import ro.fortsoft.pf4j.PluginWrapper;

import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.yeahmobi.datasystem.query.exception.ReportRuntimeException;
import com.yeahmobi.datasystem.query.extensions.FormatterGenerator;
import com.yeahmobi.datasystem.query.meta.ReportResult;
import com.yeahmobi.datasystem.query.meta.ReportResult.Entity;


public class FileFormatPlugin extends Plugin {

	public FileFormatPlugin(PluginWrapper wrapper) {
		super(wrapper);
	}

	@Extension
	public static class JsonFormatter implements FormatterGenerator {

		@Override
		public String getName() {
			return "json";
		}

		@Override
		public String asString(ReportResult reportResult) {
			return new Gson().toJson(reportResult);
		}

		@Override
		public void write(ReportResult reportResult, OutputStream outputStream) {
			String str = asString(reportResult);
			Utils.writeStrToStream(str, outputStream);
		}
	}

	@Extension
	public static class CsvFormatter implements FormatterGenerator {
		@Override
		public String getName() {
			return "csv";
		}

		@Override
		public String asString(ReportResult reportResult) {
			try (StringWriter stringWriter = new StringWriter(); CSVPrinter csvPrinter = new CSVPrinter(stringWriter, CSVFormat.DEFAULT)) {
				for (Object[] objs : reportResult.getData().getData()) {
					csvPrinter.printRecord(objs);
				}
				return stringWriter.toString();
			} catch (IOException e) {
				String msg = "generate result as csv string failed";
				logger.error(msg, e);

				// log the suppressed exception
				for (Throwable suppressedException : e.getSuppressed()) {
					logger.error("suppressed exception", suppressedException);
				}

				throw new ReportRuntimeException(e, msg);
			}
		}

		@Override
		public void write(ReportResult reportResult, OutputStream outputStream) {
			String str = asString(reportResult);
			Utils.writeStrToStream(str, outputStream);
		}
		
		private static Logger logger = Logger.getLogger(CsvFormatter.class);
	}

	

	@Extension
	public static class HtmlFormatter implements FormatterGenerator {

		@Override
		public String getName() {
			return "html";
		}

		@Override
		public String asString(ReportResult reportResult) {
			StringBuilder builder = new StringBuilder();
			builder.append("<table border=1 width=100%><tr>");
			builder.append(new Gson().toJson(reportResult.getData().getPage())).append("</tr>");
			Joiner colJoiner = Joiner.on("</td><td>").useForNull("");
			builder.append("<tr><td>");
			for (Object[] objects : reportResult.getData().getData()) {
				builder.append(colJoiner.join(objects));
				builder.append("</td></tr><tr><td>");
			}
			builder.append("</tr></table>");

			return builder.toString();
		}

		@Override
		public void write(ReportResult reportResult, OutputStream outputStream) {
			String str = asString(reportResult);
			Utils.writeStrToStream(str, outputStream);
		}
	}

	@Extension
	public static class XlsFormatter implements FormatterGenerator {
		@Override
		public String getName() {
			return "xls";
		}

		@Override
		public String asString(ReportResult reportResult) {
			throw new UnsupportedOperationException("xls file format is binary format, can't be exported as string");
		}

		@Override
		public void write(ReportResult reportResult, OutputStream outputStream) {

			HSSFWorkbook wb = new HSSFWorkbook();
			HSSFSheet sheet = wb.createSheet("QueryResult");
			int rowNum = 0;

			for (Object[] objects : reportResult.getData().getData()) {
				int colNum = 0;
				HSSFRow row = sheet.createRow(rowNum);
				rowNum++;
				for (Object obj : objects) {
					HSSFCell cell = row.createCell(colNum);
					cell.setCellValue(obj.toString());
					colNum++;
				}
			}

			try {
				wb.write(outputStream);
			} catch (IOException e) {
				String msg = "write xls file to output stream failed";
				logger.error(msg, e);
				throw new ReportRuntimeException(e, msg);
			}
		}

		private static Logger logger = Logger.getLogger(XlsFormatter.class);
	}

	public static void main(String[] args) {
		FormatterGenerator formattor = new FileFormatPlugin.CsvFormatter();
		ReportResult reportResult = new ReportResult();
		reportResult.setFlag("ok");
		reportResult.setMsg("ok");
		Entity data = new Entity();
		List<Object[]> l = new ArrayList<Object[]>();
		l.add(new Object[] { "a", "b" });
		l.add(new Object[] { "aa", "bb" });
		data.setData(l);
		reportResult.setData(data);
		String s = formattor.asString(reportResult);
		System.out.println(s);

		formattor = new FileFormatPlugin.XlsFormatter();
		File file = null;
		try {
			file = File.createTempFile("prefix", ".xls");
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(file.getAbsolutePath());

		try (OutputStream out = new FileOutputStream(file)) {
			formattor.write(reportResult, out);
		}catch (Exception e) {
			e.printStackTrace();
		}

		formattor = new FileFormatPlugin.HtmlFormatter();
		System.out.println(formattor.asString(reportResult));
		try {
			file = File.createTempFile("prefix", ".xls");
		} catch (IOException e) {
			e.printStackTrace();
		}
		try (OutputStream out = new FileOutputStream(file)) {
			formattor.write(reportResult, out);
		}catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(file.getAbsolutePath());
	}
}
