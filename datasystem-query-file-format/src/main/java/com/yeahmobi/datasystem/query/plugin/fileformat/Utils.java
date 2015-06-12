package com.yeahmobi.datasystem.query.plugin.fileformat;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.log4j.Logger;

import com.yeahmobi.datasystem.query.exception.ReportRuntimeException;
import com.yeahmobi.datasystem.query.plugin.fileformat.FileFormatPlugin.JsonFormatter;

public class Utils {

	private static Logger logger = Logger.getLogger(JsonFormatter.class);

	public static void writeStrToStream(String str, OutputStream outputStream) {

		try (Writer writer = new OutputStreamWriter(outputStream, "utf-8")) {
			writer.write(str);
		} catch (Exception e) {
			String msg = "write string into output stream failed";
			logger.error(msg, e);

			// log the suppressed exception
			for (Throwable suppressedException : e.getSuppressed()) {
				logger.error("suppressed exception", suppressedException);
			}

			throw new ReportRuntimeException(e, msg);
		}
	}
}
