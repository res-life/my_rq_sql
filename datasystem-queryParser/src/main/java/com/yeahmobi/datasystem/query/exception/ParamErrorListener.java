package com.yeahmobi.datasystem.query.exception;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

/**
 * Created by wanghw on 14-04-30.
 */

public class ParamErrorListener extends BaseErrorListener {
	public static final ParamErrorListener INSTANCE = new ParamErrorListener();
	String emsg = null;

	@Override
	public void syntaxError(Recognizer<?, ?> recognizer,
			Object offendingSymbol, int line, int charPositionInLine,
			String msg, RecognitionException e) {
		emsg = line + " " + charPositionInLine + " " + msg;
	}

	public String toString() {
		return emsg;
	}

}
