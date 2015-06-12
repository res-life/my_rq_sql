package com.yeahmobi.datasystem.query.exception;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.apache.log4j.Logger;

/**
 * 解析异常处理类打印到log文件中
 * 
 * @author chenyi
 * 
 */
public class FileErrorListener extends BaseErrorListener {
    public static final FileErrorListener INSTANCE = new FileErrorListener();

    private static final Logger logger = Logger.getLogger(FileErrorListener.class);

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg,
            RecognitionException e) {
        logger.error("line " + line + ":" + charPositionInLine + " " + msg);
    }

}
