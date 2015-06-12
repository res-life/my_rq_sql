package com.yeahmobi.datasystem.query.antlr4;

import com.yeahmobi.datasystem.query.exception.ReportParserException;
import org.antlr.v4.runtime.*;

/**
 * 
 * 将解释器封装成一个方法
 * <p>
 * 将我们自定义的语法转换成druid的语法格式(有关druid格式参考:)
 * 
 * @author chenyi
 * 
 */
public class WarpInterpreter {

    public static DruidReportParser convert(String queryParams, String dataSource, BaseErrorListener listener) throws ReportParserException {
        CharStream stream = new ANTLRInputStream(queryParams);

        DruidReportLexer lexer = new DruidReportLexer(stream);
        TokenStream tokenStream = new CommonTokenStream(lexer);
        DruidReportParser parser = new DruidReportParser(tokenStream);
        lexer.removeErrorListeners();
        parser.removeErrorListeners();

        lexer.addErrorListener(listener);
        parser.addErrorListener(listener);
        parser.setDataSource(dataSource);

        try {
            parser.query();
            if (parser.getNumberOfSyntaxErrors() > 0) {
                throw new ReportParserException("param syntax error");
            }
        } catch (Exception e) {
        	e.printStackTrace();
            throw new ReportParserException("param parse error");
        }

        /*if (parser.aggregators.isEmpty()) {
            throw new ReportParserException("at least one metric given in data section");
        }*/

        return parser;
    }
}
