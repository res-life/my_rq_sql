package com.yeahmobi.datasystem.query.antlr4;

import java.util.LinkedHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jersey.repackaged.com.google.common.base.Splitter;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.yeahmobi.datasystem.query.exception.FileErrorListener;
import com.yeahmobi.datasystem.query.exception.ReportParserException;
import com.yeahmobi.datasystem.query.exception.ReportRuntimeException;
import com.yeahmobi.datasystem.query.meta.ImpalaCfg;
import com.yeahmobi.datasystem.query.meta.ImpalaCfgItem;

/**
 * 
 * 将解释器封装成一个方法
 * <p>
 * 将我们自定义的语法转换成druid的语法格式(有关druid格式参考:)
 * 
 * @author chenyi
 * 
 */
public class SqlParserTest {

	public static void main(String[] args) throws ReportParserException {
		String sql = "select count(*), (1 - count(*) / sum(a.b)) * 100 as ratio from wikipedia wili_alias "
				+ " left join table2 t on a.a = b.b and c.c = d.d" + " where"
				+ " timestamp between '2013-02-01' and '2013-02-14'"
				+ " and (namespace = 'article' or page regexp 'Talk:.*')" + " and language in ( 'en', 'fr' ) "
				+ " and user regexp '(?i)^david.*'" + " group by granularity(timestamp, 'day'), language";
		SQLParser parser = SqlParserTest.parse(sql, new FileErrorListener());
		
	}

	public static SQLParser parse(String sql, BaseErrorListener listener) throws ReportParserException {
		
		// 公式替换
		SqlContext sqlContext = tranformMetric(sql);
		
		CharStream stream = new ANTLRInputStream(sqlContext.getTransformedSql());

		SQLLexer lexer = new SQLLexer(stream);
		TokenStream tokenStream = new CommonTokenStream(lexer);
		SQLParser parser = new SQLParser(tokenStream);
		lexer.removeErrorListeners();
		parser.removeErrorListeners();

		lexer.addErrorListener(listener);
		parser.addErrorListener(listener);

		try {
			parser.query();
			if (parser.getNumberOfSyntaxErrors() > 0) {
				throw new ReportParserException("param syntax error");
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new ReportParserException("param parse error");
		}

		/*
		 * if (parser.aggregators.isEmpty()) { throw new
		 * ReportParserException("at least one metric given in data section"); }
		 */

		return parser;
	}

	private static SqlContext tranformMetric(String sql) {
		// 公式替换
		final Pattern pattern = Pattern.compile("(select) (.+) from ([0-9,a-z,_]+) (.*)");
		final Matcher matcher = pattern.matcher(sql);
		if(!matcher.find()){
			// impala druid均可
			throw new ReportRuntimeException("request [%s] has syntax error, pattern is: [select ... from ...]", sql);
		}

		System.out.println(matcher.group(1));
		String elements = matcher.group(2);
		
		System.out.println(elements);
		System.out.println("from");
		String dataSource = matcher.group(3);
		System.out.println(dataSource);

		String rightStr = matcher.group(4);
		System.out.println(rightStr);
		final Pattern pattern2 = Pattern.compile("(.*) join (.*) on (.*)");
		final Matcher matcher2 = pattern2.matcher(rightStr);
		
		
		String metrics = null;
		boolean isJoin = false;
		LinkedHashMap<String, String> metricMap = new LinkedHashMap<>();
		if(matcher2.find()){
			isJoin = true;
			// 是join语法, 路由到impala
			// 公式替换
			System.out.println("have join");
			Iterable<String> eles = Splitter.on(',').split(elements.trim());
			Iterable<String> transedEles = Iterables.transform(eles, new Function<String, String>(){

				@Override
				public String apply(String input) {
					return input.trim();
				}
			});
			
			if(Iterables.contains(transedEles, "")){
				throw new ReportRuntimeException("request [%s] has syntax error, have empty select element", sql);
			}

			if(Iterables.size(transedEles) == 0){
				throw new ReportRuntimeException("request [%s] has syntax error, must select one item", sql);
			}

			ImpalaCfgItem sqlMetrics = ImpalaCfg.getInstance().getDatasources().get(dataSource);
			StringBuilder builder = new StringBuilder();
			
			
			for(String ele : transedEles){
				String metric = sqlMetrics.getImpala().get(ele);
				builder.append(metric + " as " + ele + ",");
				metricMap.put(ele, metric);
			}
			
			builder.deleteCharAt(builder.length());
			
			metrics = builder.toString();
			
		}else{
			// impala druid均可
			System.out.println("have no join");
		}

		String transformedSql = "select " + metrics + " from " + dataSource + " " + rightStr;
		return new SqlContext(sql, isJoin, transformedSql, metricMap);
	}
}
