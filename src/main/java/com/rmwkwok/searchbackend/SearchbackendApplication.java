package com.rmwkwok.searchbackend;

import org.apache.commons.cli.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;

// https://start.spring.io/ - generated this template

@SpringBootApplication
public class SearchbackendApplication {

	static String hadoopIndexFolder = "";
	static String indexFolder = "";
	static String w2vFolder = "";
	static int numSearchResult = 10;
	static int minSnippetWords = 60;
	static int maxSnippetWords = 90;
	static int queryExpandLimit = 3;
	static int numExtraSearchResult = 20;
	static int closeCoOccurrenceCondition = 4;
	static boolean doQueryExpansion = true;
	static boolean doQueryModification = true;

	public static void main(String[] args) {
		HelpFormatter formatter = new HelpFormatter();
		CommandLineParser parser = new DefaultParser();

		Options options = new Options();
		options.addOption(Option.builder("h").longOpt("help").build());
		options.addOption(Option.builder().required().type(File.class).hasArg().longOpt("hadoopIndexFolder").argName("folder").desc("Hadoop index folder").build());
		options.addOption(Option.builder().required().type(File.class).hasArg().longOpt("indexFolder").argName("folder").desc("Lucene index folder").build());
		options.addOption(Option.builder().required().type(File.class).hasArg().longOpt("w2vFolder").argName("folder").desc("w2v index folder").build());
		options.addOption(Option.builder().type(Number.class).hasArg().longOpt("numSearchResult").argName(String.valueOf(numSearchResult)).desc("Number of search results to return").build());
		options.addOption(Option.builder().type(Number.class).hasArg().longOpt("numExtraSearchResult").argName(String.valueOf(numExtraSearchResult)).desc("Number of extra search results to consider before returning the top results").build());
		options.addOption(Option.builder().type(Number.class).hasArg().longOpt("minSnippetWords").argName(String.valueOf(minSnippetWords)).desc("Minimum number of words in considering the best snippet").build());
		options.addOption(Option.builder().type(Number.class).hasArg().longOpt("maxSnippetWords").argName(String.valueOf(maxSnippetWords)).desc("Maximum number of words in considering the best snippet").build());
		options.addOption(Option.builder().type(Number.class).hasArg().longOpt("closeCoOccurrenceCondition").argName(String.valueOf(closeCoOccurrenceCondition)).desc("How close in terms of number of words to consider as close co-occurrence").build());
		options.addOption(Option.builder().type(Number.class).hasArg().longOpt("queryExpandLimit").argName(String.valueOf(queryExpandLimit)).desc("How many new query word to expand for each query word").build());
		options.addOption(Option.builder().type(Number.class).hasArg().longOpt("doQueryExpansion").argName(doQueryExpansion ? "1" : "0").desc("Whether to do expand query with similar words or not. Positive integer to enable").build());
		options.addOption(Option.builder().type(Number.class).hasArg().longOpt("doQueryModification").argName(doQueryModification ? "1" : "0").desc("Whether to modify query word with document id or not. Positive integer to enable").build());

		try {
			CommandLine cmd = parser.parse(options, args);

			if(cmd.hasOption( "help" )) {
				formatter.printHelp("searcher", options);
				return;
			}

			w2vFolder = ((File)cmd.getParsedOptionValue("w2vFolder")).getAbsolutePath();
			indexFolder = ((File)cmd.getParsedOptionValue("indexFolder")).getAbsolutePath();
			hadoopIndexFolder = ((File)cmd.getParsedOptionValue("hadoopIndexFolder")).getAbsolutePath();

			if(cmd.hasOption( "numSearchResult" ))
				numSearchResult = ((Number)cmd.getParsedOptionValue("numSearchResult")).intValue();
			if(cmd.hasOption( "numExtraSearchResult" ))
				numExtraSearchResult = ((Number)cmd.getParsedOptionValue("numExtraSearchResult")).intValue();
			if(cmd.hasOption( "minSnippetWords" ))
				minSnippetWords = ((Number)cmd.getParsedOptionValue("minSnippetWords")).intValue();
			if(cmd.hasOption( "closeCoOccurrenceCondition" ))
				closeCoOccurrenceCondition = ((Number)cmd.getParsedOptionValue("closeCoOccurrenceCondition")).intValue();
			if(cmd.hasOption( "doQueryExpansion" ))
				doQueryExpansion = ((Number)cmd.getParsedOptionValue("doQueryExpansion")).intValue() > 0;
			if(cmd.hasOption( "doQueryModification" ))
				doQueryModification = ((Number)cmd.getParsedOptionValue("doQueryModification")).intValue() > 0;
		}
		catch( ParseException e ) {
			System.out.println(e.getMessage());
			formatter.printHelp( "searcher", options );
			return;
		}

		SpringApplication.run(SearchbackendApplication.class, args);
	}
}
