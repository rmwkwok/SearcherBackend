package com.rmwkwok.searchbackend;

import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// https://start.spring.io/ - generated this template

@SpringBootApplication
public class SearchbackendApplication {

	static String indexPath = "/home/raymondkwok/git/InformationRetrieval/index";
	static int numSearchResult = 10;
	static int minSnippetWords = 30*2;
	static int closeCoOccurrenceCondition = 4;

	public static void main(String[] args) throws Exception {
		parseArgs(args);
		SpringApplication.run(SearchbackendApplication.class, args);
	}

	public static void parseArgs(String[] args) throws Exception {
		for (String arg: args) {
			// Option Arguments
			if (arg.startsWith("--") && arg.contains("=")) {
				String pKey = arg.substring(2, arg.indexOf("="));
				String pVal = arg.substring(arg.indexOf("=") + 1);
				switch (pKey) {
					case "indexPath": indexPath = pVal;
						break;
					case "numSearchResult": numSearchResult = Integer.parseInt(pVal);
						break;
					case "minSnippetWords": minSnippetWords = Integer.parseInt(pVal);
						break;
					case "closeCoOccurrenceCondition": closeCoOccurrenceCondition = Integer.parseInt(pVal);
						break;
				}
			}
		}
	}
}
