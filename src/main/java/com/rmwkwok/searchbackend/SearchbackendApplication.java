package com.rmwkwok.searchbackend;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// https://start.spring.io/ - generated this template

@SpringBootApplication
public class SearchbackendApplication {

	static String indexFolder = "/home/raymondkwok/git/InformationRetrieval/index";
	static String w2vFolder = "/home/raymondkwok/git/InformationRetrieval/w2v";
	static int numSearchResult = 10;
	static int numExtraSearchResult = 10;
	static int minSnippetWords = 30*2;
	static int closeCoOccurrenceCondition = 4;
	static boolean doQueryExpansion = true;
	static boolean doQueryModification = true;

	public static void main(String[] args) {
		parseArgs(args);
		SpringApplication.run(SearchbackendApplication.class, args);
	}

	public static void parseArgs(String[] args) {
		for (String arg: args) {
			System.out.println("---------------------------------------------------------------");
			System.out.println(arg);
			// Option Arguments
			if (arg.startsWith("--") && arg.contains("=")) {
				String pKey = arg.substring(2, arg.indexOf("="));
				String pVal = arg.substring(arg.indexOf("=") + 1);
				switch (pKey) {
					case "w2vFolder": w2vFolder = pVal;
						break;
					case "indexFolder": indexFolder = pVal;
						break;
					case "numSearchResult": numSearchResult = Integer.parseInt(pVal);
						break;
					case "numExtraSearchResult": numExtraSearchResult = Integer.parseInt(pVal);
						break;
					case "minSnippetWords": minSnippetWords = Integer.parseInt(pVal);
						break;
					case "closeCoOccurrenceCondition": closeCoOccurrenceCondition = Integer.parseInt(pVal);
						break;
					case "doQueryExpansion": doQueryExpansion = Integer.parseInt(pVal) > 0;
						break;
					case "doQueryModification": doQueryModification = Integer.parseInt(pVal) > 0;
						break;
				}
			}
		}
	}
}
