package main.java.com.mestrado.utils;

import java.util.ArrayList;

import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import main.java.com.mestrado.main.Main;
import main.java.com.mestrado.main.MainSpark;

public class SparkUtils {
	
	public static void initialConfig(String[] args){
		for(String s: args){
    		System.out.println("Args: "+s);
    	}
    	if(args.length != 0){
    		MainSpark.supportRate = Double.parseDouble(args[0]);
    		if(args.length == 2){
    			MainSpark.NUM_BLOCK = Integer.parseInt(args[1]);
    		}
    	}
    	
    	SparkConf conf = new SparkConf().setAppName("Initial Config").setMaster(MainSpark.sparkUrl);
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	JavaRDD<String> inputFile = sc.textFile(MainSpark.user+MainSpark.inputEntry, MainSpark.NUM_BLOCK);
    	MainSpark.totalTransactionCount = inputFile.count();
    	MainSpark.totalBlockCount = inputFile.partitions().size();
    	MainSpark.blocksIds = new ArrayList<String>();
    	for(Partition p : inputFile.partitions()){
    		MainSpark.blocksIds.add(String.valueOf(p.index()));
    	}
    	MainSpark.support = String.valueOf((MainSpark.totalTransactionCount*MainSpark.supportRate));
    	sc.close();
	}
	
	public static void printConfigs(){
    	System.out.println("\n******************************************************\n");
        System.out.println("IMRApriori");
        System.out.println("Arquivo de entrada: "+MainSpark.inputFileName);
    	System.out.println("Count: "+MainSpark.countDir);
    	System.out.println("Support rate: "+MainSpark.supportRate);
    	System.out.println("Support percentage: "+(MainSpark.supportRate*100)+"%");
    	System.out.println("Support min: "+MainSpark.support);
    	System.out.println("Total blocks/partitiions/Maps: "+MainSpark.totalBlockCount);
    	System.out.println("Total transactions: "+MainSpark.totalTransactionCount);
    	System.out.println("User dir: "+MainSpark.user);
    	System.out.println("User partition dir: "+MainSpark.outputPartialName);
    	System.out.println("Entry file: "+MainSpark.inputEntry);
    	System.out.println("Cluster url: "+MainSpark.clusterUrl);
    	for(String b: MainSpark.blocksIds){
    		System.out.println("Blocks id: "+b);
    	}
        System.out.println("Blocks: "+MainSpark.NUM_BLOCK);
    	System.out.println("\n******************************************************\n");
    }

    public static void infoNoItemset(){
        System.out.println("\n*******************\t*******************\t*******************\n");
        System.out.println("\tNenhum itemset gerado para os parametros de configuração.");
        System.out.println("\n*******************\t*******************\t*******************\n");
    }
}