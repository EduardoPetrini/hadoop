/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.main;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import main.java.com.mestrado.mapred.map.Map2Spark3;
import main.java.com.mestrado.utils.ComparatorUtils;
import main.java.com.mestrado.utils.ComparatorUtilsOne;
import main.java.com.mestrado.utils.CountItemsets;
import main.java.com.mestrado.utils.MrUtils;
import main.java.com.mestrado.utils.SparkUtils;
import scala.Tuple2;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

/**
 *
 * @author eduardo
 */
public class MainSpark implements Serializable {

	private static final long serialVersionUID = 1L;
	public static int countDir;
	private static int timeTotal;
	public static double supportRate = 0.005;
	public static String support;
	public static int totalBlockCount;
	public static String inputEntry = "input/";
	public static String inputFileName = "";
	public static String clusterUrl = "";
	// public static String clusterUrl = "hdfs://master/";
	// public static String clusterUrl = "hdfs://Lec21/";
	public static String sparkUrl = "";
	// public static String sparkUrl = "spark://master:7077";
	// public static String sparkUrl = "spark://Lec21:7077";
	// public static String sparkUrl = "yarn-client";
	public static String user = "";
	public static String outputDir = "";
	public static long totalTransactionCount;
	public static ArrayList<String> blocksIds;
	public static String outputPartialName = "";
	public static ArrayList<String> seqFilesNames;
	public static int NUM_BLOCK = 1;
	public static String globalFileName;
	public static String frePartitionsFileName;
	public static List<String> timeLog;
	public static List<String> outputDirsName;

	public MainSpark() {
		countDir = 0;
		timeTotal = 0;
		outputDirsName = new ArrayList<String>();
	}

	// public void print(List<Tuple2<String,Iterable<String>>> lista){
	// System.out.println("********************************* printing");
	// for(Tuple2<String,Iterable<String>> l: lista){
	// System.out.println(l._1+" -> "+l._2);
	// }
	// System.out.println("****************************** end printing");
	// }
	//
	// public void print2(List<Tuple2<String,String>> lista){
	// System.out.println("********************************* printing");
	// for(Tuple2<String,String> l: lista){
	// System.out.println(l._1+" -> "+l._2);
	// }
	// System.out.println("****************************** end printing");
	// }
	//
	// public void print3(List<Tuple2<Text,Text>> lista){
	// System.out.println("********************************* printing");
	// for(Tuple2<Text,Text> l: lista){
	// System.out.println(l._1+" -> "+l._2);
	// }
	// System.out.println("****************************** end printing");
	// }

//	public void printVector(List<String[]> vet){
//		for(String[] a: vet){
//			for(String b: a){
//				System.out.print(b+" ");
//			}
//			System.out.println("");
//		}
//	}
//	
//	public void printVectorString(List<String> vet){
//		for(String a: vet){
//			System.out.println(a);
//		}
//	}
	
	public void job1() {
		SparkConf conf = new SparkConf().setAppName("Spark DPC").setMaster(sparkUrl);
		JavaSparkContext sc = new JavaSparkContext(conf);
		// sc.addJar("target/spark-imrApriori-1.0-jar-with-dependencies.jar");
//		Broadcast<Double> broadSup = sc.broadcast(MainSpark.supportRate);
		Broadcast<Double> broadSupTotal = sc.broadcast(MainSpark.supportRate * MainSpark.totalTransactionCount);
		Accumulator<Integer> dirCount = sc.accumulator(1);
		StringBuilder log = new StringBuilder();

		long beginG = System.currentTimeMillis();

		JavaRDD<String> inputFile = sc.textFile(inputFileName, MainSpark.NUM_BLOCK);
		inputFile.cache();

		JavaRDD<String> oneItem = inputFile.flatMap(line -> new ArrayList<String>(Arrays.asList(line.split(" "))));
		JavaPairRDD<String, Integer> oneCounted = oneItem.mapToPair(item -> new Tuple2<String, Integer>(item, 1)).reduceByKey((v1, v2) -> v1 + v2)
				.filter(kv -> kv._2 >= broadSupTotal.getValue()).sortByKey(new ComparatorUtilsOne());
		oneCounted.saveAsTextFile(outputDir + dirCount.value());
		outputDirsName.add(outputDir + dirCount.value());
		dirCount.add(1);
		// Creating 2-itemsets
		long begin = System.currentTimeMillis();
		List<String[]> kItemsets = SparkUtils.creatTwoItemsets(oneCounted.keys().collect());

		JavaRDD<Tuple2<String, Integer>> kCounted = inputFile.mapPartitions(new Map2Spark3(kItemsets,dirCount.value()));
		ClassTag<String> tag = ClassManifestFactory$.MODULE$.fromClass(String.class);
		ClassTag<Integer> tagInt = ClassManifestFactory$.MODULE$.fromClass(Integer.class);
		JavaPairRDD<String, Integer> itemsetCounted = new JavaPairRDD<String, Integer>(kCounted.rdd(), tag, tagInt).reduceByKey((v1, v2) -> v1 + v2)
				.filter(kv -> kv._2 >= broadSupTotal.getValue()).sortByKey(new ComparatorUtils());
		itemsetCounted.saveAsTextFile(outputDir + dirCount.value());
		outputDirsName.add(outputDir + dirCount.value());
		dirCount.add(1);
		long lkSize;
		long end = System.currentTimeMillis();
		double timeByStep;
		int cSetSize;
		long ct;
		int minK, maxK;
		List<String[]> kItemsetsTmp = null;
		JavaRDD<Tuple2<String, Integer>> kCountedTmp;
		boolean cameFromBreak = false;
		// loop for k >= 3
		while (!cameFromBreak) {
			minK = dirCount.value();
			lkSize = itemsetCounted.count(); 
			timeByStep = ((double) end) - ((double) begin);
			begin = System.currentTimeMillis();
//			System.out.println("\n********************** 2: "+itemsetCounted.count());
			kItemsets = SparkUtils.createKItemsetsString(itemsetCounted.keys().collect(), dirCount.value());
//			Collections.sort(kItemsets,SparkUtils.ITEMSET_ORDER);
			dirCount.add(1);
			maxK = dirCount.value();
//			System.out.println("\n\n*********************** 3: "+ kItemsets.size()+"\n");
			cSetSize = kItemsets.size();
			if(cSetSize == 0) break;
			if (timeByStep >= 60) {
				ct = lkSize;
			} else {
				ct = (long) Math.round(lkSize * 1.2);
			}
			if(cSetSize > ct){
				dirCount.setValue(dirCount.value()-1);
				maxK = dirCount.value();
			}else{
				kItemsetsTmp = kItemsets;
			}
			 while( cSetSize <= ct){
				 kItemsetsTmp = SparkUtils.createKItemsets(kItemsetsTmp, dirCount.value());
				 if(kItemsetsTmp.size() == 0) {
					 dirCount.setValue(dirCount.value()-1);
					 maxK = dirCount.value();
					 cameFromBreak = true;
					 break;
				 }
//				 System.out.println("\n\n*********************** "+dirCount.value()+": "+ kItemsetsTmp.size()+"\n");
				 cSetSize += kItemsetsTmp.size();
				 kItemsets.addAll(kItemsetsTmp);
				 dirCount.add(1);
				 maxK = dirCount.value();
				 kItemsetsTmp = SparkUtils.createKItemsets(kItemsetsTmp, dirCount.value());
				 if(kItemsetsTmp.size() == 0) {
					 dirCount.setValue(dirCount.value()-1);
					 maxK = dirCount.value();
					 cameFromBreak = true;
					 break;
				 }
				 dirCount.add(1);
				 maxK = dirCount.value();
//				 System.out.println("\n\n*********************** "+dirCount.value()+": "+ kItemsetsTmp.size()+"\n");
				 cSetSize += kItemsetsTmp.size();
				 kItemsets.addAll(kItemsetsTmp);
				 if(cSetSize > ct){
						dirCount.setValue(dirCount.value()-1);
						maxK = dirCount.value();
					}
			 }
			 //itemsets created for this step
			 for(int i = minK; i < maxK; i++){
				 kCountedTmp = inputFile.mapPartitions(new Map2Spark3(kItemsets,i));
				 new JavaPairRDD<String, Integer>(kCountedTmp.rdd(), tag, tagInt).reduceByKey((v1, v2) -> v1 + v2)
							.filter(kv -> kv._2 >= broadSupTotal.getValue()).sortByKey(new ComparatorUtils()).saveAsTextFile(outputDir + i);
				 outputDirsName.add(outputDir + i);
			 }
			 kCounted = inputFile.mapPartitions(new Map2Spark3(kItemsets,maxK));
			 itemsetCounted = new JavaPairRDD<String, Integer>(kCounted.rdd(), tag, tagInt).reduceByKey((v1, v2) -> v1 + v2)
						.filter(kv -> kv._2 >= broadSupTotal.getValue()).sortByKey(new ComparatorUtils());
			 itemsetCounted.saveAsTextFile(outputDir + dirCount.value());
			 outputDirsName.add(outputDir + dirCount.value());
			 dirCount.add(1);
			 end = System.currentTimeMillis();
		}

		sc.stop();
		sc.close();
		long endG = System.currentTimeMillis();
		timeTotal += endG - beginG;
		log.append("Step 1 : " + (endG - beginG) + "ms " + (((double) (endG - beginG)) / 1000) + "s " + (((double) (endG - beginG)) / 1000 / 60) + "m\n\n");
		timeLog.add(log.toString());
		System.out.println("END STEP ONE");
	}

	public static void showTotalTime(StringBuilder log) {
		for (String l : timeLog) {
			System.out.println(l);
		}
		SimpleDateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		log.append("\n\n|*************************************************************************|\n");
		log.append("DATA=").append(format.format(new Date())).append("\n");
		log.append("Total time: " + timeTotal + "ms " + ((double) timeTotal) / 1000.0 + "s " + ((double) timeTotal) / 1000.0 / 60.0 + "m\n");
		log.append("TEMPO=").append(((double) timeTotal) / 1000.0);
		log.append("\n\n|*************************************************************************|\n#\n");

		MrUtils.saveTimeLog(log.toString(), MainSpark.inputFileName.split("/"));
	}

	public static void main(String[] args) {
		MainSpark m = new MainSpark();

		timeLog = new ArrayList<String>();
		SparkUtils.initialConfig(args);
		MrUtils.delOutDirs(user);
		SparkUtils.printConfigs();
		countDir++;
		m.job1();
		// m.job2();
		StringBuilder log = new StringBuilder(CountItemsets.countItemsets(outputDirsName));
		showTotalTime(log);
		// CountItemsets.printRealItemsets();
	}
}
