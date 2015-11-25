/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.main;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;

import main.java.com.mestrado.app.SupPart;
import main.java.com.mestrado.mapred.map.Map1Spark2;
import main.java.com.mestrado.mapred.map.Map1Spark3;
import main.java.com.mestrado.mapred.map.Map2Spark;
import main.java.com.mestrado.mapred.map.Map2Spark2;
import main.java.com.mestrado.mapred.map.Map2Spark3;
import main.java.com.mestrado.mapred.map.Map3Spark;
import main.java.com.mestrado.mapred.reduce.Reduce1Spark;
import main.java.com.mestrado.mapred.reduce.Reduce1Spark2;
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
	private int k = 1;
	public static int totalBlockCount;
	public static String inputEntry = "input/";
	public static String inputFileName = "";
//	public static String clusterUrl = "hdfs://master-home/";
	public static String clusterUrl = "hdfs://master/";
//	public static String sparkUrl = "spark://master-home:7077";
	public static String sparkUrl = "spark://master:7077";
	// public static String sparkUrl = "yarn-client";
	public static String user = clusterUrl + "user/hdp/";
	public static String outputDir = user + "output-spark";
	public static long totalTransactionCount;
	public static ArrayList<String> blocksIds;
	public static String outputPartialName = user + "partitions-fase-1/partition";
	public static ArrayList<String> seqFilesNames;
	public static int NUM_BLOCK = 1;
	public static String globalFileName;
	public static String frePartitionsFileName;
	public static List<String> timeLog;

	public MainSpark() {
		countDir = 0;
		timeTotal = 0;
	}

	public void job1_1() {
		SparkConf conf = new SparkConf().setAppName("Imr Iterativo").setMaster(sparkUrl);
		JavaSparkContext sc = new JavaSparkContext(conf);
		// sc.addJar("target/spark-imrApriori-1.0-jar-with-dependencies.jar");
		Accumulator<Integer> kCount = sc.accumulator(1);
		StringBuilder log = new StringBuilder();
		List<String> outputFiles = new ArrayList<String>();
		long beginG = System.currentTimeMillis();

		JavaRDD<String> inputFile = sc.textFile(inputFileName, MainSpark.NUM_BLOCK);
		// inputFile.persist(StorageLevel.MEMORY_AND_DISK());
		inputFile.cache();
		System.out.println("\n\n*******************************\n\n Execution step "+kCount.value()+"\n\n********************************\n");
		JavaRDD<Tuple2<String, SupPart>> items1 = inputFile.mapPartitionsWithIndex(new Map1Spark3(MainSpark.supportRate), true);
//		items1.persist(StorageLevel.MEMORY_AND_DISK());

		// Para itemsets que não foram frequentes em todas a partições, estimar
		// o suporte global deles.
		// Se for maior que o suporte mínimo, ok, contar nas partições que não
		// foram frequentes.
		// Caso contrário, remover do RDD

		ClassTag<String> stringTag = ClassManifestFactory$.MODULE$.fromClass(String.class);
		ClassTag<SupPart> supPartTag = ClassManifestFactory$.MODULE$.fromClass(SupPart.class);
		JavaPairRDD<String, SupPart> maped = new JavaPairRDD<String, SupPart>(items1.rdd(), stringTag, supPartTag);
//		maped.persist(StorageLevel.MEMORY_AND_DISK());
		JavaPairRDD<String, Iterable<SupPart>> grouped = maped.groupByKey(MainSpark.NUM_BLOCK);
//		maped.unpersist();
//		grouped.persist(StorageLevel.MEMORY_AND_DISK());
		JavaPairRDD<String, Integer> mapReduced = grouped.mapToPair(new Reduce1Spark2(supportRate, totalBlockCount, totalTransactionCount, blocksIds)).filter(t -> t != null);
//		grouped.unpersist();
//		mapReduced.persist(StorageLevel.MEMORY_AND_DISK());
		JavaPairRDD<String, Integer> global = mapReduced.filter(t -> !t._1.contains(":"));
		global.persist(StorageLevel.MEMORY_AND_DISK());
		JavaPairRDD<String, Integer> part = mapReduced.filter(t -> t._1.contains(":"));
//		mapReduced.unpersist();
//		part.persist(StorageLevel.MEMORY_AND_DISK());

		// Global são os itemsets frequentes em todas as partições
		// Agora é preciso contar o RDD part nas partições utilizando o RDD
		// inputFile

		JavaRDD<Tuple2<String, Integer>> partCounted = inputFile.mapPartitionsWithIndex(new Map2Spark2(part.collect()), true).filter(kv -> !kv._1.equals("#"));
//		partCounted.persist(StorageLevel.MEMORY_AND_DISK());

		// Adiciona o RDD partCounted ao global

		ClassTag<Integer> integerTag = ClassManifestFactory$.MODULE$.fromClass(Integer.class);
		JavaPairRDD<String, Integer> partCountedPair = new JavaPairRDD<String, Integer>(partCounted.rdd(), stringTag, integerTag);
//		partCounted.unpersist();
//		partCountedPair.persist(StorageLevel.MEMORY_AND_DISK());

		global = global.union(partCountedPair);
//		partCountedPair.unpersist();

		// Global 1-itemsets finished, go to disk
		System.out.println("Save in " + MainSpark.outputDir + kCount.value());
		global.saveAsTextFile(MainSpark.outputDir + kCount.value());
		outputFiles.add(MainSpark.outputDir + kCount.value());

		// Create 2-itemsets by global RDD
		List<String> newItemsetsCandidates = SparkUtils.create2Itemsets(global.sortByKey().collect());
		kCount.add(1);
		System.out.println("\n\n*******************************\n\n Execution step "+kCount.value()+"\n\n********************************\n");
//		global.unpersist();

		// Count twoItemsets at the inputFile partition using a HashMap
		// twoItemsets can to be much big
		List<Tuple2<String, Integer>> lista;
		JavaPairRDD<String,Iterable<String>> prefixSufixRDD;
		JavaRDD<Tuple2<String, SupPart>> newItemsetsFrequents;
		boolean creatItemsets = true;
		// loop
		while (creatItemsets) {
			newItemsetsFrequents = inputFile.mapPartitionsWithIndex(new Map2Spark3(supportRate, newItemsetsCandidates), true).filter(kv -> !kv._1.equalsIgnoreCase("#"));
			newItemsetsFrequents.persist(StorageLevel.MEMORY_AND_DISK());
			if (newItemsetsFrequents.count() <= 1){
				System.out.println("A few frequents item sets size "+newItemsetsFrequents.count()+" k "+ kCount.value());
				newItemsetsFrequents.saveAsTextFile(MainSpark.outputDir + kCount.value());
				outputFiles.add(MainSpark.outputDir + kCount.value());
				break;
			}
//			List<Tuple2<String, SupPart>> l = newItemsetsFrequents.collect();
//			for (Tuple2<String, SupPart> t : l) {
//				 System.out.println(t._1+" partition/sup "+t._2.getPartitionId()+" / "+t._2.getSup());
//			}
			maped = new JavaPairRDD<String, SupPart>(newItemsetsFrequents.rdd(), stringTag, supPartTag);
//			newItemsetsFrequents.unpersist();
//			maped.persist(StorageLevel.MEMORY_AND_DISK());
			grouped = maped.groupByKey();
//			grouped.persist(StorageLevel.MEMORY_AND_DISK());
//			maped.unpersist();
			mapReduced = grouped.mapToPair(new Reduce1Spark2(supportRate, totalBlockCount, totalTransactionCount, blocksIds)).filter(t -> t != null);
//			if (mapReduced.count() <= 1)
//				break;
//			mapReduced.persist(StorageLevel.MEMORY_AND_DISK());
//			grouped.unpersist();
//			lista = mapReduced.collect();
//			System.out.println("Global and Parts");
//			for (Tuple2<String, Integer> t : lista) {
//				System.out.println("Key "+t._1+" value "+t._2);
//			}
			global = mapReduced.filter(t -> !t._1.contains(":"));
//			global.persist(StorageLevel.MEMORY_AND_DISK());
			part = mapReduced.filter(t -> t._1.contains(":"));
//			part.persist(StorageLevel.MEMORY_AND_DISK());
//			mapReduced.unpersist();
			if (part.count() > 0) {
				partCounted = inputFile.mapPartitionsWithIndex(new Map2Spark2(part.collect()), true).filter(kv -> !kv._1.equals("#"));
//				partCounted.persist(StorageLevel.MEMORY_AND_DISK());
				partCountedPair = new JavaPairRDD<String, Integer>(partCounted.rdd(), stringTag, integerTag);
//				partCounted.unpersist();
//				partCountedPair.persist(StorageLevel.MEMORY_AND_DISK());
//				lista = partCountedPair.collect();
//				System.out.println("Part counted to unio global");
//				for (Tuple2<String, Integer> t : lista) {
//					 System.out.println("Key "+t._1+" value "+t._2);
//				}
				global = global.union(partCountedPair);
//				partCountedPair.unpersist();
			}
			System.out.println("Save in " + MainSpark.outputDir +kCount.value());
			global.saveAsTextFile(MainSpark.outputDir + kCount.value());
			outputFiles.add(MainSpark.outputDir + kCount.value());
//			lista = global.collect();
//			System.out.println("\n\nItemsets Global Final...\n");
//			for (Tuple2<String, Integer> t : lista) {
//				System.out.println("Key " + t._1 + " value " + t._2);
//			}
			if(global.count() <= 1) break;
			kCount.add(1);
			Broadcast<Integer> kBroad = sc.broadcast(kCount.value());
			System.out.println("\n\n*******************************\n\n Execution step "+kCount.value()+"\n\n********************************\n");
			//create news item sets candidates in spark
			prefixSufixRDD = global.keys().mapToPair(key -> new Tuple2<String,String>(key.substring(0, key.lastIndexOf(" ")),key.split(" ")[kBroad.value()-2])).groupByKey();
			
//			List<Tuple2<String,Iterable<String>>> listSS = testRdd.collect();
//			for(Tuple2<String,Iterable<String>> t: listSS){
//				List<String> tmp = new ArrayList<String>((Collection<? extends String>) t._2);
////				Collections.addAll(t._2,tmp);
//				Collections.sort(tmp);
//				System.out.println("Prefix "+t._1+" suffix "+tmp);
//			}
//			System.exit(0);
			System.out.println("Global size on candidate gen "+global.count());
			newItemsetsCandidates = prefixSufixRDD.flatMap(new Map3Spark(global.collectAsMap())).collect();
			
//			newItemsetsCandidates = SparkUtils.createKItemsets(global.sortByKey().collect());
			System.out.println("News candidates vector size: "+newItemsetsCandidates.size());
//			for(String c: newItemsetsCandidates){
//				System.out.println(c);
//			}
		}
		long endG = System.currentTimeMillis();
		// end loop

		sc.stop();
		sc.close();
		
		System.out.println("\nFinished\n");
		String[] a = new String[outputFiles.size()];
//		SparkUtils.countItemsets(outputFiles.toArray(a));
		CountItemsets.countItemsets(outputFiles.toArray(a));
		log.append("\n|**********************************|\n\n");
		log.append("Total execution time: "+(endG - beginG)+"ms "+((double)(endG - beginG))/1000.0+"s "+((double)(endG - beginG))/1000.0/60.0+"s\n");
		log.append("\n|**********************************|\n\n");
		System.out.println(log.toString());
	}

	public void job1() {
		SparkConf conf = new SparkConf().setAppName("Imr fase 1").setMaster(sparkUrl);
		JavaSparkContext sc = new JavaSparkContext(conf);
		// sc.addJar("target/spark-imrApriori-1.0-jar-with-dependencies.jar");
		Broadcast<Double> broadSup = sc.broadcast(MainSpark.supportRate);
		StringBuilder log = new StringBuilder();
		long beginG = System.currentTimeMillis();

		JavaRDD<String> inputFile = sc.textFile(inputFileName, MainSpark.NUM_BLOCK);

		inputFile.persist(StorageLevel.MEMORY_AND_DISK());

		JavaRDD<Tuple2<String, String>> mapedInter = inputFile.mapPartitionsWithIndex(new Map1Spark2(broadSup), true);
		inputFile.unpersist();
		mapedInter.persist(StorageLevel.MEMORY_AND_DISK());

		MainSpark.countDir++;

		ClassTag<String> tag = ClassManifestFactory$.MODULE$.fromClass(String.class);
		JavaPairRDD<String, String> maped = new JavaPairRDD<String, String>(mapedInter.rdd(), tag, tag);
		mapedInter.unpersist();
		maped.persist(StorageLevel.MEMORY_AND_DISK());
		JavaPairRDD<String, Iterable<String>> grouped = maped.groupByKey(MainSpark.NUM_BLOCK);
		maped.unpersist();
		grouped.persist(StorageLevel.MEMORY_AND_DISK());

		JavaPairRDD<String, String> mapReduced = grouped.mapToPair(new Reduce1Spark(supportRate, totalBlockCount, totalTransactionCount, blocksIds)).filter(t -> t != null);
		grouped.unpersist();
		mapReduced.persist(StorageLevel.MEMORY_AND_DISK());
		JavaPairRDD<String, String> global = mapReduced.filter(t -> !t._1.contains(":"));
		global.persist(StorageLevel.MEMORY_AND_DISK());
		JavaPairRDD<String, String> part = mapReduced.filter(t -> t._1.contains(":"));
		mapReduced.unpersist();
		part.persist(StorageLevel.MEMORY_AND_DISK());

		global.saveAsTextFile(MainSpark.outputDir + MainSpark.countDir);
		global.unpersist();

		globalFileName = MainSpark.outputDir + MainSpark.countDir;
		MainSpark.countDir++;

		MainSpark.countDir++;

		for (String b : blocksIds) {
			Broadcast<String> bb = sc.broadcast(b);
			part.filter(p -> p._1.substring(p._1.indexOf(":")).contains(bb.value())).saveAsTextFile(outputPartialName + b);
		}
		part.unpersist();

		sc.stop();
		sc.close();
		long endG = System.currentTimeMillis();
		timeTotal += endG - beginG;
		log.append("Step 1 : " + (endG - beginG) + "ms " + (((double) (endG - beginG)) / 1000) + "s " + (((double) (endG - beginG)) / 1000 / 60) + "m\n\n");
		timeLog.add(log.toString());
		System.out.println("END STEP ONE");
	}

	public void job2() {
		SparkConf conf = new SparkConf().setAppName("Imr fase 2").setMaster(sparkUrl);
		JavaSparkContext sc = new JavaSparkContext(conf);

		Broadcast<Double> broadSup = sc.broadcast(MainSpark.supportRate);

		StringBuilder log = new StringBuilder();

		long beginG = System.currentTimeMillis();
		JavaRDD<String> inputFile = sc.textFile(inputFileName, MainSpark.NUM_BLOCK);
		inputFile.persist(StorageLevel.MEMORY_AND_DISK());
		List<String> partitionsDirs = SparkUtils.getPartitionsFase1Dirs();

		JavaPairRDD<String, String> partition = null;
		JavaPairRDD<String, String> partitionAux = null;

		for (String p : partitionsDirs) {
			if (partition == null) {
				partition = sc.wholeTextFiles(p).filter(kv -> kv._2.length() > 4);
			} else {
				partitionAux = sc.wholeTextFiles(p).filter(kv -> kv._2.length() > 4);
				if (partitionAux != null) {
					partition = partition.union(partitionAux);
				}
			}
			partition.persist(StorageLevel.MEMORY_AND_DISK());
			// partition.foreach(t -> System.out.println("Partition " + t._1 + "
			// content: \n" + t._2));
		}

		// All step 1 partitions in "partition" RDD
		JavaRDD<Tuple2<String, String>> mapedInter = inputFile.mapPartitionsWithIndex(new Map2Spark(partition.collect()), true).filter(kv -> !kv._1.equals("#"));
		inputFile.unpersist();
		partition.unpersist();

		mapedInter.persist(StorageLevel.MEMORY_AND_DISK());

		ClassTag<String> tag = ClassManifestFactory$.MODULE$.fromClass(String.class);
		JavaPairRDD<String, String> maped = new JavaPairRDD<String, String>(mapedInter.rdd(), tag, tag);
		mapedInter.unpersist();
		maped.persist(StorageLevel.MEMORY_AND_DISK());
		JavaPairRDD<String, Iterable<String>> grouped = maped.groupByKey();
		maped.unpersist();
		grouped.persist(StorageLevel.MEMORY_AND_DISK());

		JavaPairRDD<String, String> reduced = grouped.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, String>() {

			@Override
			public Tuple2<String, String> call(Tuple2<String, Iterable<String>> t) throws Exception {
				Tuple2<String, String> kv;
				Iterable<String> value = t._2;
				Integer sup_count = 0;
				sup_count = Integer.valueOf(value.iterator().next().split(":")[0]);
				for (String v : value) {
					sup_count++;
					// sup_count = Integer.valueOf(v.split(":")[1]);
				}

				kv = new Tuple2<String, String>(t._1, String.valueOf(sup_count));
				return kv;
			}
		}).filter(kv -> Integer.parseInt(kv._2) >= broadSup.value());
		reduced.persist(StorageLevel.MEMORY_AND_DISK());
		grouped.unpersist();

		reduced.saveAsTextFile(MainSpark.outputDir + MainSpark.countDir);
		reduced.unpersist();

		frePartitionsFileName = MainSpark.outputDir + MainSpark.countDir;
		MainSpark.countDir++;
		sc.stop();
		sc.close();

		long endG = System.currentTimeMillis();
		log.append("Step 2 : " + (endG - beginG) + "ms " + (((double) (endG - beginG)) / 1000.0) + "s " + (((double) (endG - beginG)) / 1000.0 / 60.0) + "m\n\n");
		timeTotal += endG - beginG;
		timeLog.add(log.toString());

		System.out.println("END STEP TWO");
	}

	public static void showTotalTime() {
		for (String l : timeLog) {
			System.out.println(l);
		}

		System.out.println("Total time: " + timeTotal + "ms " + ((double) timeTotal) / 1000 + "s " + ((double) timeTotal) / 1000 / 60 + "m");
	}

	public static void main(String[] args) {
		MainSpark m = new MainSpark();

		timeLog = new ArrayList<String>();
		SparkUtils.initialConfig(args);
		MrUtils.delOutDirs(user);
		SparkUtils.printConfigs();
		countDir++;
		m.job1_1();
		// m.job2();
//		SparkUtils.countItemsets(globalFileName, frePartitionsFileName);
		// SparkUtils.countItemsets("hdfs://master-home/user/hdp/output-spark2","hdfs://master-home/user/hdp/output-spark4");
		// showTotalTime();

	}
}
