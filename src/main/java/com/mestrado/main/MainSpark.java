/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.main;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;

import main.java.com.mestrado.mapred.map.Map1Spark;
import main.java.com.mestrado.mapred.map.Map1Spark2;
import main.java.com.mestrado.mapred.map.Map1SparkTest;
import main.java.com.mestrado.mapred.map.Map2Spark;
import main.java.com.mestrado.mapred.reduce.Reduce1Spark;
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

	/**
	 * 
	 */
	public void jobTest() {
		SparkConf conf = new SparkConf().setAppName("Imr fase 1").setMaster(sparkUrl);
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaPairRDD<String, String> inputFile = sc.wholeTextFiles(inputFileName, 2);
		JavaRDD<String> maped = inputFile.mapPartitionsWithIndex(new Map1SparkTest(), true);
		List<String> lista = maped.collect();

		for (String s : lista) {
			System.out.println(s);
		}
		sc.close();
	}

	public void job1() {
		SparkConf conf = new SparkConf().setAppName("Imr fase 1").setMaster(sparkUrl);
		JavaSparkContext sc = new JavaSparkContext(conf);
		// sc.addJar("target/spark-imrApriori-1.0-jar-with-dependencies.jar");
		Broadcast<Double> broadSup = sc.broadcast(MainSpark.supportRate);

		StringBuilder log = new StringBuilder();
		long beginG = System.currentTimeMillis();
		JavaPairRDD<String,String> inputFile = sc.wholeTextFiles(user + inputEntry, MainSpark.NUM_BLOCK);
		inputFile.persist(StorageLevel.MEMORY_AND_DISK());
		
		JavaRDD<String> mapedAux = inputFile.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

			@Override
			public Iterator<String> call(Integer v1, Iterator<String> v2) throws Exception {
				List<String> part = new ArrayList<String>();
				StringBuilder sb;
				while (v2.hasNext()) {
					sb = new StringBuilder();
					sb.append(v1).append(":").append(v2.next());
					part.add(sb.toString());
				}
				return part.iterator();
			}

		}, true);
		mapedAux.persist(StorageLevel.MEMORY_AND_DISK());
		
		JavaPairRDD<String, String> maped = mapedAux.mapPartitionsToPair(new Map1Spark(broadSup));
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
		log.append("Step 1 : " + (endG - beginG) + "ms " + (((double) (endG - beginG)) / 1000) + "s "+ (((double) (endG - beginG)) / 1000 / 60) + "m\n\n");
		timeLog.add(log.toString());
		System.out.println("END STEP ONE");


	}

	public void job11() {
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
		log.append("Step 1 : " + (endG - beginG) + "ms " + (((double) (endG - beginG)) / 1000) + "s "+ (((double) (endG - beginG)) / 1000 / 60) + "m\n\n");
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
			partition.foreach(t -> System.out.println("Partition " + t._1 + " content: \n" + t._2));
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
		log.append("Step 2 : " + (endG - beginG) + "ms " + (((double) (endG - beginG)) / 1000.0) + "s "+ (((double) (endG - beginG)) / 1000.0/ 60.0) + "m\n\n");
		timeTotal += endG - beginG;
		timeLog.add(log.toString());
		
		System.out.println("END STEP TWO");
	}
	
	public static void showTotalTime(){
		for (String l : timeLog) {
			System.out.println(l);
		}
		
		System.out.println("Total time: "+timeTotal+"ms "+((double)timeTotal)/1000+"s "+((double)timeTotal)/1000/60+"m");
	}

	public static void main(String[] args) {
		MainSpark m = new MainSpark();

		timeLog = new ArrayList<String>();
		SparkUtils.initialConfig(args);
		MrUtils.delOutDirs(user);
		SparkUtils.printConfigs();
		countDir++;
		m.job1();
		m.job2();
		SparkUtils.countItemsets(globalFileName, frePartitionsFileName);
		// SparkUtils.countItemsets("hdfs://master-home/user/hdp/output-spark2","hdfs://master-home/user/hdp/output-spark4");
		showTotalTime();
		
	}
}
