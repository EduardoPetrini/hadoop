/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.main;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;

import main.java.com.mestrado.mapred.map.Map1Spark2;
import main.java.com.mestrado.mapred.map.Map2Spark;
import main.java.com.mestrado.mapred.reduce.Reduce1Spark;
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
//	 public static String clusterUrl = "hdfs://master/";
//	 public static String clusterUrl = "hdfs://Lec21/";
	public static String sparkUrl = "";
//	 public static String sparkUrl = "spark://master:7077";
//	 public static String sparkUrl = "spark://Lec21:7077";
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
	public static List<String> outputDirsName;

	public MainSpark() {
		countDir = 0;
		timeTotal = 0;
		outputDirsName = new ArrayList<String>();
	}
	
//	public void print(List<Tuple2<String,Iterable<String>>> lista){
//		System.out.println("********************************* printing");
//		for(Tuple2<String,Iterable<String>> l: lista){
//			System.out.println(l._1+" -> "+l._2);
//		}
//		System.out.println("****************************** end printing");
//	}
//	
//	public void print2(List<Tuple2<String,String>> lista){
//		System.out.println("********************************* printing");
//		for(Tuple2<String,String> l: lista){
//			System.out.println(l._1+" -> "+l._2);
//		}
//		System.out.println("****************************** end printing");
//	}
//	
//	public void print3(List<Tuple2<Text,Text>> lista){
//		System.out.println("********************************* printing");
//		for(Tuple2<Text,Text> l: lista){
//			System.out.println(l._1+" -> "+l._2);
//		}
//		System.out.println("****************************** end printing");
//	}

	public void job1() {
		SparkConf conf = new SparkConf().setAppName("Imr fase 1").setMaster(sparkUrl);
		JavaSparkContext sc = new JavaSparkContext(conf);
		// sc.addJar("target/spark-imrApriori-1.0-jar-with-dependencies.jar");
		Broadcast<Double> broadSup = sc.broadcast(MainSpark.supportRate);
		StringBuilder log = new StringBuilder();
		long beginG = System.currentTimeMillis();
		
		JavaRDD<String> inputFile = sc.textFile(inputFileName, MainSpark.NUM_BLOCK);
//		inputFile.persist(StorageLevel.MEMORY_AND_DISK());
		
		JavaRDD<Tuple2<String, String>> mapedInter = inputFile.mapPartitionsWithIndex(new Map1Spark2(broadSup), true);
//		inputFile.unpersist();
//		mapedInter.persist(StorageLevel.MEMORY_AND_DISK());
		
		MainSpark.countDir++;

		ClassTag<String> tag = ClassManifestFactory$.MODULE$.fromClass(String.class);
		JavaPairRDD<String, String> maped = new JavaPairRDD<String, String>(mapedInter.rdd(), tag, tag);
//		mapedInter.unpersist();
		maped.persist(StorageLevel.MEMORY_AND_DISK());
		JavaPairRDD<String, Iterable<String>> grouped = maped.groupByKey(MainSpark.NUM_BLOCK);
//		maped.unpersist();
//		grouped.persist(StorageLevel.MEMORY_AND_DISK());
		//System.out.println("First map");
//		print(grouped.collect());

		List<Tuple2<String,Iterable<String>>> disRdd = grouped.filter(kv-> kv._1.startsWith("block")).collect();
		
		Integer[] disArray = new Integer[MainSpark.NUM_BLOCK+1];
		for(int i = 0; i < disRdd.size() ;i++){
			disArray[Integer.parseInt(disRdd.get(i)._1.replaceAll("[a-z]+", ""))] = Integer.parseInt(disRdd.get(i)._2.iterator().next());
		}
		JavaPairRDD<Text, Text> mapReduced = grouped.filter(kv-> !kv._1.startsWith("block")).mapToPair(new Reduce1Spark(supportRate, totalBlockCount, totalTransactionCount, blocksIds, disArray)).filter(t -> t != null);
//		grouped.unpersist();
//		mapReduced.persist(StorageLevel.MEMORY_AND_DISK());
		//System.out.println("First reduce");
//		print2(mapReduced.collect());
		JavaPairRDD<Text, Text> global = mapReduced.filter(t -> !t._1.toString().contains(":"));
		global.persist(StorageLevel.MEMORY_AND_DISK());
		JavaPairRDD<Text, Text> part = mapReduced.filter(t -> t._1.toString().contains(":"));
		mapReduced.unpersist();
		part.persist(StorageLevel.MEMORY_AND_DISK());
		
		//System.out.println("\n\n______________________print parts__________________\n\n");
		
		global.saveAsTextFile(MainSpark.outputDir + MainSpark.countDir);
		global.unpersist();

		outputDirsName.add(MainSpark.outputDir + MainSpark.countDir);
		MainSpark.countDir++;

		MainSpark.countDir++;
		//System.out.println("Print partitions");
		for (String b : blocksIds) {
			Broadcast<String> bb = sc.broadcast(b);
//			print2(part.filter(p -> p._1.substring(p._1.indexOf(":")).contains(bb.value())).collect());
			JavaPairRDD<Text,Text> tmp = part.filter(p -> p._1.toString().substring(p._1.toString().indexOf(":")).contains(bb.value()));
			if(tmp.count() > 0){
				tmp.saveAsHadoopFile(outputPartialName + b, Text.class,Text.class,SequenceFileOutputFormat.class);
			}
		}
//		part.unpersist();

		sc.stop();
		sc.close();
		long endG = System.currentTimeMillis();
		timeTotal += endG - beginG;
		log.append("Step 1 : " + (endG - beginG) + "ms " + (((double) (endG - beginG)) / 1000) + "s "+ (((double) (endG - beginG)) / 1000 / 60) + "m\n\n");
		timeLog.add(log.toString());
		System.out.println("END STEP ONE");
	}

	public void job2() {
		System.out.println("Gargabe collect...");
		System.gc();
		System.out.println("Gargabe collect...end");
		
		SparkConf conf = new SparkConf().setAppName("Imr fase 2").setMaster(sparkUrl);
		JavaSparkContext sc = new JavaSparkContext(conf);

		Broadcast<Double> broadSup = sc.broadcast(MainSpark.supportRate);

		StringBuilder log = new StringBuilder();
		
		long beginG = System.currentTimeMillis();
		JavaRDD<String> inputFile = sc.textFile(inputFileName, MainSpark.NUM_BLOCK);
		
//		inputFile.persist(StorageLevel.MEMORY_AND_DISK());
		List<String> partitionsDirs = SparkUtils.getPartitionsFase1Dirs();
		outputDirsName.addAll(partitionsDirs);
//		System.exit(0);
//		JavaPairRDD<String, String> partition = null;
//		List<Tuple2<String,String>> partitionList = new ArrayList<Tuple2<String,String>>();
//		JavaPairRDD<String, String> partitionAux = null;
//		List<Tuple2<String,String>>[] partitionsList = new List[partitionsDirs.size()];
//		int index = 0;
//		for (String p : partitionsDirs) {
//			partitionsList[index] =  sc.wholeTextFiles(p).filter(kv -> kv._2.length() > 4).collect();
//			index++;
////			if (partition == null) {
////				partition = sc.wholeTextFiles(p).filter(kv -> kv._2.length() > 4);
////				partitionList.addAll(sc.wholeTextFiles(p).filter(kv -> kv._2.length() > 4).collect());
////				partition.persist(StorageLevel.MEMORY_AND_DISK());
//////			} else {
////				partitionAux = sc.wholeTextFiles(p).filter(kv -> kv._2.length() > 4);
////				if (partitionAux != null) {
////					partition = partition.union(partitionAux);
////				}
////			}
//			
////			partition.foreach(t -> System.out.println("Partition " + t._1 + " content: \n" + t._2));
//		}

		// All step 1 partitions in "partition" RDD
//		System.out.println("Second partitions");
//		print2(partition.collect());
		JavaRDD<Tuple2<String, String>> mapedInter = inputFile.mapPartitionsWithIndex(new Map2Spark(partitionsDirs), true).filter(kv -> !kv._1.equals("#"));
		mapedInter.persist(StorageLevel.MEMORY_AND_DISK());
//		inputFile.unpersist();
//		partition.unpersist();

		
		
		ClassTag<String> tag = ClassManifestFactory$.MODULE$.fromClass(String.class);
		JavaPairRDD<String, String> maped = new JavaPairRDD<String, String>(mapedInter.rdd(), tag, tag);
//		mapedInter.unpersist();
//		maped.persist(StorageLevel.MEMORY_AND_DISK());
		JavaPairRDD<String, Iterable<String>> grouped = maped.groupByKey();
//		maped.unpersist();
//		grouped.persist(StorageLevel.MEMORY_AND_DISK());
//		System.out.println("Second map");

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
		});
		//System.out.println("Second reduce without fiter");
		
//		reduced.persist(StorageLevel.MEMORY_AND_DISK());
//		grouped.unpersist();
//		System.out.println("Second reduce with fiter");
		reduced.filter(kv -> Integer.parseInt(kv._2) >= broadSup.value()).saveAsTextFile(MainSpark.outputDir + MainSpark.countDir);
//		reduced.unpersist();

		outputDirsName.add(MainSpark.outputDir + MainSpark.countDir);
		MainSpark.countDir++;
		sc.stop();
		sc.close();
		
		long endG = System.currentTimeMillis();
		log.append("Step 2 : " + (endG - beginG) + "ms " + (((double) (endG - beginG)) / 1000.0) + "s "+ (((double) (endG - beginG)) / 1000.0/ 60.0) + "m\n\n");
		timeTotal += endG - beginG;
		timeLog.add(log.toString());
		
		System.out.println("END STEP TWO");
	}
	
	public static void showTotalTime(StringBuilder log){
		for (String l : timeLog) {
			System.out.println(l);
		}
		SimpleDateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		log.append("\n\n|*************************************************************************|\n");
		log.append("DATA=").append(format.format(new Date())).append("\n");
		log.append("Total time: "+timeTotal+"ms "+((double)timeTotal)/1000.0+"s "+((double)timeTotal)/1000.0/60.0+"m\n");
		log.append("TEMPO=").append(((double)timeTotal)/1000.0);
		log.append("\n\n|*************************************************************************|\n#\n");
		
		MrUtils.saveTimeLog(log.toString(),MainSpark.inputFileName.split("/"));
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
		StringBuilder log = new StringBuilder(CountItemsets.countItemsets(outputDirsName));
		showTotalTime(log);
	}
}
