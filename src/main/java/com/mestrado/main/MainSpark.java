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
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import main.java.com.mestrado.mapred.map.Map1Spark;
import main.java.com.mestrado.mapred.map.Map1SparkTest;
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
public class MainSpark implements Serializable{

	private static final long serialVersionUID = 1L;
	public static int countDir;
    private static int timeTotal;
    public static double supportRate = 0.005;
    public static String support;
    private int k = 1;
    public static int totalBlockCount;
    public static String inputEntry = "input/test";
    public static String inputFileName = "";
    public static String clusterUrl = "hdfs://master-home/";
    public static String sparkUrl = "spark://master-home:7077";
//    public static String sparkUrl = "yarn-cluster";
    public static String user = clusterUrl+"user/hdp/";
    public static String outputDir = user+"output-spark"; 
    public static long totalTransactionCount;
    public static ArrayList<String> blocksIds;
    public static String outputPartialName = user+"partitions-fase-1/partition";
    public static ArrayList<String> seqFilesNames;
    public static int NUM_BLOCK = 1;
    
    public MainSpark() {
        countDir = 0;
        timeTotal = 0;
    }
 
    /**
     * 
     */
    public void jobTest(){
        SparkConf conf = new SparkConf().setAppName("Imr fase 1").setMaster(sparkUrl);
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaPairRDD<String, String> inputFile = sc.wholeTextFiles(clusterUrl+user+inputEntry,2);
        JavaRDD<String> maped = inputFile.mapPartitionsWithIndex(new Map1SparkTest(), true);
        List<String> lista = maped.collect();
        
        for (String s: lista){
        	System.out.println(s);
        }
        sc.close();
    }
    
    public void job1(){
        SparkConf conf = new SparkConf().setAppName("Imr fase 1").setMaster(sparkUrl);
        JavaSparkContext sc = new JavaSparkContext(conf);
        //sc.addJar("target/spark-imrApriori-1.0-jar-with-dependencies.jar");
        Broadcast<Double> broadSup = sc.broadcast(MainSpark.supportRate);
        
        JavaRDD<String> inputFile = sc.textFile(user+inputEntry, MainSpark.NUM_BLOCK);
        inputFile.saveAsTextFile(MainSpark.outputDir+MainSpark.countDir);
        MainSpark.countDir++;
        
        JavaRDD<Tuple2<String,String>> mapedInter = inputFile.mapPartitionsWithIndex(new Map1Spark(broadSup), true);
        if(mapedInter.count() == 0){
        	SparkUtils.infoNoItemset();
        	System.exit(0);
        }
        mapedInter.saveAsTextFile(MainSpark.outputDir+MainSpark.countDir);
        MainSpark.countDir++;
        ClassTag<String> tag = ClassManifestFactory$.MODULE$.fromClass(String.class);
        JavaPairRDD<String, String> maped = new JavaPairRDD<String,String>(mapedInter.rdd(),tag,tag);
        JavaPairRDD<String,Iterable<String>> grouped = maped.groupByKey(MainSpark.NUM_BLOCK);
        JavaPairRDD<String,String> mapReduced = grouped.mapToPair(new PairFunction<Tuple2<String,Iterable<String>>, String, String>() {

			@Override
			public Tuple2<String, String> call(Tuple2<String, Iterable<String>> t) throws Exception {
				Tuple2<String,String> kv;
				StringBuilder sb = new StringBuilder();
				Iterator<String> i = t._2.iterator();
				int index = 1;
				while(i.hasNext()){
					sb.append("v"+index).append(" -> ").append(i.next()).append(" || ");
					index++;
				}
				System.out.println("K: "+t._1+" ::: "+sb.toString());
				kv = new Tuple2<String,String>(t._1,sb.toString());
				return kv;
			}
		});        
        
        mapReduced.saveAsTextFile(MainSpark.outputDir+MainSpark.countDir);
        MainSpark.countDir++;
        //reduceByKey(new Reduce1Spark(MainSpark.supportRate, MainSpark.totalBlockCount, MainSpark.totalTransactionCount, MainSpark.blocksIds));
        
        System.out.println("FIM");
        sc.close();
    }


    
    public static void main(String[] args) {
		MainSpark m = new MainSpark();
		
		SparkUtils.initialConfig(args);
		MrUtils.delOutDirs(user);
		SparkUtils.printConfigs();
		countDir++;
		m.job1();
	}
}
