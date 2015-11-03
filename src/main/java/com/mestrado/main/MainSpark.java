/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.main;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import main.java.com.mestrado.mapred.map.Map1Spark;
import main.java.com.mestrado.mapred.map.Map1SparkTest;
import main.java.com.mestrado.utils.MrUtils;
import main.java.com.mestrado.utils.SparkUtils;
import scala.Tuple2;

/**
 *
 * @author eduardo
 */
public class MainSpark implements Serializable{

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static int countDir;
    private static int timeTotal;
    public static double supportRate = 0.005;
    public static String support;
    private int k = 1;
    public static int totalBlockCount;
    public static String inputEntry = "input/";
    public static String inputFileName = "";
    public static String clusterUrl = "hdfs://master-home/";
    public static String sparkUrl = "spark://master-home:7077";
    public static String user = clusterUrl+"user/hdp/";
    public static String outputDir = user+"output-spark"; 
//    public static String sparkUrl = "local[2]";
    public static long totalTransactionCount;
    public static ArrayList<String> blocksIds;
    public static String outputPartialName = user+"partitions-fase-1/partition";
    public static ArrayList<String> seqFilesNames;
    public static int NUM_REDUCES = 1;
    public static String NUM_BLOCK = "0";
    
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
        JavaRDD<String> maped = inputFile.mapPartitionsWithIndex(new Map1SparkTest(), false);
        List<String> lista = maped.collect();
        
        for (String s: lista){
        	System.out.println(s);
        }
        sc.close();
    }
    
    public void job1(){
        SparkConf conf = new SparkConf().setAppName("Imr fase 1").setMaster(sparkUrl);
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.addJar("target/spark-imrApriori-1.0-jar-with-dependencies.jar");
        Broadcast<Double> broadSup = sc.broadcast(MainSpark.supportRate);
        
        JavaRDD<String> inputFile = sc.textFile(user+inputEntry,2);
        
        JavaRDD<String> mapedInter = inputFile.mapPartitionsWithIndex(new Map1Spark(broadSup), false);
        mapedInter.saveAsTextFile(MainSpark.outputDir+MainSpark.countDir);
        //cada string em maped é key#value, separar em 2 strings
        JavaPairRDD<String,String> maped = mapedInter.mapToPair( keyVal -> new Tuple2<String,String>(keyVal.split("#")[0],keyVal.split("#")[1]));
        MainSpark.countDir++;
        maped.saveAsTextFile(MainSpark.outputDir+MainSpark.countDir);
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
