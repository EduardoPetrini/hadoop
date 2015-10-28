/*
 * thiago
 */

package main.java.com.mestrado.main;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Iterator;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.mortbay.jetty.security.HTAccessHandler;

import main.java.com.mestrado.app.HashNode;
import main.java.com.mestrado.app.HashPrefixTree;
import main.java.com.mestrado.utils.AprioriUtils;
import main.java.com.mestrado.utils.CountItemsets;
import main.java.com.mestrado.utils.MrUtils;
import scala.Tuple2;
import scala.collection.Map;
import scala.collection.Seq;

public class MainSpark implements Serializable {
	
	public static int countDir;
    private static int timeTotal;
    public static double supportPercentage = 0.01;
    public static double support;
    public static int k = 1;
    public static String user = "/user/thiago/";
    public static String inputEntry = "input/";
    public static String inputFileName = "";
    public static String clusterUrl = "hdfs://localhost:9000"; //-> set in core_site.xml for spark
    public static String outputCandidates = user + "outputCandidates/C";
    public static String inputCandidates = user + "inputCandidates/C";
    public static String inputCandidatesDir = user + "inputCandidates";
    public static String inputFileToGen = user + "inputToGen/input";
    public static long totalTransactionCount;
    public static ArrayList<String> candFilesNames;
    public String masterUrl = "local[8]";
    public static int num_parts = 1;
    
    private Log log = LogFactory.getLog(MainSpark.class);

	public MainSpark() {
		countDir = 0;
        timeTotal = 0;        
        setCluster();
	}
	
	public void setLocal() {
		masterUrl = "local[8]";
		user = "/user/thiago/";
		clusterUrl = "hdfs://localhost:9000";
	}
	
	public void setCluster() {
		masterUrl = "spark://master:7077";
		clusterUrl = "hdfs://master:8020";
		user = "/user/hdp/";
	}
	
	public static void endTime(){
    	StringBuilder sb = new StringBuilder();
    	sb.append("AprioriCpa - support ").append(supportPercentage).append(", transactions ").append(totalTransactionCount).append(" -- ").append(new Date()).append("\n");
    	sb.append("Arquivo ").append(inputFileName).append("\n\t");
        sb.append("Quantidade de itemsets gerados: \n\t");
    	sb.append(CountItemsets.countItemsets());
    	sb.append("\n-----------\n");
        MrUtils.saveTimeLog(sb.toString());
    }
	
	public static boolean checkOutputSequence(){
    	if(!MrUtils.checkOutputMR()){
        	System.out.println("Arquivo gerado na fase " + countDir + " é vazio!!!\n");
//    		endTime();
//    		System.exit(0);
    		return false;
        }
    	return true;
    }
	
	public void job1() {
		SparkConf conf = new SparkConf().setAppName("AprioriCpa Fase 1").setMaster(masterUrl);		
		JavaSparkContext sc = new JavaSparkContext(conf);

		Broadcast<Double> broadcastSupport = sc.broadcast(MainSpark.support);
		Broadcast<Integer> broadcastNumParts = sc.broadcast(MainSpark.num_parts);
		
		JavaRDD<String> lines = sc.textFile(clusterUrl + user + inputEntry + inputFileName);
				
		/* Map1.java */
		
		JavaPairRDD<String, Integer> words = lines.flatMap(line -> Arrays.asList(line.split(" ")))
				.mapToPair(w -> new Tuple2<String, Integer>(w, 1));
		
		/* Fim Map1.java */
		
		/* Reduce1.java */

		JavaPairRDD<String, Integer> result = words.reduceByKey((x, y) -> x + y, broadcastNumParts.value()).filter(w -> w._2 >= broadcastSupport.value()).mapToPair(w -> new Tuple2<String, Integer>(w._1, w._2));							
		
		result.saveAsTextFile(clusterUrl + user + "output" + countDir);

		/* Fim Reduce1.java */
		
		sc.close();
	}
		
	public void jobCount(){
		SparkConf conf = new SparkConf().setAppName("AprioriCpa Contagem").setMaster(masterUrl);
		
        for(int i = 0; i < candFilesNames.size(); i++) {
        	conf.set("inputCandidates" + i, candFilesNames.get(i)); //Contem Ck
        }
        
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        /*for(int i = 0; i < candFilesNames.size(); i++) {
        	//Currently MEMORY_ONLY
        	//sc.textFile(candFilesNames.get(i)).cache();
        	//sc.textFile(candFilesNames.get(i)).persist(StorageLevel.MEMORY_AND_DISK());        	
        }*/
        
        Broadcast<Double> broadcastSupport = sc.broadcast(MainSpark.support);
		Broadcast<Integer> broadcastCount = sc.broadcast(MainSpark.countDir);
		Broadcast<Integer> broadcastK = sc.broadcast(MainSpark.k);
		Broadcast<Integer> broadcastCandSize = sc.broadcast(MainSpark.candFilesNames.size());
		Broadcast<Integer> broadcastNumParts = sc.broadcast(MainSpark.num_parts);
        
		/* Map2.java */
		JavaRDD<String> lines = sc.textFile(clusterUrl + user + inputEntry + inputFileName);

		/* Map2.java setup() */
		int candSize = broadcastCandSize.value();
		String[] filesName = new String[candSize];
    	
    	log.info("AprioriCpa Map contagem de C" + k);
    	log.info("Arquivo de entrada no inputCandidates: ");

    	List<String> keko = new ArrayList<String>();
    	JavaRDD<String> fullList = null;
    	
    	for(int i = 0; i < candSize; i++) {
    		filesName[i] = sc.getConf().get("inputCandidates" + i);
        	    		
    		JavaPairRDD<Text, Integer> pairs = sc.sequenceFile(clusterUrl + filesName[i], Text.class, Integer.class);
    		
    		List<String[]> kek = pairs.map(x -> x._1.toString().split(" ")).collect();
    		
    		HashPrefixTree hpt = new HashPrefixTree();
    		
    		for (int j = 0; j < kek.size(); j++) {
    			hpt.add(hpt.getHashNode(), kek.get(j), 0);
    		}    		
    		
    		fullList = lines.mapPartitions(
    				new FlatMapFunction<Iterator<String>, String> () {
    					public Iterable<String> call(Iterator<String> it)  {
    						
    						List<String> result = new ArrayList<String>();
    						HashPrefixTree hpt_ = hpt;
    						int k = broadcastK.getValue();
    						
    						while (it.hasNext()) {
    							String t = it.next();
    							
    							String[] transactions = t.split(" ");
    							
    							String[] itemset;
    							List<String> keysOut = new ArrayList<String>();
    							
    							for (int j = 0; j < transactions.length; j++) {							
    								itemset = new String[k];
    								subSet(keysOut, transactions, hpt_.getHashNode(), j, itemset, 0, k);
    							}
    							
    							result = ListUtils.union(result, keysOut);    							
    						}
    						
							return result;
    					}
    				}
    		);    		
    	}
		
		JavaPairRDD<String, Integer> result = fullList.mapToPair(w -> new Tuple2<String, Integer>(w, 1)).reduceByKey((x, y) -> x + y, broadcastNumParts.value());    		  
    	
    	result = result.filter(f -> f._2 >= broadcastSupport.value());

		result.saveAsTextFile(clusterUrl + user + "output" + broadcastCount.value());	
    	
        /* Map2.java */
        sc.close();
    }
	
	/**
     * 
     * @param transactions
     * @param hNode
     * @param i
     * @param k
     * @param itemset
     * @param itemsetIndex
     * @param context
	 * @return 
     */
    private void subSet(List<String> keysOut, String[] transactions, HashNode hNode, int i,
			String[] itemset, int itemsetIndex, int k) {

    	if(i >= transactions.length){
			return;
		}
    	
		String keyOut;
		HashNode son = hNode.getHashNode().get(transactions[i]);
		
		if(son == null){
			return;
		} else {
			itemset[itemsetIndex] = transactions[i];
			
			if(hNode.getLevel() == k - 1){
				StringBuilder sb = new StringBuilder();
				for(String item : itemset){
					if(item != null){
						sb.append(item).append(" ");
					}
				}
				// System.out.println("Encontrou: "+sb.toString().trim());
				keyOut = sb.toString().trim();
				itemset[itemsetIndex] = "";
				keysOut.add(keyOut);
			}
			
			i++;
			itemsetIndex++;
			while(i < transactions.length) {
				subSet(keysOut, transactions, son, i, itemset, itemsetIndex, k);
				for(int j = itemsetIndex; j < itemset.length; j++){
					itemset[j] = "";
				}
				i++;
			}
		}
		return;
	}
    
    public static boolean checkCountOutput(){
    	if(!MrUtils.checkOutput(user + "output" + MainSpark.countDir)){
        	System.out.println("Arquivo gerado na fase " + countDir + " é vazio!!!\n");
//    		endTime();
//    		System.exit(0);
    		return false;
        }
    	return true;
    }
    
    public static void copyToInputGen(){
    	MrUtils.copyToInputGen(user + "output" + (MainSpark.countDir-1));
    }
    
    private boolean allSubsetIsFrequent(String[] itemset, HashSet<String> freqItemsets){
    	int indexToSkip = 0;
		StringBuilder subItem;
		for(int j = 0; j < itemset.length-1; j++){
			subItem = new StringBuilder();
			for(int i = 0; i < itemset.length; i++){
				if(i != indexToSkip){
					subItem.append(itemset[i]).append(" ");
				}
			}
			//subItem gerado, verificar se é do conjunto frequente
			
			if(!freqItemsets.contains(subItem.toString().trim())){
				return false;
			}
			indexToSkip++;
		}
		
		return true;
    }
    
    public void jobGen() throws IOException {
    	SparkConf conf = new SparkConf().setAppName("AprioriCpa Geracao").setMaster(masterUrl);
    	
    	conf.set("inputCandidates", inputCandidates + MainSpark.countDir);
    	conf.set("inputFileToGen", inputFileToGen);
    	
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	    	
    	System.out.println("AprioriCpa geração de candidatos - CountDir: " + MainSpark.countDir);

    	JavaRDD<String> lk = sc.textFile(clusterUrl + MainSpark.inputFileToGen);
    	List<String> kekk = lk.collect();
    	
    	HashSet<String> freqItemsets = new HashSet<String>();
    	
    	for (int i = 0; i < kekk.size(); i++) {
    		String s = kekk.get(i);
    		s = s.replace("(", "").replace(")", "");
    		freqItemsets.add(s.split(",")[0].trim());
    	}
    	
    	/* GenMap.java */
    
    	JavaPairRDD<String, String> gen = lk.mapToPair(
		new PairFunction<String, String, String> () {
			public Tuple2<String, String> call(String t) {
				
				t = t.replace("(", "").replace(")", "");
				
				String[] tokens = t.split(",")[0].split(" ");
				
		        StringBuilder sb = new StringBuilder();
		        for(int i = 0; i < tokens.length - 1; i++){
		        	sb.append(tokens[i]).append(" ");
		        }
		        return new Tuple2<String, String>(sb.toString().trim(), tokens[tokens.length - 1]);
			}
		}
		);
    	
    	/* GenReduce.java */
    	
    	JavaPairRDD<String, Iterable<String>> lel = gen.groupByKey();

    	JavaPairRDD<String, Integer> newK2 = lel.mapPartitionsToPair(
				new PairFlatMapFunction<Iterator<Tuple2<String, Iterable<String>>>, String, Integer> () {
					public Iterable<Tuple2<String, Integer>> call(Iterator<Tuple2<String, Iterable<String>>> it)  {
						
						List<Tuple2<String, Integer>> ret = new ArrayList<Tuple2<String, Integer>>();
						String newItemset;
						HashSet<String> freqItemsets_ = freqItemsets; 
						
						while (it.hasNext()) {
							Tuple2<String, Iterable<String>> t = it.next();
							
							java.util.Iterator<String> it_s = t._2.iterator();
							ArrayList<String> suffix = new ArrayList<String>();
							
							while (it_s.hasNext()) {
								suffix.add(it_s.next().toString());
							}
							
							Collections.sort(suffix, NUMERIC_ORDER);
							String prefix;
							int count = 0;
							
							for(int i = 0; i < suffix.size() - 1; i++) {
								prefix = t._1 + " " + suffix.get(i) + " ";
								for(int j = i + 1; j < suffix.size(); j++) {
									newItemset = prefix + suffix.get(j);
									if(allSubsetIsFrequent(newItemset.split(" "), freqItemsets_)){
										count++;
										Tuple2<String, Integer> t2 = new Tuple2<String, Integer>(newItemset, count);
										ret.add(t2);
					    			}
								}
							}
						}
						return ret;
					}
				});

        String outputCand = inputCandidates + MainSpark.countDir + "-" + String.valueOf(System.currentTimeMillis());

        //Can make this better
    	List<Tuple2<String, Integer>> kek = newK2.collect();
    	
    	sc.close();
    	
    	ArrayList<String> killme = new ArrayList<String>();
    	for (Tuple2<String, Integer> t : kek) {
    		killme.add(t._1);
    	}   
    	
    	MrUtils.saveSequenceInHDFS(killme, outputCand);
    	
    	//JavaPairRDD<String, String> result = newK2.mapToPair(x -> new Tuple2<String, String>(x._2 + " candidatos em ", x._1));
    	//result.saveAsTextFile(clusterUrl + user + "candidatosTxt" + MainSpark.countDir);
    }
    
    public static Comparator<String> NUMERIC_ORDER = new Comparator<String>() {
    	public int compare(String obj1, String obj2) {
    		
    		int o1 = Integer.parseInt(obj1.trim());
    		int o2 = Integer.parseInt(obj2.trim());
    		
			if (o1 < o2) {
				return -1;
			} else if (o1 > o2) {
				return 1;
			}
    		return 0;
    	}
	};
	
	public static void Finish() {
		CountItemsets.countItemsets();
	}
	
	public static void main(String[] args) throws Exception {
		MainSpark m = new MainSpark();
		
		long startTime = System.currentTimeMillis();
		
		MrUtils.delOutDirs(user);
		
        MrUtils.initialConfig(args);
        
        MainSpark.countDir++;//1
        MrUtils.printConfigs(m);
        
        //Main.k == 1
        try {
        	m.job1();
        } catch (Exception ex) {
            System.out.println("EXCEPTION: " + ex.getMessage());
        }
        
        //check output dir
        if (!MrUtils.checkOutput(user + "output" + MainSpark.countDir)) {
        	//endTime();
        	System.exit(0);
        }
        
        long time2k = 0;
        if ((time2k = AprioriUtils.generate2ItemsetCandidates()) == -1) {
        	//endTime();
        	System.exit(0);
        }
        
        timeTotal += time2k;
        
        MainSpark.k++; //Main.k == 2;
        MainSpark.countDir++; //2
        
        /* Iniciar proxima fase */

        do {
        	m.jobCount();//Contar a corrência de Ck na base. Le Ck e salva Lk
        	if(!checkCountOutput()){
	        	break;
	        }
        	
        	//copia Lk do output do jobCount para o input do jobGen
	        MainSpark.k++; //Main.k == 3;
	        MainSpark.countDir++;//3
	        copyToInputGen();
	        //gera lk+1
	        m.jobGen();
	        
        } while(checkOutputSequence());
        
        long endTime = System.currentTimeMillis();
        System.out.println("Terminou em " + (endTime - startTime) / 1000 + " segundos");
        
        Finish();
	}

}