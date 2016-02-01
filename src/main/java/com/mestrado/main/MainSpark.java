/*
 * Thiago Diniz Maia
 * dinizthiagobr@gmail.com
 */

package main.java.com.mestrado.main;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import main.java.com.mestrado.app.HashNode;
import main.java.com.mestrado.utils.CountItemsets;
import main.java.com.mestrado.utils.MrUtils;
import main.java.com.mestrado.utils.SerializableComparator;
import scala.Tuple2;

public class MainSpark implements Serializable {

	public static int countDir;
	private static int timeTotal;
	public static double supportPercentage = 0.01;
	public static double support;
	public static int k;
	public static String user = "/user/hdp/";
	public static String inputEntry = "input/";
	public static String inputFileName = "";
	public static String clusterUrl = "hdfs://localhost:9000"; // -> set in
																// core_site.xml
																// for spark
	public static String outputCandidates = user + "outputCandidates/C";
	public static String inputCandidates = user + "inputCandidates/C";
	public static String inputCandidatesDir = user + "inputCandidates";
	public static String inputFileToGen = user + "inputToGen/input";
	public static long totalTransactionCount;
	public static ArrayList<String> candFilesNames;
	public String masterUrl = "local[8]";
	public static int num_parts = 1;
	public static int num_blocks = 1;
	public static String durationLogName = "";
	public static ArrayList<Long> durations;
	public static int numExecution = 1;

	private Log log = LogFactory.getLog(MainSpark.class);

	public MainSpark() {
		countDir = 0;
		timeTotal = 0;
		setCluster();

		durations = new ArrayList<Long>();			
	}

	public void setLocal() {
		masterUrl = "local[8]";
		user = "/user/thiago/";
		clusterUrl = "hdfs://localhost:9000";
	}

	public void setCluster() {
		masterUrl = "spark://lec21:7077";
		clusterUrl = "hdfs://lec21:8020";
		user = "/user/hdp/";
	}
	
	public void setCluster3() {
		masterUrl = "spark://master-home:7077";
		clusterUrl = "hdfs://master-home:8020";
		user = "/user/hdp/";
	}

	public void setCluster2() {
		masterUrl = "spark://master:7077";
		clusterUrl = "hdfs://master:8020";
		user = "/user/hdp/";
	}

	public static void endTime() {
		StringBuilder sb = new StringBuilder();
		sb.append("AprioriCpa - support ").append(supportPercentage).append(", transactions ")
				.append(totalTransactionCount).append(" -- ").append(new Date()).append("\n");
		sb.append("Arquivo ").append(inputFileName).append("\n\t");
		sb.append("Quantidade de itemsets gerados: \n\t");
		sb.append(CountItemsets.countItemsets());
		sb.append("\n-----------\n");
		MrUtils.saveTimeLog(sb.toString());
	}

	public static boolean checkOutputSequence() {
		if (!MrUtils.checkOutputMR()) {
			System.out.println("Arquivo gerado na fase " + countDir + " é vazio!!!\n");
			// endTime();
			// System.exit(0);
			return false;
		}
		return true;
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
	private void subSet(List<String> keysOut, String[] transactions, HashNode hNode, int i, String[] itemset,
			int itemsetIndex, int k) {

		if (i >= transactions.length) {
			return;
		}

		String keyOut;
		HashNode son = hNode.getHashNode().get(transactions[i]);

		if (son == null) {
			return;
		} else {
			itemset[itemsetIndex] = transactions[i];

			if (hNode.getLevel() == k - 1) {
				StringBuilder sb = new StringBuilder();
				for (String item : itemset) {
					if (item != null) {
						sb.append(item).append(" ");
					}
				}
				//System.out.println("Encontrou: " + sb.toString().trim());
				keyOut = sb.toString().trim();
				itemset[itemsetIndex] = "";
				keysOut.add(keyOut);
			}

			i++;
			itemsetIndex++;
			while (i < transactions.length) {
				subSet(keysOut, transactions, son, i, itemset, itemsetIndex, k);
				for (int j = itemsetIndex; j < itemset.length; j++) {
					itemset[j] = "";
				}
				i++;
			}
		}
		return;
	}

	public static boolean checkCountOutput() {
		if (!MrUtils.checkOutput(user + "output" + MainSpark.countDir)) {
			System.out.println("Arquivo gerado na fase " + countDir + " é vazio!!!\n");
			// endTime();
			// System.exit(0);
			return false;
		}
		return true;
	}

	public static void copyToInputGen() {
		MrUtils.copyToInputGen(user + "output" + (MainSpark.countDir - 1));
	}

	private boolean allSubsetIsFrequent(String[] itemset, HashSet<String> freqItemsets) {
		int indexToSkip = 0;
		StringBuilder subItem;
		for (int j = 0; j < itemset.length - 1; j++) {
			subItem = new StringBuilder();
			for (int i = 0; i < itemset.length; i++) {
				if (i != indexToSkip) {
					subItem.append(itemset[i]).append(" ");
				}
			}
			// subItem gerado, verificar se é do conjunto frequente

			if (!freqItemsets.contains(subItem.toString().trim())) {
				return false;
			}
			indexToSkip++;
		}

		return true;
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
		MrUtils.appendToFile(MainSpark.durationLogName, "\n\n*************************************\n\n");
		CountItemsets.countSparkItemsets();
	}

	public void runItAll() {
//		ArrayList<String> currentItemSet;
		boolean done = false;

		SparkConf conf = new SparkConf().setAppName("AprioriCpa").setMaster(masterUrl);

		try {
			conf.registerKryoClasses(new Class<?>[]{
			    Class.forName("org.apache.hadoop.io.IntWritable"),
			    Class.forName("org.apache.hadoop.io.Text")
			});
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		conf.set("spark.core.connection.ack.wait.timeout","6000");
		
		JavaSparkContext sc = new JavaSparkContext(conf);

		/* Begin job1 */

		MainSpark.k = 1;
		
		Long startTime = System.currentTimeMillis();
		
		Broadcast<Integer> broadcastNumParts = sc.broadcast(MainSpark.num_parts);

		JavaRDD<String> lines = sc.textFile(clusterUrl + user + inputEntry + inputFileName, num_blocks);		
		/* Keep this, we're going to use it more times */
		lines.cache();
		
		/* Get support */
		MainSpark.support = MainSpark.supportPercentage * lines.count();
		
		Broadcast<Double> broadcastSupport = sc.broadcast(MainSpark.support);

		JavaPairRDD<String, Integer> words = lines.flatMap(line -> Arrays.asList(line.split(" ")))
				.mapToPair(w -> new Tuple2<String, Integer>(w, 1));		
		
		JavaPairRDD<String, Integer> result = words.reduceByKey((x, y) -> x + y, broadcastNumParts.value())
				.filter(w -> w._2 >= broadcastSupport.value()).mapToPair(w -> new Tuple2<String, Integer>(w._1, w._2))
				.sortByKey(new SerializableComparator());			

		long endTime = System.currentTimeMillis();
		long duration = endTime - startTime;
		duration = duration / 1000;

		MrUtils.appendToFile(MainSpark.durationLogName, "AprioriCpa Fase 1: " + Long.toString(duration));
		durations.add(duration);

		/* End job1 */

		if (result.count() == 0) {
			System.out.println("************************ Itemsets of size 1 empty! *****************************");
			System.exit(0);
		}		
		
		String outputCand = MainSpark.inputCandidates + MainSpark.countDir;		
		JavaPairRDD<Text, IntWritable> resultjob1Serializable = result.mapToPair(x -> new Tuple2<Text, IntWritable>(new Text(x._1), new IntWritable(x._2)));
				
		resultjob1Serializable.saveAsHadoopFile(outputCand, Text.class, IntWritable.class, SequenceFileOutputFormat.class);
		
		/* Generate size 2 Itemsets */

		List<Tuple2<String, Integer>> resultList = result.collect();
		
		System.out.println("++++++++++++++++++++++++++++++++++++++++ : " + resultList.size());

		List<String[]> sizeKItemset = new ArrayList<String[]>();

		for (int i = 0; i < resultList.size(); i++) {
			for (int j = i + 1; j < resultList.size(); j++) {
				sizeKItemset.add(new String[]{resultList.get(i)._1,resultList.get(j)._1});
			}
		}			

		if (sizeKItemset.size() == 0) {
			System.out.println("************************ Itemsets of size 2 empty! *****************************");
			System.exit(0);
		}			

		/* End generating size 2 Itemsets */

		MainSpark.k++; // k = 2
		MainSpark.countDir++; // countDir = 2
//		currentItemSet = sizeKItemset;		

		do {					
			/* Begin jobCount */
			
			startTime = System.currentTimeMillis();
			Broadcast<Integer> broadcastK = sc.broadcast(MainSpark.k);

			log.info("AprioriCpa Map contagem de C" + k);
			log.info("Arquivo de entrada no inputCandidates: ");

			JavaPairRDD<String,Integer> resultjobCount = null;

//			JavaRDD<String> pairs = sc.parallelize(currentItemSet);

//			List<String[]> kek = pairs.map(x -> x.split(" ")).collect();	
			resultjobCount = lines.mapPartitionsToPair(new Map2Spark3(sizeKItemset), true)
					.filter(kv -> !kv._1.equalsIgnoreCase("#"))
					.reduceByKey((x, y) -> x + y)
					.filter(f -> f._2 >= broadcastSupport.value());
//			fullList = lines.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
//				public Iterable<String> call(Iterator<String> it) {
//
//					List<String[]> kek_ = kek;
//					List<String> result = new ArrayList<String>();
//					String t;
//					String[] transactions;
//					String[] itemset;
//					List<String> keysOut = new ArrayList<String>();
//															
//					HashPrefixTree hpt_ = new HashPrefixTree();
//
//					for (int j = 0; j < kek_.size(); j++) {
//						hpt_.add(hpt_.getHashNode(), kek_.get(j), 0);
//					}
//					
//					System.out.println("**************************ARVORE CRIADA!!!");
//					
//					kek_.clear();										
//					
//					int k = broadcastK.getValue();
//
//					while (it.hasNext()) {
//						t = it.next();
//
//						transactions = t.split(" ");
//	
//						keysOut.clear();
//
//						for (int j = 0; j < transactions.length; j++) {
//							itemset = new String[k];
//							subSet(keysOut, transactions, hpt_.getHashNode(), j, itemset, 0, k);
//						}
//
//						result.addAll(keysOut);
//					}		
//					return result;
//				}
//			});																			
			
			if (resultjobCount.count() == 0) {
				System.out.println("************************ C" + MainSpark.k + " empty! *****************************");
				done = true;
				break;
			}
			
			outputCand = MainSpark.inputCandidates + MainSpark.countDir;		
			JavaPairRDD<Text, IntWritable> resultjobCountSerializable = resultjobCount.mapToPair(x -> new Tuple2<Text, IntWritable>(new Text(x._1), new IntWritable(x._2)));
					
			resultjobCountSerializable.saveAsHadoopFile(outputCand, Text.class, IntWritable.class, SequenceFileOutputFormat.class);

			endTime = System.currentTimeMillis();
			duration = endTime - startTime;
			duration = duration / 1000;		
			
			MrUtils.appendToFile(MainSpark.durationLogName, "AprioriCpa Contagem: " + Long.toString(duration));
			durations.add(duration);

			/* End jobCount */

			MainSpark.k++; // k = 3
			MainSpark.countDir++; // countDir = 3

			/* Begin jobGen */

			startTime = System.currentTimeMillis();

			System.out.println("AprioriCpa geração de candidatos - CountDir: " + MainSpark.countDir);

			HashSet<String> freqItemsets = new HashSet<String>();
			List<Tuple2<String, Integer>> listResultJobCount = resultjobCount.collect();

			for (int i = 0; i < listResultJobCount.size(); i++) {
				String s = listResultJobCount.get(i)._1;
				freqItemsets.add(s);
			}

			/* GenMap.java */

			JavaPairRDD<String, String> gen = resultjobCount
					.mapToPair(new PairFunction<Tuple2<String, Integer>, String, String>() {
						public Tuple2<String, String> call(Tuple2<String, Integer> t) {

							String s = t._1;

							String[] tokens = s.split(" ");

							StringBuilder sb = new StringBuilder();
							for (int i = 0; i < tokens.length - 1; i++) {
								sb.append(tokens[i]).append(" ");
							}
							return new Tuple2<String, String>(sb.toString().trim(), tokens[tokens.length - 1]);
						}
					});

			/* GenReduce.java */

			JavaPairRDD<String, Iterable<String>> lel = gen.groupByKey();

			JavaRDD<String[]> newK2 = lel.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,Iterable<String>>>, String[]>() {
//			});
//					new FlatMapFunction<Iterator<Tuple2<String,Iterable<String>>>, String>() {
						public Iterable<String[]> call(Iterator<Tuple2<String, Iterable<String>>> it) {

							List<String[]> ret = new ArrayList<String[]>();
							String[] newItemset;
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

								for (int i = 0; i < suffix.size() - 1; i++) {
									prefix = t._1 + " " + suffix.get(i) + " ";
									for (int j = i + 1; j < suffix.size(); j++) {
										newItemset = (prefix + suffix.get(j)).split(" ");
										if (allSubsetIsFrequent(newItemset, freqItemsets_)) {
											count++;
											ret.add(newItemset);
										}
									}
								}
							}
							return ret;
						}
					});

			if (newK2.count() == 0) {
				System.out.println("************************ C" + MainSpark.k + " empty! *****************************");
				done = true;
				break;
			}					

			// Can make this better
			sizeKItemset = newK2.collect();

//			ArrayList<String> killme = new ArrayList<String>();
//			for (Tuple2<String, Integer> t : kek2) {				
//				killme.add(t._1);
//			}
//
//			sizeKItemset = killme;

			/* Log duration */

			endTime = System.currentTimeMillis();
			duration = endTime - startTime;
			duration = duration / 1000;

			MrUtils.appendToFile(MainSpark.durationLogName, "AprioriCpa Geracao: " + Long.toString(duration));
			durations.add(duration);

			/* End jobGen */
		} while (!done);
		
		sc.stop();
		sc.close();
		
		return;
	}

	public static void main(String[] args) throws Exception {
		MainSpark m = new MainSpark();

		MrUtils.delOutDirs(user);

		MrUtils.initialConfig(args);

		MainSpark.countDir++;// 1
		MrUtils.printConfigs(m);

		MrUtils.appendToFile(MainSpark.durationLogName, "Usando " + MainSpark.inputFileName + " - "
				+ MainSpark.supportPercentage + " - " + MainSpark.num_parts + " - " + MainSpark.num_blocks + "\n");

		long startTime = System.currentTimeMillis();
		
		m.runItAll();

		long endTime = System.currentTimeMillis();
		long duration = endTime - startTime;
		duration = duration / 1000;

		System.out.println("Terminou em " + duration + " segundos");

		long totalJobsDuration = 0;
		for (long dur : durations) {
			totalJobsDuration += dur;
		}

		MrUtils.appendToFile(MainSpark.durationLogName,
				"Tempo map/reduces: " + Long.toString(totalJobsDuration) + " segundos.");
		MrUtils.appendToFile(MainSpark.durationLogName, "Terminou em " + Long.toString(duration) + " segundos.");

		Finish();
	}

}
