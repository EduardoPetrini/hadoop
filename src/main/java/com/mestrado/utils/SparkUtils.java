package main.java.com.mestrado.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import main.java.com.mestrado.main.MainSpark;
import scala.Tuple2;

public class SparkUtils {

	public static void initialConfig(String[] args) {
		StringBuilder log = new StringBuilder();
		for (String s : args) {
			System.out.println("Args: " + s);
		}
		if (args.length == 4) {
			MainSpark.supportRate = Double.parseDouble(args[0]);
			MainSpark.NUM_BLOCK = Integer.parseInt(args[1]);
			MainSpark.clusterUrl = "hdfs://"+args[3]+"/";
			MainSpark.sparkUrl = "spark://"+args[3]+":7077";
			MainSpark.user = MainSpark.clusterUrl + "user/hdp/";
			MainSpark.outputDir = MainSpark.user + "output-spark";
			MainSpark.outputPartialName = MainSpark.user + "partitions-fase-1/partition";
			MainSpark.inputFileName = MainSpark.user+MainSpark.inputEntry+args[2];
		}else{
			System.out.println("Missing arguments: SUPPORT NUM_BLOCK FILE_NAME MASTER_NAME");
			System.exit(1);
		}
		
		SparkConf conf = new SparkConf().setAppName("Initial Config").setMaster(MainSpark.sparkUrl);
		JavaSparkContext sc = new JavaSparkContext(conf);
		if(MainSpark.inputFileName == null || MainSpark.inputFileName == ""){
			MainSpark.inputFileName = getFileName();
		}
		
		long begin = System.currentTimeMillis();
		JavaRDD<String> inputFile = sc.textFile(MainSpark.inputFileName, MainSpark.NUM_BLOCK);
		inputFile.persist(StorageLevel.MEMORY_AND_DISK());
		MainSpark.totalTransactionCount = inputFile.count();
		MainSpark.totalBlockCount = inputFile.partitions().size();
		long end = System.currentTimeMillis();
		
		log.append("Initial config : "+(end-begin)+"ms "+((end-begin)/1000)+"s");
		
		MainSpark.blocksIds = new ArrayList<String>();
		for (Partition p : inputFile.partitions()) {
			MainSpark.blocksIds.add(String.valueOf(p.index()));
		}
		inputFile.unpersist();
		MainSpark.support = String.valueOf((MainSpark.totalTransactionCount * MainSpark.supportRate));
		sc.stop();
		sc.close();
		
		MainSpark.timeLog.add(log.toString());
		
	}
	
	private static String getFileName(){
		Configuration c = new Configuration();
		c.set("fs.defaultFS", MainSpark.clusterUrl);
		
		String fileName = "";
		
		try {
			FileSystem f = FileSystem.get(c);
			FileStatus[] fss = f.listStatus(new Path(MainSpark.user + MainSpark.inputEntry));
			for(FileStatus fs: fss){
				if(f.isFile(fs.getPath())){
					fileName = fs.getPath().getParent()+"/"+fs.getPath().getName();
					return fileName;
				}
			}
			f.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fileName;
	}

	public static SequenceFile.Writer[] configureWriters() {
		SequenceFile.Writer[] writers = new SequenceFile.Writer[MainSpark.NUM_BLOCK];
		String partitionFileName;
		Configuration c = new Configuration();
		c.set("fs.defaultFS", MainSpark.clusterUrl);
		for (int i = 1; i <= MainSpark.totalBlockCount; i++) {
			partitionFileName = MainSpark.outputPartialName + i;
			try {
				writers[i - 1] = SequenceFile.createWriter(c, SequenceFile.Writer.file(new Path(partitionFileName)),
						SequenceFile.Writer.keyClass(String.class), SequenceFile.Writer.valueClass(Integer.class));
			} catch (IllegalArgumentException | IOException e) {
				e.printStackTrace();
			}
		}
		return writers;
	}

	public static void printConfigs() {
		System.out.println("\n******************************************************\n");
		System.out.println("IMRApriori");
		System.out.println("Arquivo de entrada: " + MainSpark.inputFileName);
		System.out.println("Count: " + MainSpark.countDir);
		System.out.println("Support rate: " + MainSpark.supportRate);
		System.out.println("Support percentage: " + (MainSpark.supportRate * 100) + "%");
		System.out.println("Support min: " + MainSpark.support);
		System.out.println("Total blocks/partitiions/Maps: " + MainSpark.totalBlockCount);
		System.out.println("Total transactions: " + MainSpark.totalTransactionCount);
		System.out.println("User dir: " + MainSpark.user);
		System.out.println("User partition dir: " + MainSpark.outputPartialName);
		System.out.println("Entry dir: " + MainSpark.inputEntry);
		System.out.println("Cluster url: " + MainSpark.clusterUrl);
		System.out.println("Spark url: " + MainSpark.sparkUrl);
		for (String b : MainSpark.blocksIds) {
			System.out.println("Blocks id: " + b);
		}
		System.out.println("Blocks: " + MainSpark.NUM_BLOCK);
		System.out.println("\n******************************************************\n");
	}

	public static void infoNoItemset() {
		System.out.println("\n*******************\t*******************\t*******************\n");
		System.out.println("\tNenhum itemset gerado para os parametros de configuração.");
		System.out.println("\n*******************\t*******************\t*******************\n");
	}

	public static void closeWriters(Writer[] writers) {
		for(SequenceFile.Writer w: writers){
			try {
				if(w != null)
					w.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static List<String> getPartitionsFase1Dirs() {
		Configuration c = new Configuration();
        c.set("fs.defaultFS", MainSpark.clusterUrl);
        Path p = new Path(MainSpark.user+"partitions-fase-1");
        List<String> partitionsDirs = new ArrayList<String>();
        
        
        try {
			FileSystem fileSystem = FileSystem.get(c);
			
			if(fileSystem.isDirectory(p)){
				FileStatus[] fss = fileSystem.listStatus(p);
				System.out.println("Partitions directories found:");
				String dirName;
				for(FileStatus fs: fss){
					dirName = fs.getPath().getParent()+"/"+fs.getPath().getName();
					partitionsDirs.add(dirName);
					System.out.println(partitionsDirs.get(partitionsDirs.size()-1));
				}
			}
			fileSystem.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return partitionsDirs;
	}
	
	public static void countItemsets(String ... filesName) {
		Integer[] itemCounts = new Integer[20];
		StringBuilder[] sbs = new StringBuilder[20];
		SparkConf conf = new SparkConf().setAppName("Count results").setMaster(MainSpark.sparkUrl);
		conf.set("spark.shuffle.blockTransferService", "nio");
		JavaSparkContext sc = new JavaSparkContext(conf);
		String[] sp;
		StringBuilder log = new StringBuilder();
		
		for(String file: filesName){
			JavaRDD<String> inputFile = sc.textFile(file, MainSpark.NUM_BLOCK);
			inputFile.cache();
			System.out.println("\n\nSize of file: "+inputFile.count());
			List<String> lista = inputFile.collect();
			for(String l: lista){
				sp = l.replaceAll("\\(||\\)","").replaceAll(",.*","").split(" ");
				if(itemCounts[sp.length-1] == null){
					itemCounts[sp.length-1] = new Integer(1);
					sbs[sp.length-1] = new StringBuilder();
					sbs[sp.length-1].append(l).append("\n");
				}else{
					itemCounts[sp.length-1]++;
					sbs[sp.length-1].append(l).append("\n");
				}
			}
			inputFile.unpersist();
		}
		
		sc.stop();
		sc.close();
		int index = 1;
		log.append("\n|**********************************|\n\n");
		for(Integer i: itemCounts){
			if(i != null){
//				System.out.println(sbs[index-1].toString());
				log.append("**********  "+index+" : "+i+"\n");
				index++;
			}
		}
		log.append("\n|**********************************|\n\n");
		System.out.println(log.toString());
	}
	
	public static List<String[]> create2Itemsets(List<Tuple2<String,Integer>> globalOneItemsets){
		List<String[]> twoItemsets = new ArrayList<String[]>();
		for(int i = 0; i < globalOneItemsets.size(); i++){
			for(int j = i+1; j < globalOneItemsets.size(); j++){
				twoItemsets.add(new String[]{globalOneItemsets.get(i)._1,globalOneItemsets.get(j)._1});
			}
		}
		
		return twoItemsets;
	}

	public static List<String> createKItemsets(List<Tuple2<String, Integer>> kLessOneItemsets) {
		String prefix;
		String sufix;
		String newItemSet;
		StringBuilder tmpItem;
		List<String> newItemsetsCandidates = new ArrayList<String>();
		for_ext:
		for(int i=0; i<kLessOneItemsets.size(); i++){
//			// System.out.println("Progress: "+context.getProgress());
			for(int j=i+1; j<kLessOneItemsets.size(); j++){
//				System.out.println("Combining "+kLessOneItemsets.get(i)._1+" with "+kLessOneItemsets.get(j)._1);
				prefix = getPrefix(kLessOneItemsets.get(i)._1);
				
				if(kLessOneItemsets.get(j)._1.startsWith(prefix)){
					/*Se o próximo elemento já possui o mesmo prefixo, basta concatenar o sufixo do segundo item.*/
					sufix = getSufix(kLessOneItemsets.get(j)._1);
					
					tmpItem = new StringBuilder();
					tmpItem.append(kLessOneItemsets.get(i)._1).append(" ").append(sufix);
					//tmpItem é o novo candidato, verificar e todo o seu subconjunto é frequente
					newItemSet = tmpItem.toString().trim();
					if(allSubsetIsFrequent(newItemSet.split(" "),kLessOneItemsets)){
//						System.out.println("Combined!");
						newItemsetsCandidates.add(newItemSet);
					}
				}else{
					continue for_ext;
				}
			}
		}
		return newItemsetsCandidates;
	}
	
	private static boolean allSubsetIsFrequent(String[] itemset,List<Tuple2<String, Integer>> kLessOneItemsets) {
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
			//See other way more efficient
			for(Tuple2<String,Integer> t: kLessOneItemsets){
				if(t._1.equalsIgnoreCase(subItem.toString())){
					return false;
				}
			}
			indexToSkip++;
		}
		
		return true;
	}
	
	public static String getSufix(String kitem){
		String[] spkitem = kitem.split(" ");
		return spkitem[spkitem.length-1].trim();
	}
	
	public static String getPrefix(String kitem){
        
        String[] spkitem = kitem.split(" ");
        StringBuilder sb = new StringBuilder();
        
        for (int i = 0; i < spkitem.length-1; i++) {
            
            sb.append(spkitem[i]).append(" ");
        }
        
        //k = spkitem.length;
        return sb.toString();
    }
}

