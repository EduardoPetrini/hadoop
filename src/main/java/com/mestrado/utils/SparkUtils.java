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

public class SparkUtils {

	public static void initialConfig(String[] args) {
		StringBuilder log = new StringBuilder();
		for (String s : args) {
			System.out.println("Args: " + s);
		}
		if (args.length != 0) {
			MainSpark.supportRate = Double.parseDouble(args[0]);
			if (args.length == 2) {
				MainSpark.NUM_BLOCK = Integer.parseInt(args[1]);
			}
		}
		
		SparkConf conf = new SparkConf().setAppName("Initial Config").setMaster(MainSpark.sparkUrl);
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		MainSpark.inputFileName = getFileName();
		
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
		JavaSparkContext sc = new JavaSparkContext(conf);
		String[] sp;
		for(String file: filesName){
			JavaRDD<String> inputFile = sc.textFile(file, MainSpark.NUM_BLOCK);
			inputFile.persist(StorageLevel.MEMORY_AND_DISK());
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
		for(Integer i: itemCounts){
			if(i != null){
//				System.out.println(sbs[index-1].toString());
				System.out.println(index+" : "+i);
				index++;
			}
		}
	}
}
