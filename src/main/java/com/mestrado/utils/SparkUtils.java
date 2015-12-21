package main.java.com.mestrado.utils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

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

public class SparkUtils implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

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

		if (MainSpark.inputFileName == null || MainSpark.inputFileName == "") {
			MainSpark.inputFileName = getFileName();
		}

		long begin = System.currentTimeMillis();
		JavaRDD<String> inputFile = sc.textFile(MainSpark.inputFileName, MainSpark.NUM_BLOCK);
		inputFile.persist(StorageLevel.MEMORY_AND_DISK());
		MainSpark.totalTransactionCount = inputFile.count();
		MainSpark.totalBlockCount = inputFile.partitions().size();
		long end = System.currentTimeMillis();

		log.append("Initial config : " + (end - begin) + "ms " + ((end - begin) / 1000) + "s");

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

	private static String getFileName() {
		Configuration c = new Configuration();
		c.set("fs.defaultFS", MainSpark.clusterUrl);

		String fileName = "";

		try {
			FileSystem f = FileSystem.get(c);
			FileStatus[] fss = f.listStatus(new Path(MainSpark.user + MainSpark.inputEntry));
			for (FileStatus fs : fss) {
				if (f.isFile(fs.getPath())) {
					fileName = fs.getPath().getParent() + "/" + fs.getPath().getName();
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
				writers[i - 1] = SequenceFile.createWriter(c, SequenceFile.Writer.file(new Path(partitionFileName)), SequenceFile.Writer.keyClass(String.class),
						SequenceFile.Writer.valueClass(Integer.class));
			} catch (IllegalArgumentException | IOException e) {
				e.printStackTrace();
			}
		}
		return writers;
	}

	public static void printConfigs() {
		System.out.println("\n******************************************************\n");
		System.out.println("Spark IMRApriori v2");
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
		for (SequenceFile.Writer w : writers) {
			try {
				if (w != null)
					w.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static List<String> getPartitionsFase1Dirs() {
		Configuration c = new Configuration();
		c.set("fs.defaultFS", MainSpark.clusterUrl);
		Path p = new Path(MainSpark.user + "partitions-fase-1");
		List<String> partitionsDirs = new ArrayList<String>();

		try {
			FileSystem fileSystem = FileSystem.get(c);

			if (fileSystem.isDirectory(p)) {
				FileStatus[] fss = fileSystem.listStatus(p);
				System.out.println("Partitions directories found:");
				String dirName;
				for (FileStatus fs : fss) {
					dirName = fs.getPath().getParent() + "/" + fs.getPath().getName();
					partitionsDirs.add(dirName);
					System.out.println(partitionsDirs.get(partitionsDirs.size() - 1));
				}
			}
			fileSystem.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return partitionsDirs;
	}

	public static List<String> getAllFilesInDir(Configuration conf, String sequenceFileName) {
		List<String> partitionsFiles = new ArrayList<String>();
		try {
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path(sequenceFileName);
			if (fs.exists(path)) {
				FileStatus[] fileStatus = fs.listStatus(path);

				for (FileStatus individualFileStatus : fileStatus) {
					System.out.println(individualFileStatus.getPath().getName() + " " + individualFileStatus.getLen());
					if (individualFileStatus.getLen() > 0) {
						partitionsFiles.add(sequenceFileName + "/" + individualFileStatus.getPath().getName());
					}
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		return partitionsFiles;
	}

	public static void countItemsets(String... filesName) {
		Integer[] itemCounts = new Integer[20];
		StringBuilder[] sbs = new StringBuilder[20];
		SparkConf conf = new SparkConf().setAppName("Count results").setMaster(MainSpark.sparkUrl);
		JavaSparkContext sc = new JavaSparkContext(conf);
		String[] sp;
		for (String file : filesName) {
			JavaRDD<String> inputFile = sc.textFile(file, MainSpark.NUM_BLOCK);
			inputFile.persist(StorageLevel.MEMORY_AND_DISK());
			List<String> lista = inputFile.collect();
			for (String l : lista) {
				sp = l.replaceAll("\\(||\\)", "").replaceAll(",.*", "").split(" ");
				if (itemCounts[sp.length - 1] == null) {
					itemCounts[sp.length - 1] = new Integer(1);
					sbs[sp.length - 1] = new StringBuilder();
					sbs[sp.length - 1].append(l).append("\n");
				} else {
					itemCounts[sp.length - 1]++;
					sbs[sp.length - 1].append(l).append("\n");
				}
			}
			inputFile.unpersist();
		}

		sc.stop();
		sc.close();
		int index = 1;
		for (Integer i : itemCounts) {
			if (i != null) {
				// System.out.println(sbs[index-1].toString());
				System.out.println(index + " : " + i);
				index++;
			}
		}
	}

	public static void saveEntryArrayInHdfs(Configuration conf, List<Entry<String, Integer>> entries) {
		Path path = new Path(MainSpark.user + "output-spark-newItemsets/itemsets");
		try {
			FileSystem fs = FileSystem.get(conf);
			
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(path)));
			
			for(Entry<String, Integer> entry: entries){
				bw.write(entry.getKey()+"\t:"+entry.getValue()+"\n");
			}
			bw.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static List<String[]> creatTwoItemsets(List<String> oneItems){
		List<String[]> twoItems = new ArrayList<String[]>();
		for(int i = 0; i < oneItems.size()-1; i++){
			for(int j = i+1; j < oneItems.size(); j++){
				twoItems.add(new String[]{oneItems.get(i),oneItems.get(j)});
			}
		}
		return twoItems;
	}
	
	public static List<String[]> createKItemsets(List<String[]> kLessOneItemsets, int k){
		Collections.sort(kLessOneItemsets,ITEMSET_ORDER);
		
		List<String[]> kItemsets = new ArrayList<String[]>();
		String[] itemA;
		String[] itemB;
		String[] itemsetCSpt;
		for_ext:
		for (int i = 0; i < kLessOneItemsets.size()-1; i++){
//			System.out.println("\nin item i "+kLessOneItemsets.get(i)+"\n");
        	itemA = kLessOneItemsets.get(i);
        	for (int j = i+1; j < kLessOneItemsets.size(); j++){
        		itemB = kLessOneItemsets.get(j);
//        		System.out.println("\nin item j "+kLessOneItemsets.get(j)+" k is "+k+"\n");
        		if(isSamePrefix(itemA, itemB, i, j, k)){
        			itemsetCSpt = new String[k];
        			combine(itemA, itemB, itemsetCSpt, k);
        			if(allSubsetIsFrequent(itemsetCSpt, kLessOneItemsets)){
//	        			itemsetAux.add(itemsetC);
        				kItemsets.add(itemsetCSpt);
        			}
        		}else continue for_ext;
        	}
		}
		return kItemsets;
	}
	/**
     * 
     * @param itemA
     * @param itemB
     * @param i
     * @param j
     * @return
     */
    public static boolean isSamePrefix(String[] itemA, String[] itemB, int i, int j, int k){
    	for(int a = 0; a < k - 2; a++){
            if(!itemA[a].equals(itemB[a])){
            	//System.out.println("Não é o mesmo prefixo: "+itemA[a]+" != "+itemB[a]);
                return false;
            }
        }
        
    	return true;
    }
    
    /**
     * 
     * @param itemA
     * @param itemB
     * @return
     */
    public static void combine(String[] itemA, String[] itemB, String[] itemsetCSpt, int k){
        for(int i = 0; i < itemA.length; i++){
            itemsetCSpt[i] = itemA[i]; 
        }
        itemsetCSpt[k-1] = itemB[itemB.length-1];
    }
    
    /**
     * 
     * @param itemset
     * @param frequents
     * @return
     */
    private static boolean allSubsetIsFrequent(String[] itemset, List<String[]> frequents) { 
		int indexToSkip = 0;
		int index;
		String[] subItem = new String[itemset.length-1];
		boolean subIsFrequent;
		for(int j = 0; j < itemset.length-1; j++){
			index = 0;
			for(int i = 0; i < itemset.length; i++){
				if(i != indexToSkip){
					subItem[index] = itemset[i];
					index++;
				}
			}
			//subItem gerado, verificar se é do conjunto frequente
			subIsFrequent = false;
			for_middle:
			for(int i = 0; i < frequents.size(); i++){
				for(int x = 0; x < frequents.get(i).length; x++){
					if(!frequents.get(i)[x].equalsIgnoreCase(subItem[x])){
						continue for_middle;
					}else{
						if(x == subItem.length-1){
							subIsFrequent = true;
						}
					}
				}
			}
			if(!subIsFrequent) return false;
			indexToSkip++;
		}
		
		return true;
	}

	public static List<String[]> createKItemsetsString(List<String> kLessOneItemsets, Integer k) {
		List<String[]> kItemsets = new ArrayList<String[]>();
		String[] itemA;
		String[] itemB;
		String[] itemsetCSpt;
		for (int i = 0; i < kLessOneItemsets.size()-1; i++){
        	itemA = kLessOneItemsets.get(i).split(" ");
        	for (int j = i+1; j < kLessOneItemsets.size(); j++){
        		itemB = kLessOneItemsets.get(j).split(" ");
        		if(isSamePrefix(itemA, itemB, i, j, k)){
        			itemsetCSpt = new String[k];
        			combine(itemA, itemB, itemsetCSpt, k);
        			if(allSubsetIsFrequentString(itemsetCSpt, kLessOneItemsets)){
        				kItemsets.add(itemsetCSpt);
        			}
        		}
        	}
		}
		return kItemsets;
	}

	private static boolean allSubsetIsFrequentString(String[] itemset, List<String> frequents) {
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
			
			if(!frequents.contains(subItem.toString().trim())){
				return false;
			}
			indexToSkip++;
		}
		
		return true;
	}
	public static void main(String[] nda){
		int j = 1,i = 2;
		System.out.println(j+" "+i);
	}
	
	public static Comparator<String[]> ITEMSET_ORDER = new Comparator<String[]>() {

		@Override
		public int compare(String[] o1, String[] o2) {
			for(int i =0; i < o1.length; i++){
				if(Integer.parseInt(o1[i]) < Integer.parseInt(o2[i])){
					return -1;
				}else if(Integer.parseInt(o1[i]) > Integer.parseInt(o2[i])){
					return 1;
				}
			}
			return 0;
		}
		
	};
}
