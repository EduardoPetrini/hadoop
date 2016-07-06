package main.java.com.mestrado.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import main.java.com.mestrado.main.MainSpark;

public class MrUtils {
	private Log log = LogFactory.getLog(MrUtils.class);
	
    public boolean increseMapTask(Path file, Configuration c){
        try {
            FileSystem fs = FileSystem.get(c);
            
            FileStatus f = fs.getFileStatus(file);
            
            long blockSize = f.getBlockSize();
            long fileSize = f.getLen();
            
            if((4*blockSize) >= fileSize){
                return false;
            }
            
        } catch (IOException ex) {
            Logger.getLogger(MrUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return true;
        
    }
    
    public static void delOutDirs(String d) {
        
        System.out.println("Excluindo diretórios anteriores...");
        Configuration c = new Configuration();
        c.set("fs.defaultFS", MainSpark.clusterUrl);
        Path p = new Path(d);
        Path aux;
        
        try {
            FileSystem fs = FileSystem.get(c);
 
            if(fs.isDirectory(p)){
                FileStatus[] ff = fs.listStatus(p);
                for(FileStatus f: ff){
                    aux = f.getPath();
                    if(aux.getName().contains("output") || aux.getName().contains("partitions-fase-1")){
                        if(fs.delete(aux, true)){
                        	System.out.println("Excluido diretório -> "+aux.getName());
                        }else{
                        	System.out.println("Nao foi possivel excluir "+aux.getName());
                        }
                    }
                }
                
            }
            
        } catch (IOException ex) {
            Logger.getLogger(MrUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
	 public static ArrayList<String> getAllOuputFilesNames(String outName) {
	    	ArrayList<String> candNames = new ArrayList<String>();
	    	
	    	Path p = new Path(outName);
	    	Path aux;
	        Configuration c = new Configuration();
	        c.set("fs.defaultFS", MainSpark.clusterUrl);
	        try{
	            FileSystem fs = FileSystem.get(c);
	            if(fs.exists(p)){
		            FileStatus[] ff = fs.listStatus(p);
		
					 for(FileStatus f: ff){
					     aux = f.getPath();
					     if(aux.getName().startsWith("part")){
					    	 candNames.add(outName+"/"+aux.getName());
					     }
					 }
	            }
	        }catch(IOException e){
	            System.out.println("ERROR: "+e);
	        }
			return candNames;
		}
	 
	/**
	 * 
	 * @param part1
	 * @param part2
	 * @param byteDiference
	 * @return
	 */
	public static boolean checkPartitions(String part1, String part2, long byteDiference){
		
		long p1 = Integer.valueOf(part1);
		long p2 = Integer.valueOf(part2);
		if((p1 <= (p2+byteDiference)) && (p1 >= (p2-byteDiference))) return true;
		return false;
	}
	
	/**
	 * 
	 * @param data
	 * @param fileName
	 */
	public static void saveFileInLocal(String data, String fileName){
		File file = new File(fileName);
		
		FileWriter fw;
		BufferedWriter bw;
		try{
			fw = new FileWriter(file.getAbsoluteFile(), true);
			bw = new BufferedWriter(fw);
			bw.write(data);
			bw.close();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param data
	 */
	public static void saveTimeLog(String data, String[] inputFileName){
		StringBuilder sb = new StringBuilder("/home/hadoop/petrini/times/");
		File file = new File(sb.toString());
		if(!file.isDirectory()){
			file.mkdirs();
		}
		sb.append("Spark-IMRApriori-Iterative").append("-").append(inputFileName[inputFileName.length-1]).append("-").append(MainSpark.supportRate).append("-").append(MainSpark.NUM_BLOCK).append(".log");
		System.out.println("Saving: "+data+"\n into "+sb.toString());
		saveFileInLocal(data, sb.toString());
	}
}
