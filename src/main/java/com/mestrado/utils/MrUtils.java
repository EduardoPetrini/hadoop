package main.java.com.mestrado.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import main.java.com.mestrado.main.Main;
import main.java.com.mestrado.mapred.reduce.Reduce1;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
    
    /**
     * Deleta o diretório output
     */
    private void delDirs(String d) {
        
        log.info("Excluindo diretórios anteriores...");
        Configuration c = new Configuration();
//          c.set("fs.defaultFS", Main.clusterUrl);
        Path p = new Path(d);
        try {
            FileSystem fs = FileSystem.get(c);
            
            if(fs.isDirectory(p)){
                
                if(fs.delete(p, true)){
                    log.info("Excluido diretório -> "+p.getName());
                }else{
                    log.info("Nao foi possivel excluir "+p.getName());
                }
            }else{
                log.info(p.getParent()+""+p.getName()+" Nao eh um diretorio.");
                
                if(fs.isFile(p)){
                    log.info("Eh um arquivo, excluindo...");
                    
                    if(fs.delete(p, true)){
                        log.info("Excluindo arquivo -> "+p.getName());
                    }else{
                        log.info("Nao foi possivel excluir o arquivo -> "+p.getName());
                    }
                }
            }
            
        } catch (IOException ex) {
            Logger.getLogger(MrUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    public static void delOutDirs(String d) {
        
        System.out.println("Excluindo diretórios anteriores...");
        Configuration c = new Configuration();
//          c.set("fs.defaultFS", Main.clusterUrl);
        Path p = new Path(d);
        Path aux;
        
        try {
            FileSystem fs = FileSystem.get(c);
            
            if(fs.isDirectory(p)){
                
                FileStatus[] ff = fs.listStatus(p);
                
                for(FileStatus f: ff){
                    aux = f.getPath();
                    
                    if(aux.getName().contains("output")){
                        
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
    
    public void createTempDir(String d){
    	 Configuration c = new Configuration();
//           c.set("fs.defaultFS", Main.clusterUrl);
        try {
            FileSystem fs = FileSystem.get(c);
            
            if(fs.mkdirs(new Path(d))){
                log.info("Diretorio "+d+" criado com sucesso.");
            }else{
                log.info("Nao foi possivel criar o diretorio: "+d);
            }
            
        } catch (IOException ex) {
            Logger.getLogger(Reduce1.class.getName()).log(Level.SEVERE, null, ex);
        }
    
    }
    
    public void delContentFiles(String dir){
        Path p = new Path(dir);
        Configuration c = new Configuration();
//          c.set("fs.defaultFS", Main.clusterUrl);
        try {
            FileSystem fs = FileSystem.get(c);
            
            if(fs.isDirectory(p)){
                
                log.info(p.getName()+" eh um diretorio!");
                
                FileStatus[] f = fs.listStatus(p);
                log.info("Conteudo do diretorio: ");
                
                for(FileStatus ff: f){
                                        
                    log.info(ff.getPath().getName());
                    
                    if(ff.toString().contains("confIn")) continue;
                    
                    p = ff.getPath();
                    
                    if(fs.isFile(p)){
                        log.info("Deletando: "+p.getName());
                        if(fs.delete(p, true)){
                            
                            log.info("Deletado!");
                        }else{
                            log.info("Falha ao deletar.");
                        }
                    }
                }
            }
            
            
        }catch(IOException ex) {
            Logger.getLogger(Reduce1.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    public boolean checkOutput(String dir){
        
        Path p = new Path(dir);
        Path aux;
        Configuration c = new Configuration();
//          c.set("fs.defaultFS", Main.clusterUrl);
        System.out.println("Verificando diretório: "+dir);
        
        try{
            FileSystem fs = FileSystem.get(c);

                if(fs.isDirectory(p)){

                     FileStatus[] ff = fs.listStatus(p);

                     for(FileStatus f: ff){
                         
                         aux = f.getPath();
                         if(aux.getName().startsWith("part")){
                             System.out.println("Arquivos dentro do dir: "+aux.getName()+" "+f.getLen()/1024+"Kb ou "+f.getLen()+" bytes");
                             
                             if(f.getLen() > 0){
                                return true;
                                 
                             }else{
                                 return false;
                             }
                         }
                     }

                }else{
                    System.out.println("Não é um diretório: "+dir);
                    return false;
                }
        }catch(IOException e){
            System.out.println("ERROR: "+e);
        }
        System.out.println("Não contém part: "+dir);
        return false;
    }
    
    public boolean checkOutputMR(){
        String dir = Main.user+"outputCached/outputMR"+Main.countDir;
        Path p = new Path(dir);
        
        Configuration c = new Configuration();
//          c.set("fs.defaultFS", Main.clusterUrl);
        System.out.println("Verificando diretório: "+dir);
        
        try {
            FileSystem fs = FileSystem.get(c);
            
            if(fs.getLength(p) > 0){
                System.out.println("O arquivo "+dir+" não é vazio! "+fs.getLength(p));
                return true;
            }
            System.out.println("O arquivo "+dir+" é vazio! "+fs.getLength(p));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        return false;
    }
    
    /**
     * 
     */
    public static void initialConfig(){
    	String inputPathUri = Main.user+Main.inputEntry;
    	
    	Path inputPath = new Path(inputPathUri);
    	Configuration c = new Configuration();
//          c.set("fs.defaultFS", Main.clusterUrl);
        
        try {
        	
			FileSystem fs = inputPath.getFileSystem(c);
			FileStatus conf = fs.getFileStatus(inputPath);
			
			long blockSize = conf.getLen();
			long defaultSize = conf.getBlockSize();
			
			double bs = (double)blockSize;
			double ds = (double)defaultSize;
			double numBlock = bs/ds;
			
			int totalBlocks = (int)Math.ceil(numBlock);
			
			Main.totalBlockCount = totalBlocks;
			
//             FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(inputPath)));
			String line;
			Main.totalTransactionCount = 0;
			while (br.readLine() != null){
				Main.totalTransactionCount++;
			}
			Main.support = String.valueOf((Main.totalTransactionCount*Main.supportPercentage)/100);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
        
    }
    
    /**
     * Obtem a lista de blocos ids (offset+length)
     * @return
     */
    public static ArrayList<String> extractBlocksIds(){
    	String inputPathUri = Main.user+Main.inputEntry;
        Path inputPath = new Path(inputPathUri);
    	Configuration c = new Configuration();
//          c.set("fs.defaultFS", Main.clusterUrl);
        ArrayList<String> blocksIds = new ArrayList<String>();
        
        try{
        	FileSystem fs = inputPath.getFileSystem(c);
        	
        	BlockLocation[] bl = fs.getFileBlockLocations(fs.getFileStatus(inputPath),0,Long.MAX_VALUE);
        	for(BlockLocation b: bl){
        		blocksIds.add(String.valueOf(b.getOffset()));
        		
        	}
        	
        }catch(IOException e){
        	e.printStackTrace();
        }
        
        return blocksIds;
        
    }
    
    public static void test(){
    	String inputPathUri = Main.user+Main.inputEntry;
        Path inputPath = new Path(inputPathUri);
    	Configuration c = new Configuration();
//          c.set("fs.defaultFS", Main.clusterUrl);
        
        try{
        	FileSystem fs = FileSystem.get(c);
        	
        	FileStatus[] splitFiles = fs.listStatus(inputPath);
        	
        	for(FileStatus f: splitFiles){
        		System.out.println("Split Name: "+f.getPath().getName());
        		
        	}
        }catch(IOException e){
        	e.printStackTrace();
        }
    }

    /**
     * 
     * @param pathName
     */
    public static void createIfNotExistOrClean(String pathName){
    	Path path = new Path(pathName.substring(0,pathName.length()-9));
    	Configuration c = new Configuration();
//          c.set("fs.defaultFS", Main.clusterUrl);
    	
        try{
        	FileSystem fs = FileSystem.get(c);
        	
        	if(fs.exists(path)){
        		FileStatus[] fileStatus = fs.listStatus(path);
        		
        		for(FileStatus individualFileStatus: fileStatus){
        			fs.delete(individualFileStatus.getPath(), true);
        		}
        	}else{
        		fs.mkdirs(path);
        	}
        	
        	
        }catch(IOException e){
        	e.printStackTrace();
        }
    }
    
    /**
     * Obtém as partições criadas na fase 1
     * @param pathName
     * @return
     */
    public static ArrayList<String> getPartitions(String pathName){
    	ArrayList<String> partitions = new ArrayList<String>();
    	
    	Path path = new Path(pathName.substring(0,pathName.length()-9));
    	Configuration c = new Configuration();
//          c.set("fs.defaultFS", Main.clusterUrl);
        
        try{
        	FileSystem fs = FileSystem.get(c);
        	
        	if(fs.exists(path)){
        		FileStatus[] fileStatus = fs.listStatus(path);
        		
        		for(FileStatus individualFileStatus: fileStatus){
        			long len =individualFileStatus.getLen();
        			if(len > 128){
        				partitions.add(individualFileStatus.getPath().getName());
        			}
        		}
        	}else{
        		fs.mkdirs(path);
        	}
        	        	
        }catch(IOException e){
        	e.printStackTrace();
        }
    	
    	return partitions;
    }
    
    /**
     *     public static String user = "/user/eduardo/";
    public static String inputEntry = "input/T2.5I2D10N1500K.dobro";
    public static String clusterUrl = "hdfs://master/";
    public static long totalTransactionCount;
    public ArrayList<String> blocksIds;
    private String outputPartialName = user+"partitions-fase-1/partition";
     * @param m
     */
    public static void printConfigs(Main m){
    	System.out.println("\n******************************************************\n");
    	System.out.println("Count: "+m.countDir);
    	System.out.println("Support percentage: "+Main.supportPercentage);
    	System.out.println("Support: "+Main.support);
    	System.out.println("Total blocks/partitiions/Maps: "+Main.totalBlockCount);
    	System.out.println("Total transactions: "+Main.totalTransactionCount);
    	System.out.println("User dir: "+Main.user);
    	System.out.println("User partition dir: "+m.outputPartialName);
    	System.out.println("Entry file: "+Main.inputEntry);
    	System.out.println("Cluster url: "+Main.clusterUrl);
    	for(String b: m.blocksIds){
    		System.out.println("Blocks id: "+b);
    	}
    	System.out.println("\n******************************************************\n");
    }

	public static void configGlobalSupporte() {
		Main.support = String.valueOf(Main.supportPercentage * Main.totalTransactionCount);
		
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
}
