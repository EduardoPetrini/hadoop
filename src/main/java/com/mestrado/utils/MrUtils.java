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

import main.java.com.mestrado.main.Main;
import main.java.com.mestrado.mapred.reduce.Reduce1;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

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
    public void delDirs(String d) {
        
        log.info("Excluindo diretórios anteriores...");
        Configuration c = new Configuration();
         c.set("fs.defaultFS", Main.clusterUrl);
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
         c.set("fs.defaultFS", Main.clusterUrl);
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
          c.set("fs.defaultFS", Main.clusterUrl);
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
         c.set("fs.defaultFS", Main.clusterUrl);
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
         c.set("fs.defaultFS", Main.clusterUrl);
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
    
    /**
     * 
     * @return false if empty file
     */
    public static boolean checkOutputMR(){
        String dir = Main.user+"outputCached/outputMR"+Main.countDir;
        Path p = new Path(dir);
        
        Configuration c = new Configuration();
         c.set("fs.defaultFS", Main.clusterUrl);
        System.out.println("Verificando diretório: "+dir);
        
        try {
            FileSystem fs = FileSystem.get(c);
            FileStatus conf = fs.getFileStatus(p);
            long len = conf.getLen();
            if(conf.getLen() > 130){
                System.out.println("O arquivo "+dir+" não é vazio! "+conf.getLen());
                return true;
            }
            System.out.println("O arquivo "+dir+" é vazio! "+conf.getLen());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        return false;
    }
    
    /**
     * Calcula a quantidade de transações do arquivo de entrada
     * Calcula o suporte
     */
    public static void initialConfig(String[] args){
    	
    	for(String s: args){
    		System.out.println("Args: "+s);
    	}
    	if(args.length != 0){
    		Main.supportPercentage = Double.parseDouble(args[0]);
    	}
    	String inputPathUri = Main.user+Main.inputEntry;
    	
    	Path inputPath = new Path(inputPathUri);
    	Configuration c = new Configuration();
         c.set("fs.defaultFS", Main.clusterUrl);
        
        try {
        	
			FileSystem fs = inputPath.getFileSystem(c);
			FileStatus[] subFiles = fs.listStatus(inputPath);
			FileStatus conf = subFiles[0];
			Main.inputFileName = conf.getPath().getName();
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(conf.getPath())));
			Main.totalTransactionCount = 0;
			while (br.readLine() != null){
				Main.totalTransactionCount++;
			}
			Main.support = String.valueOf(Main.totalTransactionCount*Main.supportPercentage);
			
//			Path outputCached = new Path(Main.user+"outputCached/");
//			if(!fs.exists(outputCached)){
//				fs.create(outputCached);
//			}else{
//				for(FileStatus fss: fs.listStatus(outputCached)){
//					fs.delete(fss.getPath(), true);
//				}
//			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
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
         c.set("fs.defaultFS", Main.clusterUrl);
    	
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
         c.set("fs.defaultFS", Main.clusterUrl);
        
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
    	System.out.println("Count: "+Main.countDir);
    	System.out.println("Support percentage: "+Main.supportPercentage);
    	System.out.println("Support: "+Main.support);
    	System.out.println("User dir: "+Main.user);
    	System.out.println("Entry file: "+Main.inputEntry);
    	System.out.println("Cluster url: "+Main.clusterUrl);
    	
    	System.out.println("\n******************************************************\n");
    }
    
    public static int getK(int k) {
		String inputPathUri = Main.fileCached+(Main.countDir);
    	
    	Path inputPath = new Path(inputPathUri);
    	Configuration c = new Configuration();
         c.set("fs.defaultFS", Main.clusterUrl);
        
        try {
        	
			FileSystem fs = inputPath.getFileSystem(c);
			SequenceFile.Reader reader = new SequenceFile.Reader(c, SequenceFile.Reader.file(inputPath));
				
			Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), c);
			IntWritable value = (IntWritable) ReflectionUtils.newInstance(reader.getValueClass(), c);
			int kCount = 0;
			while (reader.next(key, value)) {
				System.out.println("Key: "+key.toString());
	            kCount = key.toString().split(" ").length;
	            break;
	        }
		
			return kCount;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;
	}
    
    /**
	 * 
	 * @param data
	 * @param fileName
	 */
	public static void saveFileInLocal(String data, String fileName){
		File file = new File(fileName);
		if(file.exists()){
			try{
				file.delete();
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		FileWriter fw;
		BufferedWriter bw;
		try{
			fw = new FileWriter(file.getAbsoluteFile(), false);
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
	public static void saveTimeLog(String data){
		StringBuilder sb = new StringBuilder("/home/eduardo/times/");
		File file = new File(sb.toString());
		if(!file.isDirectory()){
			file.mkdirs();
		}
		sb.append(data.split(" ")[0]).append("-").append("-").append(Main.inputFileName).append("_").append(System.currentTimeMillis()).append(".log");
		System.out.println("Saving: "+data+"\n into "+sb.toString());
		saveFileInLocal(data, sb.toString());
	}
}
