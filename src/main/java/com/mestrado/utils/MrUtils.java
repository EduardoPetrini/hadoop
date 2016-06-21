package main.java.com.mestrado.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

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
    
    /**
     * Deleta o diretório output
     */
    public void delDirs(String d) {
        
        log.info("Excluindo diretórios anteriores...");
        Configuration c = new Configuration();
         c.set("fs.defaultFS", MainSpark.clusterUrl);
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
        
        File file = new File(MainSpark.durationLogName);
        file.delete();
        
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
                    
                    if(aux.getName().contains("output") || aux.getName().contains("inputToGen") ||  aux.getName().contains("inputCached") ||
                    		aux.getName().contains("candidatosTxt") || aux.getName().contains("outputCandidates") || 
                    		aux.getName().contains("inputCandidates") || aux.getName().contains("discard")){
                        
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
    	 c.set("fs.defaultFS", MainSpark.clusterUrl);
        try {
            FileSystem fs = FileSystem.get(c);
            
            if(fs.mkdirs(new Path(d))){
                log.info("Diretorio "+d+" criado com sucesso.");
            }else{
                log.info("Nao foi possivel criar o diretorio: "+d);
            }
            
        } catch (IOException ex) {
            Logger.getLogger(MrUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
    
    }
    
    /**
     * @param p
     */
    public static void delContentFiles(Path p){
        Configuration c = new Configuration();
         c.set("fs.defaultFS", MainSpark.clusterUrl);
        try {
            FileSystem fs = FileSystem.get(c);
            
            if(fs.isDirectory(p)){
                
                System.out.println(p.getName()+" eh um diretorio!");
                
                FileStatus[] f = fs.listStatus(p);
                System.out.println("Conteudo do diretorio: ");
                
                for(FileStatus ff: f){
                                        
                	System.out.println(ff.getPath().getName());
                    
                    p = ff.getPath();
                    
                	System.out.println("Deletando: "+p.getName());
                    if(fs.delete(p, true)){
                        
                    	System.out.println("Deletado!");
                    }else{
                    	System.out.println("Falha ao deletar.");
                    }
                }
            }
            
            
        }catch(IOException ex) {
            Logger.getLogger(MrUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    /**
     * 
     * @param dir
     * @return
     */
    public static boolean checkOutput(String dir){
        
        Path p = new Path(dir);
        Path aux;
        Configuration c = new Configuration();
        c.set("fs.defaultFS", MainSpark.clusterUrl);
        System.out.println("Verificando diretório: " + dir);
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
    
    public static boolean checkOutputMR(){
        Path path;
        
        Configuration c = new Configuration();
        c.set("fs.defaultFS", MainSpark.clusterUrl);

        try {
            FileSystem fs = FileSystem.get(c);
            
            //obter todos os arquivos dos candidatos atuais
            MainSpark.candFilesNames = getAllCandidatesFilesNames();
            ArrayList<String> filesEmpty = new ArrayList<String>();
            for(String fileName: MainSpark.candFilesNames){
            	path = new Path(fileName);
            	FileStatus conf = fs.getFileStatus(path);
            	if(conf.getLen() > 128){
            		System.out.println("O arquivo " + path.getName() + " não é vazio! " + conf.getLen());
            	}else{
            		filesEmpty.add(fileName);
            		System.out.println("O arquivo " + path.getName() + " é vazio! " + conf.getLen());
            	}
            }
            if (!filesEmpty.isEmpty())
            	MainSpark.candFilesNames.removeAll(filesEmpty);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (MainSpark.candFilesNames.size() > 0){
        	return true;
        }
        return false;
    }
    
    private static ArrayList<String> getAllCandidatesFilesNames() {
    	ArrayList<String> candNames = new ArrayList<String>();
    	
    	Path p = new Path(MainSpark.inputCandidatesDir);
    	Path aux;
        Configuration c = new Configuration();
        c.set("fs.defaultFS", MainSpark.clusterUrl);
        try{
            FileSystem fs = FileSystem.get(c);
            FileStatus[] ff = fs.listStatus(p);

			 for(FileStatus f: ff){
			     aux = f.getPath();
			     if(aux.getName().contains("C"+MainSpark.countDir)){
			    	 candNames.add(MainSpark.inputCandidatesDir+"/"+aux.getName());
			     }
			 }

        }catch(IOException e){
            System.out.println("ERROR: "+e);
        }
		return candNames;
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

	public static boolean checkInputMR(){
//        String dir = MainSpark.inputL+MainSpark.countDir;
//        Path p = new Path(dir);
//        
//        Configuration c = new Configuration();
//         c.set("fs.defaultFS", MainSpark.clusterUrl);
//        System.out.println("Verificando diretório: "+dir);
//        
//        try {
//            FileSystem fs = FileSystem.get(c);
//            if(fs.exists(p)){
//	            FileStatus conf = fs.getFileStatus(p);
//	            long len = conf.getLen();
//	            if(conf.getLen() > 128){
//	                System.out.println("O arquivo "+dir+" não é vazio! "+conf.getLen());
//	                return true;
//	            }
//	            System.out.println("O arquivo "+dir+" é vazio! "+conf.getLen());
//            }else{
//            	System.out.println("Arquivo "+dir+" não existe!");
//            }
//        } catch (IOException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//        
        return false;
    }
    
    /**
     * Calcula a quantidade de transações do arquivo de entrada
     * Calcula o suporte
     */
    public static void initialConfig(String[] args){
    	
    	for(String s: args){
    		System.out.println("Args: " + s);
    	}
    	
    	if (args.length == 0) {
    		System.out.println("Modo de usar: ... aprioriCpaSpark.jar {INPUT FILENAME} {SUPPORT % (Optional)} {NUM_PARTS (Optional)} {NUM_BLOCKS (Optional)}");
    		System.exit(1);
    	} else if (args.length == 1) {
    		MainSpark.inputFileName = args[0];
    	} else if (args.length == 2) {
    		MainSpark.inputFileName = args[0];
    		MainSpark.supportPercentage = Double.parseDouble(args[1]);
    	} else if (args.length == 3) {
    		MainSpark.inputFileName = args[0];
    		MainSpark.supportPercentage = Double.parseDouble(args[1]);
    		MainSpark.num_parts = Integer.parseInt(args[2]);
    	} else if (args.length == 4) {
        	MainSpark.inputFileName = args[0];
        	MainSpark.supportPercentage = Double.parseDouble(args[1]);
        	MainSpark.num_parts = Integer.parseInt(args[2]);
        	MainSpark.num_blocks = Integer.parseInt(args[3]);
    	}  else if (args.length == 5) {
        	MainSpark.inputFileName = args[0];
        	MainSpark.supportPercentage = Double.parseDouble(args[1]);
        	MainSpark.num_parts = Integer.parseInt(args[2]);
        	MainSpark.num_blocks = Integer.parseInt(args[3]);
        	MainSpark.numExecution = Integer.parseInt((args[4]));
    	} else {
    		System.out.println("Modo de usar: ... aprioriCpaSpark.jar {INPUT FILENAME} {SUPPORT % (Optional)} {NUM_PARTS (Optional)} {NUM_BLOCKS (Optional)}");
    		System.exit(1);
    	}
    	String logDir = "/home/hadoop/petrini/times/resultados-" + MainSpark.inputFileName;
    	File f = new File(logDir);
    	if (!f.isDirectory()) {
    		f.mkdir();
    	}
    	
    	MainSpark.durationLogName = logDir+ "/" + MainSpark.inputFileName + " - " + MainSpark.supportPercentage + " - " + MainSpark.num_parts + " - " + MainSpark.num_blocks + ".log" + MainSpark.numExecution; 
    	
    	String inputPathUri = MainSpark.user + MainSpark.inputEntry;
    	
    	Path inputPath = new Path(inputPathUri + MainSpark.inputFileName);
    	Configuration c = new Configuration();
        c.set("fs.defaultFS", MainSpark.clusterUrl);
        
        try {	
			FileSystem fs = inputPath.getFileSystem(c);
			
			//FileStatus[] subFiles = fs.listStatus(inputPath);
			
			//Path inputFile = subFiles[0].getPath();*/
			//MainSpark.inputFileName = inputFile.getName();
			/*BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inputPath)));
			MainSpark.totalTransactionCount = 0;
			while (br.readLine() != null){
				MainSpark.totalTransactionCount++;
			}
			
			MainSpark.support = MainSpark.totalTransactionCount * MainSpark.supportPercentage;*/
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    /**
     * Obtem a lista de blocos ids (offset+length)
     * @return
     */
    public static ArrayList<String> extractBlocksIds(){
    	String inputPathUri = MainSpark.user+MainSpark.inputEntry;
        Path inputPath = new Path(inputPathUri);
    	Configuration c = new Configuration();
        c.set("fs.defaultFS", MainSpark.clusterUrl);
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
    	String inputPathUri = MainSpark.user+MainSpark.inputEntry;
        Path inputPath = new Path(inputPathUri);
    	Configuration c = new Configuration();
         c.set("fs.defaultFS", MainSpark.clusterUrl);
        
        try{
        	FileSystem fs = FileSystem.get(c);
        	FileStatus[] subFiles = fs.listStatus(inputPath);
			Path inputFile = subFiles[0].getPath();
        	FileStatus[] splitFiles = fs.listStatus(inputFile);
        	
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
         c.set("fs.defaultFS", MainSpark.clusterUrl);
    	
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
         c.set("fs.defaultFS", MainSpark.clusterUrl);
        
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
    
    public static void appendToFile(String fileName, String text) {
    	try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(fileName, true)))) {
    	    out.println(text);
    	}catch (IOException e) {
    	    //exception handling left as an exercise for the reader
    	}
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
    public static void printConfigs(MainSpark m){
    	System.out.println("\n******************************************************\n");
    	
    	System.out.println("Count: " + MainSpark.countDir);
    	System.out.println("Support percentage: " + MainSpark.supportPercentage);
    	System.out.println("Support: " + MainSpark.support);
    	System.out.println("User dir: " + MainSpark.user);
    	System.out.println("Entry file: " + MainSpark.inputEntry + MainSpark.inputFileName);
    	System.out.println("Cluster url: " + MainSpark.clusterUrl);
    	System.out.println("Num parts: " + MainSpark.num_parts);
    	
    	System.out.println("\n******************************************************\n");
    }
    
    /**
     * 
     * @param fileName
     * @return ArrayList<String>
     */
    public static ArrayList<String> readFromHDFS(String fileName){
    	Path inputPath = new Path(fileName);
    	Configuration c = new Configuration();
        c.set("fs.defaultFS", MainSpark.clusterUrl);
        ArrayList<String> data = new ArrayList<String>();
        
        try {
        	
			FileSystem fs = inputPath.getFileSystem(c);
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(inputPath)));
			String line;
			while ((line = br.readLine()) != null){
				data.add(line.split("\\t")[0].trim());
				
			}
			br.close();
        }catch(IOException e){
        	e.printStackTrace();
        }
        
        return data;
    }
    
    /**
     * 
     * @param dirName
     * @return
     */
    public static ArrayList<String> readAllFromHDFSDir(String dirName){
    	Path inputPath = new Path(dirName);
    	Configuration c = new Configuration();
        c.set("fs.defaultFS", MainSpark.clusterUrl);
        ArrayList<String> data = new ArrayList<String>();
        
        try {   	
			FileSystem fs = inputPath.getFileSystem(c);
			FileStatus[] files = fs.listStatus(inputPath);
			BufferedReader br;
			String line;
			for (FileStatus fst : files) {
				if (fst.getPath().getName().startsWith("part")) {
					System.out.println("Carregando itemsets de " + dirName + " " + fst.getPath().getName());
					br = new BufferedReader(new InputStreamReader(fs.open(fst.getPath())));
					while ((line = br.readLine()) != null) {
						line = line.substring(1, line.length() -1);
						data.add(line.split(",")[0].trim());
					}
					br.close();
				}
			}
        } catch (IOException e) {
        	e.printStackTrace();
        }
        
        return data;
    }
    /**
     * 
     * @param data
     * @param fileName
     */
    public static void saveTextInHDFS(ArrayList<String> data, String fileName){
    	Path inputPath = new Path(fileName);
    	Configuration c = new Configuration();
         c.set("fs.defaultFS", MainSpark.clusterUrl);
        
        	try {
			FileSystem fs = inputPath.getFileSystem(c);
			if(fs.exists(inputPath)){
				fs.delete(inputPath, true);
			}
			
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(inputPath, true)));
			
			for(String d: data){
				bw.write(d+"\n");
			}
			bw.close();
        }catch(IOException e){
        	e.printStackTrace();
        }
    }

    /**
     * 
     * @param outputDir
     * @return
     */
	public static String getOutputFile(String outputDir) {
		// TODO Auto-generated method stub
		System.out.println("Encontrado o arquivo de saída dentro de "+outputDir);
		String outputFile = null;
		
		Path p = new Path(outputDir);
        Path aux;
        Configuration c = new Configuration();
         c.set("fs.defaultFS", MainSpark.clusterUrl);
        try{
            FileSystem fs = FileSystem.get(c);

            if(fs.isDirectory(p)){

                 FileStatus[] ff = fs.listStatus(p);
                 System.out.println("Há "+ff.length+" arquivos dentro de "+outputDir);
                 for(FileStatus f: ff){
                     System.out.println(f.getPath().getName());
                     aux = f.getPath();
                     if(aux.getName().startsWith("part")){
                    	 outputFile = aux.getParent()+"/"+aux.getName();
                    	 System.out.println("Encontrado arquivo de saída "+outputFile);
                    	 break;
                     }
                 }

            }else{
                System.out.println("Não é um diretório: "+outputDir);
            }
        }catch(IOException e){
        	e.printStackTrace();
        }
        return outputFile;
	}

	/**
	 * 
	 * @param data
	 * @param fileOut
	 */
	public static void saveSequenceInHDFS(ArrayList<String> data, String fileOut) {
		Path p = new Path(fileOut);
        Configuration c = new Configuration();
        c.set("fs.defaultFS", MainSpark.clusterUrl);
        
        System.out.println("Salvando arquivo de sequência " + fileOut + " com " + data.size() + " elementos...");
		try {
			SequenceFile.Writer writer = SequenceFile.createWriter(c, SequenceFile.Writer.file(p),
			           SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(IntWritable.class));
			Text text = new Text();
			IntWritable value = new IntWritable(1);
			for(String is : data){
				text.set(is);
				writer.append(text, value);
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/*public static void saveItemSetsInHDFS(ArrayList<String, Integer> data, String fileOut) {
		Path p = new Path(fileOut);
        Configuration c = new Configuration();
        c.set("fs.defaultFS", MainSpark.clusterUrl);
        
        System.out.println("Salvando arquivo de sequência " + fileOut + " com " + data.size() + " elementos...");
		try {
			SequenceFile.Writer writer = SequenceFile.createWriter(c, SequenceFile.Writer.file(p),
			           SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(IntWritable.class));
			Text text = new Text();
			IntWritable value = new IntWritable(1);
			for(String is : data){
				text.set(is);
				writer.append(text, value);
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}*/
		
	/**
	 * 
	 * @param fileName
	 * @return
	 */
	public static ArrayList<String> readSequenfileInHDFS(String fileName){
		ArrayList<String> itemsets = new ArrayList<String>();
		Path p = new Path(fileName);
        Configuration c = new Configuration();
         c.set("fs.defaultFS", MainSpark.clusterUrl);
		
		try{
			SequenceFile.Reader reader = new SequenceFile.Reader(c, SequenceFile.Reader.file(p));
			Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), c);
			IntWritable value = (IntWritable) ReflectionUtils.newInstance(reader.getValueClass(), c);
			while (reader.next(key, value)) {
				itemsets.add(key.toString());
	        }
		}catch(IOException e){
			e.printStackTrace();
		}
		return itemsets;
	}

	public static void copyToInputGen(String outputCount) {
		Path path = new Path(outputCount);
		Path input = new Path(MainSpark.inputFileToGen);
		Configuration c = new Configuration();
        c.set("fs.defaultFS", MainSpark.clusterUrl);
        
        try {
        	FileSystem fs = FileSystem.get(c);
        	if (fs.exists(input)){
        		fs.delete(input, true);
        	}
        	if (FileUtil.copyMerge(fs, path, fs, input, false, c, "")) {
        		System.out.println("Arquivos copiados com sucesso!");
        	} else{ 
        		System.out.println("Erro ao copiar os arquivos");
        	}
        } catch(IOException e) {
        	e.printStackTrace();
        }
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
		StringBuilder sb = new StringBuilder("/home/thiago/times/");
		File file = new File(sb.toString());
		if(!file.isDirectory()){
			file.mkdirs();
		}
		sb.append(data.split(" ")[0]).append("-").append(MainSpark.inputFileName).append("_").append(System.currentTimeMillis()).append(".log");
		System.out.println("Saving: "+data+"\n into "+sb.toString());
		saveFileInLocal(data, sb.toString());
	}
	
}
