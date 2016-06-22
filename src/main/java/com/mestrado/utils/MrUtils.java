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

	public boolean increseMapTask(Path file, Configuration c) {
		try {
			FileSystem fs = FileSystem.get(c);

			FileStatus f = fs.getFileStatus(file);

			long blockSize = f.getBlockSize();
			long fileSize = f.getLen();

			if ((4 * blockSize) >= fileSize) {
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

			if (fs.isDirectory(p)) {

				if (fs.delete(p, true)) {
					log.info("Excluido diretório -> " + p.getName());
				} else {
					log.info("Nao foi possivel excluir " + p.getName());
				}
			} else {
				log.info(p.getParent() + "" + p.getName() + " Nao eh um diretorio.");

				if (fs.isFile(p)) {
					log.info("Eh um arquivo, excluindo...");

					if (fs.delete(p, true)) {
						log.info("Excluindo arquivo -> " + p.getName());
					} else {
						log.info("Nao foi possivel excluir o arquivo -> " + p.getName());
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

			if (fs.isDirectory(p)) {

				FileStatus[] ff = fs.listStatus(p);

				for (FileStatus f : ff) {
					aux = f.getPath();

					if (aux.getName().contains("output")) {

						if (fs.delete(aux, true)) {
							System.out.println("Excluido diretório -> " + aux.getName());

						} else {
							System.out.println("Nao foi possivel excluir " + aux.getName());
						}
					}
				}

			}

		} catch (IOException ex) {
			Logger.getLogger(MrUtils.class.getName()).log(Level.SEVERE, null, ex);
		}

	}

	public void createTempDir(String d) {
		Configuration c = new Configuration();
		c.set("fs.defaultFS", Main.clusterUrl);
		try {
			FileSystem fs = FileSystem.get(c);

			if (fs.mkdirs(new Path(d))) {
				log.info("Diretorio " + d + " criado com sucesso.");
			} else {
				log.info("Nao foi possivel criar o diretorio: " + d);
			}

		} catch (IOException ex) {
			Logger.getLogger(Reduce1.class.getName()).log(Level.SEVERE, null, ex);
		}

	}

	public void delContentFiles(String dir) {
		Path p = new Path(dir);
		Configuration c = new Configuration();
		c.set("fs.defaultFS", Main.clusterUrl);
		try {
			FileSystem fs = FileSystem.get(c);

			if (fs.isDirectory(p)) {

				log.info(p.getName() + " eh um diretorio!");

				FileStatus[] f = fs.listStatus(p);
				log.info("Conteudo do diretorio: ");

				for (FileStatus ff : f) {

					log.info(ff.getPath().getName());

					if (ff.toString().contains("confIn"))
						continue;

					p = ff.getPath();

					if (fs.isFile(p)) {
						log.info("Deletando: " + p.getName());
						if (fs.delete(p, true)) {

							log.info("Deletado!");
						} else {
							log.info("Falha ao deletar.");
						}
					}
				}
			}

		} catch (IOException ex) {
			Logger.getLogger(Reduce1.class.getName()).log(Level.SEVERE, null, ex);
		}

	}

	public boolean checkOutput(String dir) {

		Path p = new Path(dir);
		Path aux;
		Configuration c = new Configuration();
		c.set("fs.defaultFS", Main.clusterUrl);
		System.out.println("Verificando diretório: " + dir);

		try {
			FileSystem fs = FileSystem.get(c);

			if (fs.isDirectory(p)) {

				FileStatus[] ff = fs.listStatus(p);

				for (FileStatus f : ff) {

					aux = f.getPath();
					if (aux.getName().startsWith("part")) {
						System.out.println("Arquivos dentro do dir: " + aux.getName() + " " + f.getLen() / 1024 + "Kb ou " + f.getLen() + " bytes");

						if (f.getLen() > 0) {
							return true;

						} else {
							return false;
						}
					}
				}

			} else {
				System.out.println("Não é um diretório: " + dir);
				return false;
			}
		} catch (IOException e) {
			System.out.println("ERROR: " + e);
		}
		System.out.println("Não contém part: " + dir);
		return false;
	}

	/**
	 * 
	 * @return false if empty file
	 */
	public static boolean checkOutputMR() {
		String dir = Main.fileCachedDir;
		Path p = new Path(dir);

		Configuration c = new Configuration();
		c.set("fs.defaultFS", Main.clusterUrl);
		System.out.println("Verificando diretório: " + dir);

		try {
			FileSystem fs = FileSystem.get(c);
			FileStatus[] files = fs.listStatus(p);
			Main.seqFilesNames = new ArrayList<String>();
			for (FileStatus fst : files) {
				if (fst.getPath().getName().startsWith("outputMR" + Main.countDir)) {
					if (fst.getLen() > 128) {
						System.out.println("O arquivo " + fst.getPath().getName() + " não é vazio! " + fst.getLen());
						Main.seqFilesNames.add(Main.fileCachedDir + fst.getPath().getName());
					} else {
						System.out.println("O arquivo " + Main.fileCachedDir + fst.getPath().getName() + " é vazio! " + fst.getLen());
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (Main.seqFilesNames.size() > 0) {
			return true;
		}
		return false;
	}

	public static ArrayList<String> getAllSequenceFilesNames(String dir, int index) {
		ArrayList<String> seqNames = new ArrayList<String>();

		Path p = new Path(dir);
		Path aux;
		Configuration c = new Configuration();
		c.set("fs.defaultFS", Main.clusterUrl);
		try {
			FileSystem fs = FileSystem.get(c);
			FileStatus[] ff = fs.listStatus(p);

			for (FileStatus f : ff) {
				aux = f.getPath();
				if (aux.getName().contains("outputMR" + index)) {
					seqNames.add(dir + "/" + aux.getName());
				}
			}

		} catch (IOException e) {
			System.out.println("ERROR: " + e);
		}
		return seqNames;
	}

	public static ArrayList<String> getAllOuputFilesNames(String outName) {
		ArrayList<String> candNames = new ArrayList<String>();

		Path p = new Path(outName);
		Path aux;
		Configuration c = new Configuration();
		c.set("fs.defaultFS", Main.clusterUrl);
		try {
			FileSystem fs = FileSystem.get(c);
			if (fs.exists(p)) {
				FileStatus[] ff = fs.listStatus(p);

				for (FileStatus f : ff) {
					aux = f.getPath();
					if (aux.getName().startsWith("part")) {
						candNames.add(outName + "/" + aux.getName());
					}
				}
			}
		} catch (IOException e) {
			System.out.println("ERROR: " + e);
		}
		return candNames;
	}

	/**
	 * Calcula a quantidade de transações do arquivo de entrada Calcula o
	 * suporte
	 */
	public static void initialConfig(String[] args) {

		for (String s : args) {
			System.out.println("Args: " + s);
		}
		if (args.length != 0) {
			Main.supportRate = Double.parseDouble(args[0]);
			if (args.length == 2) {
				Main.NUM_REDUCES = Integer.parseInt(args[1]);
			} else if (args.length == 3) {
				Main.NUM_REDUCES = Integer.parseInt(args[1]);
				Main.NUM_BLOCK = args[2];
			} else if (args.length == 4) {
				Main.NUM_REDUCES = Integer.parseInt(args[1]);
				Main.NUM_BLOCK = args[2];
				Main.inputFileName = Main.user + Main.inputEntry + args[3];
			}
		}

		Configuration c = new Configuration();
		c.set("fs.defaultFS", Main.clusterUrl);
		String inputPathUri = Main.user + Main.inputEntry;
		Path inputPath = null;

		if (Main.inputFileName == null || Main.inputFileName == "") {
			inputPath = new Path(inputPathUri);
			Main.inputFileName = Main.user +  Main.inputEntry;
		} else {
			inputPath = new Path(Main.inputFileName);
		}

		try	{

			FileSystem fs = inputPath.getFileSystem(c);
			FileStatus[] subFiles = fs.listStatus(inputPath);
			FileStatus conf = subFiles[0];
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(conf.getPath())));
			Main.totalTransactionCount = 0;
			while (br.readLine() != null) {
				Main.totalTransactionCount++;
			}
			Main.support = String.valueOf(Main.totalTransactionCount * Main.supportRate);

			// Path outputCached = new Path(Main.user+"outputCached/");
			// if(!fs.exists(outputCached)){
			// fs.create(outputCached);
			// }else{
			// for(FileStatus fss: fs.listStatus(outputCached)){
			// fs.delete(fss.getPath(), true);
			// }
			// }

		} catch (

		IOException e)

		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * 
	 * @param pathName
	 */
	public static void createIfNotExistOrClean(String pathName) {
		Path path = new Path(pathName.substring(0, pathName.length() - 9));
		Configuration c = new Configuration();
		c.set("fs.defaultFS", Main.clusterUrl);

		try {
			FileSystem fs = FileSystem.get(c);

			if (fs.exists(path)) {
				FileStatus[] fileStatus = fs.listStatus(path);

				for (FileStatus individualFileStatus : fileStatus) {
					fs.delete(individualFileStatus.getPath(), true);
				}
			} else {
				fs.mkdirs(path);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Obtém as partições criadas na fase 1
	 * 
	 * @param pathName
	 * @return
	 */
	public static ArrayList<String> getPartitions(String pathName) {
		ArrayList<String> partitions = new ArrayList<String>();

		Path path = new Path(pathName.substring(0, pathName.length() - 9));
		Configuration c = new Configuration();
		c.set("fs.defaultFS", Main.clusterUrl);

		try {
			FileSystem fs = FileSystem.get(c);

			if (fs.exists(path)) {
				FileStatus[] fileStatus = fs.listStatus(path);

				for (FileStatus individualFileStatus : fileStatus) {
					long len = individualFileStatus.getLen();
					if (len > 128) {
						partitions.add(individualFileStatus.getPath().getName());
					}
				}
			} else {
				fs.mkdirs(path);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		return partitions;
	}

	/**
	 * public static String user = "/user/hdp/"; public static String inputEntry
	 * = "input/T2.5I2D10N1500K.dobro"; public static String clusterUrl =
	 * "hdfs://master/"; public static long totalTransactionCount; public
	 * ArrayList<String> blocksIds; private String outputPartialName =
	 * user+"partitions-fase-1/partition";
	 * 
	 * @param m
	 */
	public static void printConfigs(Main m) {
		System.out.println("\n******************************************************\n");
		System.out.println("AprioriDPC");
		System.out.println("Arquivo de entrada: " + Main.inputFileName);
		System.out.println("Count: " + Main.countDir);
		System.out.println("Support rate: " + Main.supportRate);
		System.out.println("Support percentage: " + (Main.supportRate * 100) + "%");
		System.out.println("Support: " + Main.support);
		System.out.println("User dir: " + Main.user);
		System.out.println("Entry file: " + Main.inputEntry);
		System.out.println("Cluster url: " + Main.clusterUrl);
		System.out.println("Reduces: " + Main.NUM_REDUCES);
		System.out.println("Blocks: " + Main.NUM_BLOCK);

		System.out.println("\n******************************************************\n");
	}

	public static int getK() {
		ArrayList<String> inputPathsUri = getAllSequenceFilesNames(Main.fileCachedDir, Main.countDir);

		Configuration c = new Configuration();
		c.set("fs.defaultFS", Main.clusterUrl);
		Path inputPath;
		FileSystem fs;
		SequenceFile.Reader reader;
		for (String inputPathUri : inputPathsUri) {

			inputPath = new Path(inputPathUri);
			try {

				fs = inputPath.getFileSystem(c);
				reader = new SequenceFile.Reader(c, SequenceFile.Reader.file(inputPath));

				Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), c);
				IntWritable value = (IntWritable) ReflectionUtils.newInstance(reader.getValueClass(), c);
				while (reader.next(key, value)) {
					System.out.println("Key: " + key.toString());
					return key.toString().split(" ").length;
				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return -1;
	}

	/**
	 * 
	 * @param data
	 * @param fileName
	 */
	public static void saveFileInLocal(String data, String fileName) {
		File file = new File(fileName);

		FileWriter fw;
		BufferedWriter bw;
		try {
			fw = new FileWriter(file.getAbsoluteFile(), true);
			bw = new BufferedWriter(fw);
			bw.write(data);
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param dirName
	 * @return
	 */
	public static ArrayList<String> readAllFromHDFSDir(String dirName) {
		Path inputPath = new Path(dirName);
		Configuration c = new Configuration();
		c.set("fs.defaultFS", Main.clusterUrl);
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
						data.add(line.split("\\t")[0].trim());
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
	 * @param fileName
	 * @return
	 */
	public static ArrayList<String> readSequenfileInHDFS(String fileName) {
		ArrayList<String> itemsets = new ArrayList<String>();
		Path p = new Path(fileName);
		Configuration c = new Configuration();
		c.set("fs.defaultFS", Main.clusterUrl);

		try {
			SequenceFile.Reader reader = new SequenceFile.Reader(c, SequenceFile.Reader.file(p));
			Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), c);
			IntWritable value = (IntWritable) ReflectionUtils.newInstance(reader.getValueClass(), c);
			while (reader.next(key, value)) {
				itemsets.add(key.toString());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return itemsets;
	}

	private static ArrayList<String> readAllSequenceFromMatchName(String cCurrent) {
		Path p = new Path(Main.fileCachedDir);
		Configuration c = new Configuration();
		c.set("fs.defaultFS", Main.clusterUrl);
		ArrayList<String> itemsets = new ArrayList<String>();
		try {
			FileSystem fs = FileSystem.get(c);
			if (fs.isDirectory(p)) {
				FileStatus[] fss = fs.listStatus(p);
				for (FileStatus status : fss) {
					if (status.getPath().getName().contains(cCurrent)) {
						System.out.println("Lendo itemsets from sequence file \"" + Main.fileCachedDir + "/" + status.getPath().getName() + "\"");
						itemsets.addAll(readSequenfileInHDFS(Main.fileCachedDir + "/" + status.getPath().getName()));
					}

				}
			} else {
				System.out.println("\"" + p.getName() + "\" não é um diretório!");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return itemsets;
	}

	/**
	 * 
	 * @param data
	 */
	public static void saveTimeLog(String data, String[] inputFileName) {
		StringBuilder sb = new StringBuilder("/home/hadoop/petrini/times/");
		File file = new File(sb.toString());
		if (!file.isDirectory()) {
			file.mkdirs();
		}
		sb.append("AprioriDPC").append("-").append(inputFileName[inputFileName.length - 1]).append("-").append(Main.supportRate).append("-").append(Main.NUM_REDUCES).append("-")
				.append(Main.NUM_BLOCK).append(".log");
		System.out.println("Saving: " + data + "\n into " + sb.toString());
		saveFileInLocal(data, sb.toString());
	}

	public static String countItemsetInOutput(String output) {
//		if (output.contains("outputMR" + Main.countDir)) {
//			return String.valueOf(readAllSequenceFromMatchName(output).size());
//		} else {
//			return String.valueOf(readAllFromHDFSDir(output).size());
//		}
		return "So many...";
	}
}
