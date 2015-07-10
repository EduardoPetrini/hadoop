/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.mapred.map;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

import main.java.com.mestrado.app.HashNode;
import main.java.com.mestrado.app.HashPrefixTree;
import main.java.com.mestrado.utils.MrUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Gerar itemsets de tamanho 2.
 * 
 * @author eduardo
 */
public class Map2 extends Mapper<LongWritable, Text, Text, Text> {

	private Log log = LogFactory.getLog(Map2.class);
	private SequenceFile.Reader reader;
	private ArrayList<String> blocksIds;
	private String splitName;
	private String inputPartialName;
	private HashMap<String, Integer[]> itemSup;
	private HashPrefixTree hpt;
	private int maxK;
	private Text valueOut;
	private Text keyOut;
	/**
	 * Le o arquivo invertido para a memória.
	 * 
	 * @param context
	 * @throws IOException
	 */
	@Override
	public void setup(Context context) throws IOException {
		log.info("Iniciando Map 2");
		int totalPartitions = Integer.parseInt(context.getConfiguration().get(
				"totalPartitions"));
		inputPartialName = context.getConfiguration().get("outputPartialName");

		blocksIds = new ArrayList<String>();
		for (int i = 1; i <= totalPartitions; i++) {
			blocksIds.add(context.getConfiguration().get("blockId" + i)
					.replace("partition", ""));// Id da partição é o offset do
												// bloco
		}
	}

	public boolean checkPartition() {

		for (String ids : blocksIds) {
			System.out
					.println("Verificando se a partição atual será processada: "
							+ splitName + " == " + ids);
			if (MrUtils.checkPartitions(ids, splitName, 100)) {
				splitName = ids;
				return true;
			}
		}

		return false;
	}

	@Override
	public void map(LongWritable key, Text value, Context context) {
		// key é o offset, id do bloco/partição
		splitName = String.valueOf(key.get());
		System.out.println("Id da partição: " + splitName);
		// Verificar se é uma partição a ser processada

		if (checkPartition()) {
			// A partição atual será processada
			System.out
					.println("A partição atual será processada: " + splitName);
			itemSup = new HashMap<String, Integer[]>();
			hpt = new HashPrefixTree();
			openFile(context);// Ler o arquivo da partição para a prefixTree

			String[] itemset;
			String[] tr;
			int itemsetIndex;
			boolean endBlock = false;
			int pos;
			int start = 0;
			int len;
			valueOut = new Text();
			keyOut = new Text();
			while ((pos = value.find("\n", start)) != -1) {
				len = pos - start;
				try {
					tr = Text.decode(value.getBytes(), start, len).split(" ");
					for (int i = 0; i < tr.length; i++) {
						itemset = new String[maxK];
						itemsetIndex = 0;
						subSet(tr, hpt.getHashNode(), i, itemset, itemsetIndex, context);
					}
				} catch (CharacterCodingException e) {
					e.printStackTrace();
					System.exit(1);
				}
				start = pos + 1;
				if (start >= value.getLength()) {
					// System.out.println("Break... " + value.getLength());
					endBlock = true;
					break;
				}
			}
			
			// pegar a ultima transação, caso tenha
			if (!endBlock) {
				len = value.getLength() - start;
				try {
					tr = Text.decode(value.getBytes(), start, len).split(" ");
					for (int i = 0; i < tr.length; i++) {
						itemset = new String[maxK];
						itemsetIndex = 0;
						subSet(tr, hpt.getHashNode(), i, itemset, itemsetIndex, context);
					}
				} catch (CharacterCodingException e) {
					e.printStackTrace();
					System.exit(1);
				}
			}
		}
	}

	/**
	 * 
	 * @param transaction
	 * @param pt
	 * @param i
	 * @param k
	 * @param itemset
	 * @param itemsetIndex
	 */
	private void subSet(String[] transaction, HashNode hNode, int i,
			String[] itemset, int itemsetIndex, Context context) {
		
		if(i >= transaction.length){
			return;
		}
		
		HashNode son = hNode.getHashNode().get(transaction[i]);
		
		if(son == null){
			return;
		}else{
			itemset[itemsetIndex] = transaction[i];
			
			StringBuilder sb = new StringBuilder();
			for(String item: itemset){
				if(item != null){
					sb.append(item).append(" ");
				}
			}
			addToHashItemSupAndSendToReduce(sb.toString().trim(), context);
			
			// System.out.println("Encontrou: "+sb.toString().trim());
			i++;
			itemsetIndex++;
			while(i < transaction.length){
				subSet(transaction, son, i, itemset, itemsetIndex, context);
				for(int j = itemsetIndex; j < itemset.length; j++){
					itemset[j] = "";
				}
				i++;
			}
		}
	}

	/**
	 * 
	 * @param context
	 */
	public void openFile(Context context) {
		Path path = new Path(inputPartialName + splitName);

		try {
			System.out.println("Lendo a partição '" + path.getName()
					+ "' para a memória!");
			reader = new SequenceFile.Reader(context.getConfiguration(),
					SequenceFile.Reader.file(path));

			Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(),
					context.getConfiguration());
			IntWritable value = (IntWritable) ReflectionUtils.newInstance(
					reader.getValueClass(), context.getConfiguration());
			String k;
			String[] kSpt;
			maxK = 0;
			Integer vHash[];
			while (reader.next(key, value)) {
				// System.out.println("Add Key: "+key.toString());
				k = key.toString();
				kSpt = k.split(" ");
				vHash = new Integer[2];
				vHash[0] = value.get();
				vHash[1] = 0;
				itemSup.put(k, vHash);
				hpt.add(hpt.getHashNode(), kSpt, 0);
				if(kSpt.length > maxK){
					maxK = kSpt.length; 
				}
			}

		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
	}

	public void addToHashItemSupAndSendToReduce(String item, Context context) {
		Integer[] value;
		
		if ((value = itemSup.get(item)) != null) {
			valueOut.set(String.valueOf(value[0])+":1");
			keyOut.set(item);
			
			try {
				context.write(keyOut, valueOut);
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
