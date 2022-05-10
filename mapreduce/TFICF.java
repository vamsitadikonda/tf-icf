/*
 * Single Author info:
 * btadiko Bapiraju Vamsi Tadikonda
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.*;

/*
 * Main class of the TFICF MapReduce implementation.
 * Author: Tyler Stocksdale
 * Date:   10/18/2017
 */
public class TFICF {

    public static void main(String[] args) throws Exception {
        // Check for correct usage
        if (args.length != 2) {
            System.err.println("Usage: TFICF <input corpus0 dir> <input corpus1 dir>");
            System.exit(1);
        }
		
		// return value of run func
		int ret = 0;
		
		// Create configuration
		Configuration conf0 = new Configuration();
		Configuration conf1 = new Configuration();
		
		// Input and output paths for each job
		Path inputPath0 = new Path(args[0]);
		Path inputPath1 = new Path(args[1]);
        try{
            ret = run(conf0, inputPath0, 0);
        }catch(Exception e){
            e.printStackTrace();
        }
        if(ret == 0){
        	try{
            	run(conf1, inputPath1, 1);
        	}catch(Exception e){
            	e.printStackTrace();
        	}        	
        }
     
     	System.exit(ret);
    }
		
	public static int run(Configuration conf, Path path, int index) throws Exception{
		// Input and output paths for each job

		Path wcInputPath = path;
		Path wcOutputPath = new Path("output" +index + "/WordCount");
		Path dsInputPath = wcOutputPath;
		Path dsOutputPath = new Path("output" + index + "/DocSize");
		Path tficfInputPath = dsOutputPath;
		Path tficfOutputPath = new Path("output" + index + "/TFICF");
		
		// Get/set the number of documents (to be used in the TFICF MapReduce job)
        FileSystem fs = path.getFileSystem(conf);
        FileStatus[] stat = fs.listStatus(path);
		String numDocs = String.valueOf(stat.length);
		conf.set("numDocs", numDocs);
		
		// Delete output paths if they exist
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(wcOutputPath))
			hdfs.delete(wcOutputPath, true);
		if (hdfs.exists(dsOutputPath))
			hdfs.delete(dsOutputPath, true);
		if (hdfs.exists(tficfOutputPath))
			hdfs.delete(tficfOutputPath, true);
		
		// Create and execute Word Count job
			/************ YOUR CODE HERE ************/
		// Creating a job Instance with an appropriate name
		Job wcJob = Job.getInstance(conf, "WordCount");
		// Setting the correct class and Jar file to be used for Mapper and Reducer
		wcJob.setJarByClass(TFICF.class);
		// Seting mapper and reducer
		wcJob.setMapperClass(WCMapper.class);
		wcJob.setReducerClass(WCReducer.class);
		// Setting output key and value class type
		wcJob.setOutputKeyClass(Text.class);
		wcJob.setOutputValueClass(IntWritable.class);
		// Setting input and output paths
		FileInputFormat.addInputPath(wcJob, wcInputPath);
		FileOutputFormat.setOutputPath(wcJob, wcOutputPath);
		// Executing the job and waiting for its completion
		wcJob.waitForCompletion(true);
		// Create and execute Document Size job
			/************ YOUR CODE HERE ************/
		// Creating a job Instance with an appropriate name
		Job dsJob = Job.getInstance(conf, "DocSize");
		// Setting the correct class and Jar file to be used for Mapper and Reducer
		dsJob.setJarByClass(TFICF.class);
		// Setting mapper and reducer
		dsJob.setMapperClass(DSMapper.class);
		dsJob.setReducerClass(DSReducer.class);
		// Setting result key and value classes
		dsJob.setOutputKeyClass(Text.class);
		dsJob.setOutputValueClass(Text.class);
		// Setting input and output paths
		FileInputFormat.addInputPath(dsJob, dsInputPath);
		FileOutputFormat.setOutputPath(dsJob, dsOutputPath);
		// Executing the job and waiting for its completion
		dsJob.waitForCompletion(true);

		//Create and execute TFICF job
			/************ YOUR CODE HERE ************/
		// Creating a job Instance with an appropriate name
		Job tficfJob = Job.getInstance(conf, "TFICF");
		// Setting the correct class and Jar file to be used for Mapper and Reducer
		tficfJob.setJarByClass(TFICF.class);
		// Setting mapper and reducer
		tficfJob.setMapperClass(TFICFMapper.class);
		tficfJob.setReducerClass(TFICFReducer.class);
		// Setting result key and value classes
		tficfJob.setOutputKeyClass(Text.class);
		tficfJob.setOutputValueClass(Text.class);
		// Setting input and output paths
		FileInputFormat.addInputPath(tficfJob, tficfInputPath);
		FileOutputFormat.setOutputPath(tficfJob, tficfOutputPath);
		//Return final job code , e.g. retrun tficfJob.waitForCompletion(true) ? 0 : 1
			/************ YOUR CODE HERE ************/
		int status= tficfJob.waitForCompletion(true) ? 0 : 1;
		fs.close();
		hdfs.close();
		return status;
    }
	
	/*
	 * Creates a (key,value) pair for every word in the document 
	 *
	 * Input:  ( byte offset , contents of one line )
	 * Output: ( (word@document) , 1 )
	 *
	 * word = an individual word in the document
	 * document = the filename of the document
	 */
	public static class WCMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		/************ YOUR CODE HERE ************/
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		/*
		 * Function to count each word in a line of the document into a key,value pair
		 * Referenced from the MapReduce Tutorial : https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
		 */

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			// Fetching the document name from the context
			Path documentPath = ((FileSplit) context.getInputSplit()).getPath();
			String document = documentPath.getName();
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken().toLowerCase();
				token = token.replaceAll("[.,'{}=(!?)\"]", "");
				//If the remaining token starts with a square brackets, the token is ignored.
				if (token.length()>0 && token.charAt(0)=='[' &&token.charAt(token.length()-1)==']')
					continue;
				// If the token is a number, or starts with a number or has a special character as the first character, The token is exempted.
				else if(token.matches("\\b[0-9]+\\b") || token.matches("\\b[0-9]+.*\\b") ||token.matches("[^a-zA-Z\\d\\s]+[a-zA-Z\\d\\s:]*"))
					continue;
				// Ignoring empty tokens
				else if(token.length()>0){
					word.set(String.join("@",token,document));
					context.write(word, one);
				}
			}
		}

	}

    /*
	 * For each identical key (word@document), reduces the values (1) into a sum (wordCount)
	 *
	 * Input:  ( (word@document) , 1 )
	 * Output: ( (word@document) , wordCount )
	 *
	 * wordCount = number of times word appears in document
	 */
	public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		/************ YOUR CODE HERE ************/
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
						   Context context
		) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
    }
	
	/*
	 * Rearranges the (key,value) pairs to have only the document as the key
	 *
	 * Input:  ( (word@document) , wordCount )
	 * Output: ( document , (word=wordCount) )
	 */
	public static class DSMapper extends Mapper<Object, Text, Text, Text> {
		
		/************ YOUR CODE HERE ************/
		private Text mapping = new Text();
		private Text document = new Text();

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			String word;
			String doc;
			String wc;
			// fetching the actual key and value pair from the values. The key is just an object
			String actualKey = value.toString().split("\t")[0];
			String actualValue = value.toString().split("\t")[1];
			// splitting the key to word and document
			String[] temparr = actualKey.split("@", 2);
			word = temparr[0];
			doc = temparr[1];
			// setting the key as document and value as word=wordCount
			mapping.set(word+"="+actualValue);
			document.set(doc);
			context.write(document, mapping);
		}
    }

    /*
	 * For each identical key (document), reduces the values (word=wordCount) into a sum (docSize) 
	 *
	 * Input:  ( document , (word=wordCount) )
	 * Output: ( (word@document) , (wordCount/docSize) )
	 *
	 * docSize = total number of words in the document
	 */
	public static class DSReducer extends Reducer<Text, Text, Text, Text> {
		
		/************ YOUR CODE HERE ************/
		private Text wordLocation = new Text();
		private Text frequency = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int docSize=0;
			String document = key.toString();
			//Creating an arrayList to save the values and reuse them for second iteration
			ArrayList<String> cache = new ArrayList<String>();
			// finding the total word count of a document.
			for (Text val : values) {
				// Saving the value to the cache
				cache.add(val.toString());
				//splitting the value to have word and wordCount in the temparr
				String[] temparr = val.toString().split("=", 2);
				int count =Integer.parseInt(temparr[1]);
				// updating the total word count for document
				docSize = docSize + count;
			}
			// Appending the total word count to each word along with their word count.
			for(String val: cache) {
				String word;
				//splitting the value to have word and wordCount in the temparr
				String[] temparr = val.split("=", 2);
				word = temparr[0];
				// Joining the key to have word and document
				String wordloc = String.join("@", word, document);
				wordLocation.set(wordloc);
				// Setting the value to be wordCount/docSize
				frequency.set(String.join("/",temparr[1],Integer.toString(docSize)));
				context.write(wordLocation, frequency);
			}
		}
	}
	
	/*
	 * Rearranges the (key,value) pairs to have only the word as the key
	 * 
	 * Input:  ( (word@document) , (wordCount/docSize) )
	 * Output: ( word , (document=wordCount/docSize) )
	 */
	public static class TFICFMapper extends Mapper<Object, Text, Text, Text> {

		/************ YOUR CODE HERE ************/
		private Text mapping = new Text();
		private Text word = new Text();

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			String w;
			String doc;
			// fetching the actual key and value pair from the values. The key is just an object
			String actualKey = value.toString().split("\t")[0];
			String actualValue = value.toString().split("\t")[1];
			// fetching the word and document from actualKey
			String[] temparr = actualKey.split("@", 2);
			w = temparr[0];
			doc = temparr[1];
			// setting the key as word & value as document=wordCount/docSize
			mapping.set(doc+"="+actualValue);
			word.set(w);
			context.write(word, mapping);
		}
    }

    /*
	 * For each identical key (word), reduces the values (document=wordCount/docSize) into a 
	 * the final TFICF value (TFICF). Along the way, calculates the total number of documents and 
	 * the number of documents that contain the word.
	 * 
	 * Input:  ( word , (document=wordCount/docSize) )
	 * Output: ( (document@word) , TFICF )
	 *
	 * numDocs = total number of documents
	 * numDocsWithWord = number of documents containing word
	 * TFICF = ln((wordCount/docSize) + 1) * ln(numDocs+ 1/numDocsWithWord +1)
	 *
	 * Note: The output (key,value) pairs are sorted using TreeMap ONLY for grading purposes. For
	 *       extremely large datasets, having a for loop iterate through all the (key,value) pairs 
	 *       is highly inefficient!
	 */
	public static class TFICFReducer extends Reducer<Text, Text, Text, Text> {
		
		private static int numDocs;
		private Map<Text, Text> tficfMap = new HashMap<>();

		// gets the numDocs value and stores it
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			numDocs = Integer.parseInt(conf.get("numDocs"));
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			/************ YOUR CODE HERE ************/
			List<String> cache = new ArrayList<String>();
			// Fetching the word
			String word = key.toString();
			int numDocsWithWord =0;
			double tficf=0.0;
			// Iterating the first time, to count the number of documents with this word
			for (Text val: values){
				// Saving the value to the cache
				cache.add(val.toString());
				numDocsWithWord++;
			}
			// Iterating the second time to calculate the TFICF
			for (String val: cache){
				// Extracting document and wordCount/docSize
				String[] temparr = val.split("=", 2);
				String document = temparr[0];
				// Extracting wordCount and docSize from temparr
				int wordCount = Integer.parseInt(temparr[1].split("/",2)[0]);
				int docSize = Integer.parseInt(temparr[1].split("/",2)[1]);
				String tfi_doc;
				double freq= (double)wordCount / docSize;
				// Calculating TFICF = ln((wordCount/docSize) + 1) * ln(numDocs+ 1/numDocsWithWord +1)
				tficf = Math.log( freq+ 1.0) * Math.log((double)(numDocs+ 1.0)/(numDocsWithWord +1.0));
				//Put the output (key,value) pair into the tficfMap instead of doing a context.write
				tfi_doc = document+"@"+word;
				tficfMap.put(new Text(tfi_doc),new Text(Double.toString(tficf)));
			}

		}
		
		// sorts the output (key,value) pairs that are contained in the tficfMap
		protected void cleanup(Context context) throws IOException, InterruptedException {
            Map<Text, Text> sortedMap = new TreeMap<Text, Text>(tficfMap);
			for (Text key : sortedMap.keySet()) {
                context.write(key, sortedMap.get(key));
            }
        }
		
    }
}
