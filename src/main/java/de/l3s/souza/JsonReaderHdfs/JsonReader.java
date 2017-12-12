package de.l3s.souza.JsonReaderHdfs;

import static java.nio.charset.StandardCharsets.*;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ch.qos.logback.core.subst.Token.Type;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import de.l3s.boilerpipe.BoilerpipeExtractor;
import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.CommonExtractors;
import de.l3s.souza.tagMeClient.TagmeAnnotator;

import it.acubelab.tagme.AnnotatedText;
import it.acubelab.tagme.Annotation;
import it.acubelab.tagme.preprocessing.TopicSearcher;


public final class JsonReader extends Configured implements Tool {
	
	private static Configuration conf;
	private static TagmeAnnotator annotation;
	
	 public static class RecordSerializer implements JsonSerializer<Record> {
	        public JsonElement serialize(final Record record, final Type type, final JsonSerializationContext context) {
	            JsonObject result = new JsonObject();
	            result.add("annotations", new JsonPrimitive(record.getAnnotations()));
	            result.add("article", new JsonPrimitive(record.getArticle()));
	           
	            return result;
	        }

			@Override
			public JsonElement serialize(Record src,
					java.lang.reflect.Type typeOfSrc,
					JsonSerializationContext context) {
				// TODO Auto-generated method stub
				return null;
			}
	    }
	 

protected void setup(Context context) throws IOException,
		InterruptedException {
	
	annotation = new TagmeAnnotator ("en");

	}
public static class SampleMapper extends Mapper<Object, Text, NullWritable, Text > { 
	
	
    private final NullWritable outKey = NullWritable.get();
    private static BoilerpipeExtractor extractor;
  
    private static String article;
    private List<Annotation> listAnnotation;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException 
    {
    	extractor = CommonExtractors.ARTICLE_EXTRACTOR;
    	annotation = new TagmeAnnotator ("en");
    }
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		AnnotatedText ann_text = null;
		String str = value.toString();
		String[] str2 = str.split("},", -1);
		String record = str2[0];
		String payload = str2[1];
	
		record = record.substring(13, record.length()-3);
	
		payload = payload.substring(28, payload.length());
		//record = record + ","+payload;
		record = record + "\n}\n";	
		//JsonElement jelement = new JsonParser().parse(record);
	  
		Gson gson = new Gson();

		Record rec = gson.fromJson(record, Record.class);
		
		try {
			article = extractor.getText(payload);
		
		} catch (BoilerpipeProcessingException e) {
			
			return;
		}
		
//		article = "Dilma Rousseff meet Angela Merkel and Barack Obama in Brazil";
		byte ptext[] = article.getBytes(ISO_8859_1); 
        String encoded = new String(ptext, UTF_8); 
		String entitiesDisambiguated = ""; 
       
        try {
			listAnnotation = annotation.getAnnots();
			ann_text = annotation.getAnn_text();
			TopicSearcher searcher = annotation.getSearcher();
		} catch (Exception e) {
			
		}
     
        for (Annotation a : listAnnotation)
        {
        	if (a.getRho() >= 0.3)
        		entitiesDisambiguated = entitiesDisambiguated + "spot: " + ann_text.getOriginalText(a) + "\n";
        }
    
       
        rec.setAnnotations(entitiesDisambiguated);
		 article = article.replaceAll("\n", "");
	     article = article.replaceAll("\t", "");
        rec.setArticle(article);
       
        Gson gsonOutput = new GsonBuilder().registerTypeAdapter(Record.class, new RecordSerializer())
                .create();
		context.write(outKey,new Text (rec.getOriginalUrl() +"\n" + rec.getAnnotations()));
    //    context.write(outKey,new Text (rec.getTimestamp() + "\n" + rec.getArticle()));
		
		}
	
}

@Override
public int run(String[] args) throws Exception {
	
	Path inputPath = new Path(args[0]);
	Path outputDir = new Path(args[1]);

	System.setProperty("hadoop.home.dir", "/");
	// Create configuration
	
	
	Job job = Job.getInstance(getConf());
	job.setJarByClass(JsonReader.class);

	conf = job.getConfiguration();
	conf.set("textinputformat.record.delimiter","}\n}\n");
	// Setup MapReduce
	job.setMapperClass(SampleMapper.class);
	job.setReducerClass(Reducer.class);
	job.setNumReduceTasks(1);

	// Specify key / value
	job.setOutputKeyClass(NullWritable.class);
	job.setOutputValueClass(Text.class);

	// Input
	FileInputFormat.addInputPath(job, inputPath);
	job.setInputFormatClass(TextInputFormat.class);

	// Output
	FileOutputFormat.setOutputPath(job, outputDir);
	job.setOutputFormatClass(TextOutputFormat.class);

	// Delete output if exists
	FileSystem hdfs = FileSystem.get(conf);
	if (hdfs.exists(outputDir))
	hdfs.delete(outputDir, true);

	//FileOutputFormat.setCompressOutput(job, true);
	//FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
	// Execute job
	return job.waitForCompletion(true) ? 0 : 1;
	
}


public static void main(String[] args) throws Exception {

	 int res = ToolRunner.run(conf,new JsonReader(), args);
     System.exit(res);


}


}

