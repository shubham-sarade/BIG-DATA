import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.StringTokenizer;
import java.io.IOException;

public class Marks{

	public static class MarksMapper extends Mapper<Object,Text,Text,IntWritable>{
	
	public void map(Object key,Text value, Context context) 
	throws IOException, InterruptedException
	
	{
		String line = value.toString();
		StringTokenizer st =  new StringTokenizer(line,",");
		String name = st.nextToken();
		int marks = Integer.parseInt(st.nextToken());
		String cls = st.nextToken();
		if(marks >=60)
			context.write(new Text("Marks"), new IntWritable(marks));

	}
    
	
}
public static class MarksReducer extends Reducer<Text,IntWritable, Text, FloatWritable>{

	public void reduce (Text key,
			    Iterable<IntWritable> values ,
			    Context context)
		throws IOException, InterruptedException{
		
		FloatWritable avg = new FloatWritable();
		int sum =0;
		int total =0;
		for(IntWritable num : values){
			sum = sum + num.get();
			total++;
		}
		
		avg.set((float)sum/total);
		context.write(new Text("Average Marks:"),avg);

}

}

public static void main(String args[])throws Exception{
	
	//create object of Confuguration class
	Configuration conf = new Configuration();
	
	//create object of job class
	Job job = new Job(conf,"Marks");
	
	//set the data type of output key
	job.setOutputKeyClass(Text.class);
	
	//set the data type of output value
	job.setOutputValueClass(FloatWritable.class);
	
	//set the data format of input
	job.setInputFormatClass(TextInputFormat.class);
	
	job.setMapOutputKeyClass(Text.class);
	
	job.setMapOutputValueClass(IntWritable.class);
	
	
	//set the name of Mapper class
	job.setMapperClass(MarksMapper.class);
	
	//set the name of Reducer class
	job.setReducerClass(MarksReducer.class);
	
	//set the input file from 0th argument
	FileInputFormat.addInputPath(job, new Path(args[0]));
	
	//set the input file from 1tht argument
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	//Excute the job and wait for the completion
	job.waitForCompletion(true);

}


}
