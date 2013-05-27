import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MultipleOutputsDemo extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MultipleOutputsDemo(), args);
		System.out.println("退出代码" + exitCode);
	}

	public int run(String[] args) throws IOException {
		if (args.length != 2) {
			System.err.println("需要两个参数：输入目录名和输出目录名");
			System.err.println("实际参数数量：" + args.length);
			for (String arg : args) {
				System.err.print(arg + " ");
			}
			System.err.println();
			return -1;
		}

		JobConf conf = new JobConf(getConf(), getClass());
		conf.setJobName("MultipleOutputs 演示");

		FileSystem fs = FileSystem.get(URI.create("file:///"), conf);

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.setMapperClass(DemoMapper.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setReducerClass(DemoReducer.class);
		/*
		 * conf.setOutputKeyClass(NullWritable.class);
		 * conf.setOutputValueClass(Text.class);
		 */

		MultipleOutputs.addMultiNamedOutput(conf, "BIP",
				TextOutputFormat.class, NullWritable.class, Text.class);
		JobClient.runJob(conf);
		return 0;
	}

	static class DemoMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString().trim();
			String[] items = line.split("\t");
			output.collect(new Text(items[0]), new Text(line));
		}

	}

	static class DemoReducer extends MapReduceBase implements
			Reducer<Text, Text, NullWritable, Text> {
		private MultipleOutputs multipleOutputs;

		public void configure(JobConf conf) {
			multipleOutputs = new MultipleOutputs(conf);
		}

		public void close() throws IOException {
			multipleOutputs.close();
		}

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<NullWritable, Text> output, Reporter reporter)
				throws IOException {
			OutputCollector<NullWritable, Text> collector = multipleOutputs.getCollector("BIP", key
					.toString().substring(key.getLength() - 3), reporter);
			while (values.hasNext()) {
				/** 使用NullWritable，丢弃掉key */
				String line = values.next().toString();
				String[] items = line.split("\t");
				if (items.length == 2) {
					collector.collect(NullWritable.get(), new Text(line));
				}
			}
		}

	}
}
