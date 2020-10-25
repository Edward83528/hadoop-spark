package core;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class p1 {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		String FileName = null;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// 得到文件名
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			FileName = inputSplit.getPath().getName();
			System.out.println("FileName:" + FileName);
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// 把一段文章一行一行讀進來
			String line = value.toString();
			// StringTokenizer建構子可以打空白做切割
			StringTokenizer tokenizer = new StringTokenizer(line);

			// 切割完有存在元素就跑迴圈
			while (tokenizer.hasMoreTokens()) {
				// 單筆元素
				String token = tokenizer.nextToken();
				// 以key value形式,相同的key集合在一起,每個key的value給1
				context.write(new Text(token), new IntWritable(1));
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			// 累計相同單詞的次數
			for (IntWritable val : values) {
				// LongWritable是hadoop資料型別,用get可以轉為一般java資料型別
				sum += val.get();
			}
			// 把計算好的資料往後丟
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {

		// 設定
		Configuration conf = new Configuration();

		Job job = new Job(conf, "myWordCount");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setJarByClass(p1.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// 定義輸入路徑
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// 定義輸出路徑
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}