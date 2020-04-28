// from Blog of Ravi Kiran "MapReduce Tutorial"

import java.io.IOException;
import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Reducer;

@SuppressWarnings("deprecation")
public class KMeans {
    public static String OUT = "outfile";
    public static String IN = "inputlarger";
    public static String CENTROID_FILE_NAME = "/centroid.txt";
    public static String OUTPUT_FILE_NAME = "/part-00000";
    public static String DATA_FILE_NAME = "/computers.csv";
    public static String JOB_NAME = "KMeans";
    public static String SPLITTER = "\\t| ";
    public static List<double[]> mCenters = new ArrayList<double[]>();

    public static class MyPointWritable implements Writable, WritableComparable<MyPointWritable> {
        public double[] points;

        public MyPointWritable() {
            points = new double[7];
        }

        public void set(double[] in) {
            for (int i = 0; i < 7; i++) {
                points[i] = in[i];
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            for (int i = 0; i < 7; i++) {
                points[i] = in.readDouble();
            }
        }

        @Override
        public void write(DataOutput out) throws IOException {
            for (int i = 0; i < 7; i++) {
                out.writeDouble(points[i]);
            }
        }

        @Override
        public String toString() {
            String result = Double.toString(points[0]);
            for (int i = 1; i < 7; i++) {
                result += " " + Double.toString(points[i]);
            }
            return result;
        }

        @Override
        public int compareTo(MyPointWritable other) {
            for (int i = 0; i < this.points.length && i < other.points.length; i++) {
                if (this.points[i] < other.points[i]) {
                    return -1;
                } else if (this.points[i] < other.points[i]) {
                    return 1;
                }
            }
            return 0;
        }
    }

    // public static class MyPointWritable extends ArrayWritable {
    // public MyPointWritable(DoubleWritable[] doubleWritables) {
    // super(DoubleWritable.class);
    // }

    // @Override
    // public DoubleWritable[] get() {
    // return (DoubleWritable[]) super.get();
    // }

    // @Override
    // public String toString() {
    // DoubleWritable[] values = get();
    // String result = "";
    // for (int i = 0; i < values.length; i++) {
    // if (i > 0) {
    // result += " ";
    // }
    // result += values[i].toString();
    // }
    // return result;
    // }

    // @Override
    // public int compareTo(MyPointWritable otherWritable) {
    // DoubleWritable[] me = get();
    // DoubleWritable[] other = otherWritable.get();
    // for (int i = 0; i < me.length && i < other.length; i++) {
    // if (me[i].get() < other[i].get()) {
    // return -1;
    // } else if (me[i].get() < other[i].get()) {
    // return 1;
    // }
    // }
    // if (me.length < other.length) {
    // return -1;
    // } else if (me.length > other.length) {
    // return 1;
    // }
    // return 0;
    // }
    // }

    public static class Map extends MapReduceBase
            implements Mapper<LongWritable, Text, MyPointWritable, MyPointWritable> {
        @Override
        public void configure(JobConf job) {
            try {
                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
                if (cacheFiles != null && cacheFiles.length > 0) {
                    String line;
                    mCenters.clear();
                    BufferedReader cacheReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
                    try {
                        while ((line = cacheReader.readLine()) != null) {
                            String[] temp = line.split(SPLITTER);

                            double[] temp2 = new double[7];
                            for (int i = 0; i < 7; i++) {
                                temp2[i] = Double.parseDouble(temp[i]);
                            }

                            mCenters.add(temp2);
                        }
                    } finally {
                        cacheReader.close();
                    }
                }
            } catch (IOException e) {
                System.err.println("Exception reading DistribtuedCache: " + e);
            }
        }

        @Override
        public void map(LongWritable key, Text value, OutputCollector<MyPointWritable, MyPointWritable> output,
                Reporter reporter) throws IOException {
            String line = value.toString();
            if (line.equals("")) {
                return;
            }
            String[] data = line.split(",");
            if (data[0].equals("") || data[0].equals("\"\"")) {
                return;
            }
            for (int i = 0; i < data.length; i++) {
                if (data[i].charAt(0) == '"' && data[i].charAt(data[i].length() - 1) == '"') {
                    data[i] = data[i].substring(1, data[i].length() - 1);
                }
            }

            double[] point = new double[7];
            point[0] = (Double.parseDouble(data[1]) - 949) / (5399 - 949); // price
            point[1] = (Double.parseDouble(data[2]) - 25) / (100 - 25); // speed
            point[2] = (Double.parseDouble(data[3]) - 80) / (2100 - 80); // hd
            point[3] = (Double.parseDouble(data[4]) - 2) / (32 - 2); // ram
            point[4] = (Double.parseDouble(data[5]) - 14) / (17 - 14); // screen
            point[5] = (Double.parseDouble(data[9]) - 39) / (339 - 39); // ads
            point[6] = (Double.parseDouble(data[10]) - 1) / (35 - 1); // trend

            double min1, min2 = Double.MAX_VALUE;
            double[] nearest_center = mCenters.get(0);
            for (double[] c : mCenters) {
                min1 = 0;
                for (int i = 0; i < 7; i++) {
                    min1 += Math.pow(c[i] - point[i], 2);
                }
                min1 = Math.sqrt(min1);

                if (Math.abs(min1) < Math.abs(min2)) {
                    nearest_center = c;
                    min2 = min1;
                }
            }
            // DoubleWritable[] nearest_center_temp = new DoubleWritable[7];
            // for (int i = 0; i < 7; i++) {
            // nearest_center_temp[i] = new DoubleWritable(nearest_center[i]);
            // }
            // DoubleWritable[] point_temp = new DoubleWritable[7];
            // for (int i = 0; i < 7; i++) {
            // point_temp[i] = new DoubleWritable(point[i]);
            // }
            // output.collect(new MyPointWritable(nearest_center_temp), new
            // MyPointWritable(point_temp));
            MyPointWritable nearest_center_temp = new MyPointWritable();
            nearest_center_temp.set(nearest_center);
            MyPointWritable point_temp = new MyPointWritable();
            point_temp.set(point);
            output.collect(nearest_center_temp, point_temp);
        }
    }

    public static class Reduce extends MapReduceBase
            implements Reducer<MyPointWritable, MyPointWritable, MyPointWritable, Text> {
        @Override
        public void reduce(MyPointWritable key, Iterator<MyPointWritable> values,
                OutputCollector<MyPointWritable, Text> output, Reporter reporter) throws IOException {
            double[] newCenter = new double[7];
            double[] sum = new double[7];
            for (int i = 0; i < 7; i++) {
                sum[i] = 0;
            }
            int no_elements = 0;
            String points = "";
            while (values.hasNext()) {
                MyPointWritable temp = values.next();
                points += points + "| ";
                for (int i = 0; i < 7; i++) {
                    points += Double.toString(temp.points[i]) + " ";
                    sum[i] += temp.points[i];
                }
                ++no_elements;
            }
            // DoubleWritable[] newCenter = new DoubleWritable[7];
            // for (int i = 0; i < 7; i++) {
            // newCenter[i] = new DoubleWritable(sum[i] / no_elements);
            // }
            MyPointWritable newCenter_temp = new MyPointWritable();
            newCenter_temp.set(newCenter);
            output.collect(newCenter_temp, new Text(points));
        }
    }

    public static void main(String[] args) throws Exception {
        run(args);
    }

    public static void run(String[] args) throws Exception {
        IN = args[0];
        OUT = args[1];
        String input = IN;
        String output = OUT + System.nanoTime();
        String again_input = output;
        int iteration = 0;
        boolean isdone = false;
        while (isdone == false) {
            JobConf conf = new JobConf(KMeans.class);
            if (iteration == 0) {
                Path hdfsPath = new Path(input + CENTROID_FILE_NAME);
                DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
            } else {
                Path hdfsPath = new Path(again_input + OUTPUT_FILE_NAME);
                DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
            }
            conf.setJobName(JOB_NAME);
            conf.setMapOutputKeyClass(MyPointWritable.class);
            conf.setMapOutputValueClass(MyPointWritable.class);
            conf.setOutputKeyClass(MyPointWritable.class);
            conf.setOutputValueClass(Text.class);
            conf.setMapperClass(Map.class);
            conf.setReducerClass(Reduce.class);
            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);
            FileInputFormat.setInputPaths(conf, new Path(input + DATA_FILE_NAME));
            FileOutputFormat.setOutputPath(conf, new Path(output));
            JobClient.runJob(conf);
            Path ofile = new Path(output + OUTPUT_FILE_NAME);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(ofile)));
            List<Double> centers_next = new ArrayList<Double>();
            String line = br.readLine();
            while (line != null) {
                String[] sp = line.split(SPLITTER);
                double c = Double.parseDouble(sp[0]);
                centers_next.add(c);
                line = br.readLine();
            }
            br.close();
            String prev;
            if (iteration == 0) {
                prev = input + CENTROID_FILE_NAME;
            } else {
                prev = again_input + OUTPUT_FILE_NAME;
            }
            Path prevfile = new Path(prev);
            FileSystem fs1 = FileSystem.get(new Configuration());
            BufferedReader br1 = new BufferedReader(new InputStreamReader(fs1.open(prevfile)));
            List<Double> centers_prev = new ArrayList<Double>();
            String l = br1.readLine();
            while (l != null) {
                String[] sp1 = l.split(SPLITTER);
                double d = Double.parseDouble(sp1[0]);
                centers_prev.add(d);
                l = br1.readLine();
            }
            br1.close();
            Collections.sort(centers_next);
            Collections.sort(centers_prev);

            Iterator<Double> it = centers_prev.iterator();
            for (double d : centers_next) {
                double temp = it.next();
                if (Math.abs(temp - d) <= 0.1) {
                    isdone = true;
                } else {
                    isdone = false;
                    break;
                }
            }
            ++iteration;
            again_input = output;
            output = OUT + System.nanoTime();
        }
    }
}
