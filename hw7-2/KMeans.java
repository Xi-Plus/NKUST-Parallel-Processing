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
    public static List<MyPointWritable> mCenters = new ArrayList<MyPointWritable>();

    public static class MyPointWritable
            implements Writable, WritableComparable<MyPointWritable>, Comparator<MyPointWritable> {
        public int id;
        public double[] points;

        public MyPointWritable() {
            id = 0;
            points = new double[] { 0, 0, 0, 0, 0, 0, 0 };
        }

        public void set(double[] in) {
            for (int i = 0; i < 7; i++) {
                points[i] = in[i];
            }
        }

        public double getDis(MyPointWritable other) {
            double result = 0;
            for (int i = 0; i < 7; i++) {
                result += Math.pow(this.points[i] - other.points[i], 2);
            }
            return Math.sqrt(result);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            id = in.readInt();
            for (int i = 0; i < 7; i++) {
                points[i] = in.readDouble();
            }
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(id);
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
            for (int i = 0; i < 7; i++) {
                if (Math.abs(this.points[i] - other.points[i]) < 0.0001) {
                    continue;
                }
                if (this.points[i] < other.points[i]) {
                    return -1;
                }
                return 1;
            }
            return 0;
        }

        @Override
        public int compare(MyPointWritable a, MyPointWritable b) {
            return a.compareTo(b);
        }
    }

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

                            MyPointWritable temp2 = new MyPointWritable();
                            for (int i = 0; i < 7; i++) {
                                temp2.points[i] = Double.parseDouble(temp[i]);
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

            MyPointWritable point = new MyPointWritable();
            point.id = Integer.parseInt(data[0]);
            point.points[0] = (Double.parseDouble(data[1]) - 949) / (5399 - 949); // price
            point.points[1] = (Double.parseDouble(data[2]) - 25) / (100 - 25); // speed
            point.points[2] = (Double.parseDouble(data[3]) - 80) / (2100 - 80); // hd
            point.points[3] = (Double.parseDouble(data[4]) - 2) / (32 - 2); // ram
            point.points[4] = (Double.parseDouble(data[5]) - 14) / (17 - 14); // screen
            point.points[5] = (Double.parseDouble(data[9]) - 39) / (339 - 39); // ads
            point.points[6] = (Double.parseDouble(data[10]) - 1) / (35 - 1); // trend

            double min1, min2 = Double.MAX_VALUE;
            MyPointWritable nearest_center = mCenters.get(0);
            for (MyPointWritable c : mCenters) {
                min1 = c.getDis(point);

                if (Math.abs(min1) < Math.abs(min2)) {
                    nearest_center = c;
                    min2 = min1;
                }
            }
            output.collect(nearest_center, point);
        }
    }

    public static class Reduce extends MapReduceBase
            implements Reducer<MyPointWritable, MyPointWritable, MyPointWritable, Text> {
        @Override
        public void reduce(MyPointWritable key, Iterator<MyPointWritable> values,
                OutputCollector<MyPointWritable, Text> output, Reporter reporter) throws IOException {
            MyPointWritable newCenter = new MyPointWritable();
            int no_elements = 0;
            String points = "";
            while (values.hasNext()) {
                MyPointWritable temp = values.next();
                points += " ";
                points += temp.id;
                // points += temp.toString();
                for (int i = 0; i < 7; i++) {
                    newCenter.points[i] += temp.points[i];
                }
                ++no_elements;
            }
            for (int i = 0; i < 7; i++) {
                newCenter.points[i] /= no_elements;
            }
            output.collect(newCenter, new Text(points));
        }
    }

    public static void main(String[] args) throws Exception {
        run(args);
    }

    public static void run(String[] args) throws Exception {
        IN = args[0];
        OUT = args[1];
        int iteration = 0;
        String input = IN;
        String output = OUT + iteration;
        String again_input = output;
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
            List<MyPointWritable> centers_next = new ArrayList<MyPointWritable>();
            String line = br.readLine();
            while (line != null) {
                String[] sp = line.split(SPLITTER);
                MyPointWritable c = new MyPointWritable();
                for (int i = 0; i < 7; i++) {
                    c.points[i] = Double.parseDouble(sp[i]);
                }
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
            List<MyPointWritable> centers_prev = new ArrayList<MyPointWritable>();
            String l = br1.readLine();
            while (l != null) {
                String[] sp1 = l.split(SPLITTER);
                MyPointWritable d = new MyPointWritable();
                for (int i = 0; i < 7; i++) {
                    d.points[i] = Double.parseDouble(sp1[i]);
                }
                centers_prev.add(d);
                l = br1.readLine();
            }
            br1.close();
            Collections.sort(centers_next);
            Collections.sort(centers_prev);

            Iterator<MyPointWritable> it = centers_prev.iterator();
            isdone = true;
            for (MyPointWritable d : centers_next) {
                MyPointWritable temp = it.next();
                for (int i = 0; i < 7; i++) {
                    if (Math.abs(temp.points[i] - d.points[i]) > 0.1) {
                        isdone = false;
                        break;
                    }
                }
                if (!isdone) {
                    break;
                }
            }
            ++iteration;
            again_input = output;
            output = OUT + iteration;
        }
    }
}
