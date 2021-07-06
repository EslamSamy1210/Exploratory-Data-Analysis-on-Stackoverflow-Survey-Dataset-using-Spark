import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import java.util.*;


public class Main {



        public static  void main(String[] args) throws Exception {
            //Configure Spark and set local cores
            Logger.getLogger("org").setLevel(Level.ERROR);
            SparkConf conf = new SparkConf().setAppName("count").setMaster("local[*]");
            JavaSparkContext sc = new JavaSparkContext(conf);
            //Create an instance of the class Wuzzuf data set
            WuzzufDataSet dataSet = new WuzzufDataSet();
            //Create a data set that reads from the Csv File Wuzzuf_Jobs
            Dataset<Row> df = dataSet.dataframe_fromCsv("In/Wuzzuf_Jobs.csv");


            System.out.println("*******************Display Some Of The Data Set**********************");
            df.show(20);


            System.out.println("*******************Print out Schema and Summary**********************");
            dataSet.Display_Schema_Summary(df);


            System.out.println("*******************Print data frame without Null and Duplicates **********************");
            df.summary("count").show();
            df = dataSet.Clean_NullDup(df);
            df.summary("count").show();
            df.show();



            System.out.println("*******************Print the count jobs per company **********************");
            Dataset<Row> JobCount_Perr_Com = dataSet.Count_Jobs_ByCompany(df);
            JobCount_Perr_Com.show(20);


            System.out.println("*******************Print the jobs popularity **********************");
            Dataset<Row> Job_count_pop = dataSet.Count_Jobs_Pop(df);
            Job_count_pop.show(20);


            System.out.println("*******************Print the most popular areas **********************");
            Dataset<Row>Pop_Areas=dataSet.PopAreas(df);
            Pop_Areas.show(20);


            System.out.println("**********************Print the number of Occurences for each skill*********************");
            Dataset<Row>skillCol=df.select("Skills");
            JavaRDD<String> skillColRdd=skillCol.toJavaRDD().map(row -> row.getString(0));
            List<String>skillsLst=  skillColRdd.collect();
            String[] words = skillsLst.stream().flatMap(str -> Arrays.stream(str.split(","))).toArray(String[]::new);
            List<String>skillLst;
            skillLst= Arrays.asList(words);

            Map<String,Integer>filterMap=new HashMap<>();
            ArrayList<String>skill=new ArrayList<>();
            ArrayList<Integer>skillCount=new ArrayList<>();
            for (int i =0;i<skillLst.size();i++)
            {
                if(skill.contains(skillLst.get(i).trim()))
                {
                    continue;
                }
                else{
                    int count=1;
                    skill.add(skillLst.get(i));
                    for (String s : skillLst) {
                        if (skillLst.get(i).toLowerCase(Locale.ROOT).equals(s.trim().toLowerCase(Locale.ROOT))) {
                            count += 1;
                        }
                    }
                    skillCount.add(count);

                }

            }


            for(int i=0;i< skill.size();i++)
            {
                filterMap.put(skill.get(i),skillCount.get(i));
            }

            Map<String,Integer>SkillMap=new HashMap<>();
            filterMap.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                    .forEachOrdered(x -> SkillMap.put(x.getKey(), x.getValue()));
            System.out.println(SkillMap.size());
            SkillMap.forEach((key, value) -> System.out.println(key + ":" + value));

        }


}







