import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;


public class WuzzufDataSet {
    public WuzzufDataSet() {

    }

    public Dataset<Row> dataframe_fromCsv(String filename) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("WuzzufDataSet").master("local[*]").getOrCreate();
        Dataset<Row> df = session.read().option("header", "true").csv(filename);
        return df;
    }

    public void Display_Schema_Summary(Dataset<Row> df) {
        df.printSchema();
        df.summary().show();

    }

    public Dataset<Row> Clean_NullDup(Dataset<Row> df) {
        df = df.na().drop();
        df = df.dropDuplicates();

        return df;
    }

    public Dataset<Row> Count_Jobs_ByCompany(Dataset<Row> df) {

        Dataset<Row> Job_count_per_company = df.select("Company", "Title").groupBy("Company").count().orderBy(df.col("Company"));
        Job_count_per_company = Job_count_per_company.orderBy(org.apache.spark.sql.functions.col("count").desc());
        return Job_count_per_company;
    }

    public Dataset<Row> Count_Jobs_Pop(Dataset<Row> df) {
        Dataset<Row> Job_Counts_per_pop = df.select("Title").groupBy("Title").count().orderBy(df.col("Title"));
        Job_Counts_per_pop = Job_Counts_per_pop.orderBy(org.apache.spark.sql.functions.col("count").desc());
        return Job_Counts_per_pop;
    }

    public Dataset<Row> PopAreas(Dataset<Row> df) {
        Dataset<Row> Pop_Area_Count = df.select("Location").groupBy("Location").count().orderBy(df.col("Location"));
        Pop_Area_Count = Pop_Area_Count.orderBy(org.apache.spark.sql.functions.col("count").desc());
        return Pop_Area_Count;
    }

    public Dataset<Row> SkillSet(Dataset<Row>df){
        return df.select("Skills");


}


}
