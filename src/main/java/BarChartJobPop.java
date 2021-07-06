import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.chart.BarChart;
import javafx.scene.chart.CategoryAxis;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.stage.Stage;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

public class BarChartJobPop extends Application {
    @Override
    public void start(Stage stage) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("count").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        WuzzufDataSet dataSet = new WuzzufDataSet();
        Dataset<Row> df = dataSet.dataframe_fromCsv("In/Wuzzuf_Jobs.csv");

        Dataset<Row> JobPop= dataSet.Count_Jobs_Pop(df);
        JavaRDD<Row> JobPopRdd=JobPop.select("Title","Count").javaRDD();
        JavaRDD<String>Title=JobPopRdd.map(row -> row.getString(0));
        JavaRDD<Long>Count=JobPopRdd.map(row -> row.getLong(1));
        List<String> TitleList=Title.collect();
        List<Long>CountList=Count.collect();

        stage.setTitle("Bar Chart Sample");
        final NumberAxis xAxis = new NumberAxis();
        final CategoryAxis yAxis = new CategoryAxis();
        final BarChart<Number, String> bc = new BarChart<>(xAxis, yAxis);
        bc.setTitle("Job Popularity");
        xAxis.setLabel("Number of Jobs");
        xAxis.setTickLabelRotation(90);
        yAxis.setLabel("Title");

        XYChart.Series series1 = new XYChart.Series();
        for (int i =0;i<20;i++){
            series1.getData().add(new XYChart.Data(CountList.get(i),TitleList.get(i) ));

        }
        Scene scene = new Scene(bc, 800, 600);
        bc.getData().addAll(series1);
        stage.setScene(scene);
        stage.show();
    }

    public static void main(String[] args) {
        launch(args);
    }
}