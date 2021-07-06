import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.chart.PieChart;
import javafx.stage.Stage;
import javafx.scene.Group;
import javafx.collections.FXCollections;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

public class pieChart extends Application {

    // launch the application
    public void start(Stage stage) {
        // set title for the stage
        stage.setTitle("Creating Pie Chart");

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("count").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        WuzzufDataSet dataSet = new WuzzufDataSet();
        Dataset<Row> df = dataSet.dataframe_fromCsv("In/Wuzzuf_Jobs.csv");

        Dataset<Row> JobCount_Perr_Com = dataSet.Count_Jobs_ByCompany(df);
        JavaRDD<Row>JobCountPerCom=JobCount_Perr_Com.select("Company","Count").javaRDD();
        JavaRDD<String>company=JobCountPerCom.map(row -> row.getString(0));
        JavaRDD<Long>Count=JobCountPerCom.map(row -> row.getLong(1));
        List<String> ComList=company.collect();
        List<Long>CountList=Count.collect();

        // piechart data
        PieChart.Data[] data = new PieChart.Data[5];

        for (int i = 0; i < 5; i++) {
            data[i] = new PieChart.Data(ComList.get(i), CountList.get(i));
        }

        // create a pie chart
        PieChart pie_chart = new
                PieChart(FXCollections.observableArrayList(data));

        // create a Group
        Group group = new Group(pie_chart);

        // create a scene
        Scene scene = new Scene(group, 800, 600);

        // set the scene
        stage.setScene(scene);

        stage.show();
    }
}