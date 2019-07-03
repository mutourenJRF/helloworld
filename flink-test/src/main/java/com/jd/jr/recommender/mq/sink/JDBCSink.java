package com.jd.jr.recommender.mq.sink;

import com.jd.jr.recommender.mq.bean.JDBCProperties;
import com.jd.jr.recommender.mq.bean.RecChartData;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class JDBCSink extends RichSinkFunction<RecChartData> {

    private JDBCProperties jdbcProperties;
    private Connection connection;
    private PreparedStatement preparedStatement;

    public JDBCSink(JDBCProperties jdbcProperties) {
        this.jdbcProperties = jdbcProperties;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 加载JDBC驱动
        Class.forName(this.jdbcProperties.getDriverClass());
        // 获取数据库连接
        connection = DriverManager.getConnection(this.jdbcProperties.getUrl(), this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());//写入mysql数据库
        preparedStatement = connection.prepareStatement(this.jdbcProperties.getSql());//insert sql在配置文件中
    }

    @Override
    public void invoke(RecChartData recChartData, Context context) throws Exception {
        try {
            preparedStatement.setInt(1, recChartData.getType());
            preparedStatement.setString(2,recChartData.getRecCode());
            preparedStatement.setInt(3,recChartData.getExperimentId());
            preparedStatement.setString(4,recChartData.getRuleCode());
            preparedStatement.setString(5,recChartData.getTarget());
            preparedStatement.setDouble(6, recChartData.getTargetValue());
            preparedStatement.setString(7, recChartData.getDt());
            preparedStatement.executeUpdate();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(preparedStatement != null){
            preparedStatement.close();
        }
        if(connection != null){
            connection.close();
        }
    }
}
