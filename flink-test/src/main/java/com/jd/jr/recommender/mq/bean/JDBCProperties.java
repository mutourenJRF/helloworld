package com.jd.jr.recommender.mq.bean;

import com.jd.jr.recommender.mq.PropertyUtil;

import java.io.Serializable;

/**
 * @Auther: qiuyujiang
 * @Date: 2019/5/10.
 * @Description: 请填写
 */
public class JDBCProperties implements Serializable {

    private String driverClass;
    private String url;
    private String user;
    private String password;
    private String sql;

    private JDBCProperties() {}

    public static JDBCProperties loadProperties() {
        final String SQL = "INSERT INTO rd_rec_yxmx_chart_data(type,rec_code,experiment_id,rule_code,target,target_value,dt) VALUES(?,?,?,?,?,?,?)";

        JDBCProperties jdbcProperties = new JDBCProperties();
        jdbcProperties.setDriverClass(PropertyUtil.getInstance().getProperty("mysql.driverClass"));
        jdbcProperties.setUrl(PropertyUtil.getInstance().getProperty("mysql.url"));
        jdbcProperties.setUser(PropertyUtil.getInstance().getProperty("mysql.user"));
        jdbcProperties.setPassword(PropertyUtil.getInstance().getProperty("mysql.password"));
        jdbcProperties.setSql(SQL);
        return jdbcProperties;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
