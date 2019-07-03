package com.jd.jr.recommender.mq.bean;

import com.jd.jr.recommender.mq.PropertyUtil;

import java.io.Serializable;

public class JmqProperties implements Serializable {

    private String app;

    private String address;

    private String user;

    private String password;

    private Integer sendTimeout;

    private String standardizedTrackingTopic;

    private Boolean epoll;

    private JmqProperties() {}

    public static JmqProperties loadProperties() {
        JmqProperties jmqProperties = new JmqProperties();
        jmqProperties.setApp(PropertyUtil.getInstance().getProperty("jmq.app"));
        jmqProperties.setAddress(PropertyUtil.getInstance().getProperty("jmq.address"));
        jmqProperties.setUser(PropertyUtil.getInstance().getProperty("jmq.user"));
        jmqProperties.setPassword(PropertyUtil.getInstance().getProperty("jmq.password"));
        jmqProperties.setSendTimeout(Integer.valueOf(PropertyUtil.getInstance().getProperty("jmq.sendTimeout")));
        jmqProperties.setStandardizedTrackingTopic(PropertyUtil.getInstance().getProperty("jmq.standardizedTrackingTopic"));
        jmqProperties.setEpoll(Boolean.valueOf(PropertyUtil.getInstance().getProperty("jmq.epoll")));
        return jmqProperties;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
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

    public Integer getSendTimeout() {
        return sendTimeout;
    }

    public void setSendTimeout(Integer sendTimeout) {
        this.sendTimeout = sendTimeout;
    }

    public String getStandardizedTrackingTopic() {
        return standardizedTrackingTopic;
    }

    public void setStandardizedTrackingTopic(String standardizedTrackingTopic) {
        this.standardizedTrackingTopic = standardizedTrackingTopic;
    }

    public Boolean getEpoll() {
        return epoll;
    }

    public void setEpoll(Boolean epoll) {
        this.epoll = epoll;
    }
}
