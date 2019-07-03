package com.jd.jr.recommender.mq.bean;

import java.io.Serializable;

public class StandardizedMq implements Serializable {

    private String pin;
    private String timestamp;
    private String type;
    private String recloc;
    private String experimentid;
    private String rule;
    private String id;
    private String uid;
    private String branch;

    @Override
    public String toString() {
        return "{" +
                "pin='" + pin + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", type='" + type + '\'' +
                ", recloc='" + recloc + '\'' +
                ", experimentid='" + experimentid + '\'' +
                ", rule='" + rule + '\'' +
                ", id='" + id + '\'' +
                ", uid='" + uid + '\'' +
                ", branch='" + branch + '\'' +
                '}';
    }

    public String getPin() {
        return pin;
    }

    public void setPin(String pin) {
        this.pin = pin;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getRecloc() {
        return recloc;
    }

    public void setRecloc(String recloc) {
        this.recloc = recloc;
    }

    public String getExperimentid() {
        return experimentid;
    }

    public void setExperimentid(String experimentid) {
        this.experimentid = experimentid;
    }

    public String getRule() {
        return rule;
    }

    public void setRule(String rule) {
        this.rule = rule;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getBranch() {
        return branch;
    }

    public void setBranch(String branch) {
        this.branch = branch;
    }
}
