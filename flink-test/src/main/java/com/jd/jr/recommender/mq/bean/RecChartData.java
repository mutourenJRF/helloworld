package com.jd.jr.recommender.mq.bean;

import java.io.Serializable;

public class RecChartData implements Serializable {

    public final static Integer TYPE_REALTIME = 1;

    private Integer id;

    /**
     * 数据来源（1：数据部、2：风控）
     */
    private Integer type;

    /**
     * 推荐位编码
     */
    private String recCode;

    /**
     * 实验ID
     */
    private Integer experimentId;

    /**
     * 规则集编码
     */
    private String ruleCode;

    /**
     * 指标类型
     * pv 浏览量（曝光量）
     * uv 去重以后的pv
     * pcv 点击量（针对于pv）
     * ucv 点击量（针对于uv）
     * pcvr pcv/pv
     * ucvr ucv/uv
     */
    private String target;

    /**
     * 指标值
     */
    private Double targetValue;

    /**
     * 离线数据日期格式：yyyy-MM-dd
     * 实时数据时间格式：yyyy-MM-dd HH:mm
     */
    private String dt;

    public RecChartData(){}
    public RecChartData(Integer type,String recCode,Integer experimentId,String ruleCode,String target,Double targetValue,String dt){
        this.type=type;
        this.recCode=recCode;
        this.experimentId=experimentId;
        this.ruleCode=ruleCode;
        this.target=target;
        this.targetValue=targetValue;
        this.dt=dt;
    }

    @Override
    public String toString() {
        return "{\"type\":" + this.type + ",\"recCode\":\"" + this.recCode + "\",\"experimentId\":" + this.experimentId + ",\"ruleCode\":\"" + this.ruleCode + "\"," +
                "\"target\":\"" + this.target + "\",\"targetValue\":" + this.targetValue + ",\"dt\":\"" + this.dt + "\"}";
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public String getRecCode() {
        return recCode;
    }

    public void setRecCode(String recCode) {
        this.recCode = recCode;
    }

    public Integer getExperimentId() {
        return experimentId;
    }

    public void setExperimentId(Integer experimentId) {
        this.experimentId = experimentId;
    }

    public String getRuleCode() {
        return ruleCode;
    }

    public void setRuleCode(String ruleCode) {
        this.ruleCode = ruleCode;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public Double getTargetValue() {
        return targetValue;
    }

    public void setTargetValue(Double targetValue) {
        this.targetValue = targetValue;
    }

    public String getDt() {
        return dt;
    }

    public void setDt(String dt) {
        this.dt = dt;
    }
}
