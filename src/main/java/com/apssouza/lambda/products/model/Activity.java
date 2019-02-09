package com.apssouza.lambda.products.model;


public class Activity {
    Long timestamp_hour;
    String referrer;
    String action;
    String prevPage;
    String visitor;
    String page;
    String product;

    public Activity(Long timestamp_hour, String referrer, String action, String prevPage, String visitor, String page, String product) {
        this.timestamp_hour = timestamp_hour;
        this.referrer = referrer;
        this.action = action;
        this.prevPage = prevPage;
        this.visitor = visitor;
        this.page = page;
        this.product = product;
    }

    public Long getTimestamp_hour() {
        return timestamp_hour;
    }

    public String getReferrer() {
        return referrer;
    }

    public String getAction() {
        return action;
    }

    public String getPrevPage() {
        return prevPage;
    }

    public String getVisitor() {
        return visitor;
    }

    public String getPage() {
        return page;
    }

    public String getProduct() {
        return product;
    }
}
