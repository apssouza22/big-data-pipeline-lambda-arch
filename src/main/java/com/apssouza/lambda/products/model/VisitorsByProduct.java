package com.apssouza.lambda.products.model;


public class VisitorsByProduct {

    String product;
    Long timestampHour;
    Long uniquVisitors;

    public VisitorsByProduct(String product, Long timestampHour, Long uniquVisitors) {
        this.product = product;
        this.timestampHour = timestampHour;
        this.uniquVisitors = uniquVisitors;
    }

    public String getProduct() {
        return product;
    }

    public Long getTimestampHour() {
        return timestampHour;
    }

    public Long getUniquVisitors() {
        return uniquVisitors;
    }
}
