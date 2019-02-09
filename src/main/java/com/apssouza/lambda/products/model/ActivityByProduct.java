package com.apssouza.lambda.products.model;

public class ActivityByProduct {

    String product;
    Long timestampHour;
    Long purchaseCount;
    Long addToCartCount;
    Long pageViewCount;

    public ActivityByProduct(String product, Long timestampHour, Long purchaseCount, Long addToCartCount, Long pageViewCount) {
        this.product = product;
        this.timestampHour = timestampHour;
        this.purchaseCount = purchaseCount;
        this.addToCartCount = addToCartCount;
        this.pageViewCount = pageViewCount;
    }

    public String getProduct() {
        return product;
    }

    public Long getTimestampHour() {
        return timestampHour;
    }

    public Long getPurchaseCount() {
        return purchaseCount;
    }

    public Long getAddToCartCount() {
        return addToCartCount;
    }

    public Long getPageViewCount() {
        return pageViewCount;
    }
}
