package com.netcompany.techtalk.batch.web;

import com.netcompany.techtalk.batch.util.StringUtils;

/**Created by Pawel Gawedzki on 9/21/2017.*/
public class QueryColumnBuilder {
    private String select;
    private String from;
    private String limit;

    private QueryColumnBuilder select(String select) {
        this.select = select;
        return this;
    }

    private QueryColumnBuilder from(String from) {
        this.from = from;
        return this;
    }

    private QueryColumnBuilder limit(int limit) {
        this.limit = String.valueOf(limit);
        return this;
    }

    public static QueryColumnBuilder queryForEquipmentCargoColumns(String branch) {
        return new QueryColumnBuilder()
                .select("*")
                .from("\\u0060" + branch + "\\u0060.\\u0060/maersk/advanced_analytics/spotlanes/data_sets/equipment_cargo\\u0060")
                .limit(10);
    }

    public static QueryColumnBuilder queryForOperationalRoutesColumns(String branch) {
        return new QueryColumnBuilder()
                .select("*")
                .from("\\u0060" + branch + "\\u0060.\\u0060/maersk/advanced_analytics/spotlanes/data_sets/operational_routes\\u0060")
                .limit(10);
    }

    public static QueryColumnBuilder queryForHaulageInfoColumns(String branch) {
        return new QueryColumnBuilder()
                .select("*")
                .from("\\u0060" + branch + "\\u0060.\\u0060/maersk/advanced_analytics/spotlanes/data_sets/haulage_info\\u0060")
                .limit(10);
    }

    public static QueryColumnBuilder queryForHaulageEquipmentColumns(String branch) {
        return new QueryColumnBuilder()
                .select("*")
                .from("\\u0060" + branch + "\\u0060.\\u0060/maersk/advanced_analytics/data/datasources/spotlanes/haulage_equipment\\u0060")
                .limit(10);
    }

    public static QueryColumnBuilder queryForGlobalBookingsTruckinglegsColumns(String branch) {
        return new QueryColumnBuilder()
                .select("*")
                .from("\\u0060" + branch + "\\u0060.\\u0060/maersk/advanced_analytics/spotlanes/data_sets/global_bookings_truckinglegs\\u0060")
                .limit(10);
    }

    public static QueryColumnBuilder queryForCargoConditioningColumns(String branch) {
        return new QueryColumnBuilder()
                .select("*")
                .from("\\u0060" + branch + "\\u0060.\\u0060/maersk/advanced_analytics/data/datasources/spotlanes/cargo_conditioning_limit\\u0060")
                .limit(10);
    }

    String build() {
        if (!StringUtils.nullOrEmpty(limit)) {
            limit = "10";
        }
        String selectQuery = "SELECT " + select + " FROM " + from + " LIMIT " + limit;
        return String.format("{\"query\" : \"%s\"}", selectQuery);
    }

    @Override
    public String toString() {
        return this.build();
    }
}
