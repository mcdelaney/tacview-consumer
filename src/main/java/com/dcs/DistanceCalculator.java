package com.dcs;

class DistanceCalculator {
    static DistanceComparison compute_distance(DCSObject rec_1, DCSObject rec_2) {
        Double[] p_1 = convert_to_cartesian(rec_1);
        Double[] p_2 = convert_to_cartesian(rec_2);
        Double dist = Math.sqrt((Math.pow((p_2[0] - p_1[0]), 2) + Math.pow((p_2[1] - p_1[1]), 2) + Math.pow((p_2[2] - p_1[2]), 2)));
        return new DistanceComparison(rec_1.id, dist, rec_1.type);
    }

    private static Double[] convert_to_cartesian(DCSObject rec) {
        double x = rec.alt * Math.cos(rec.lat) * Math.sin(rec.lon);
        double y = rec.alt * Math.sin(rec.lat);
        double z = (rec.alt * Math.cos(rec.lat) * Math.cos(rec.lon));
        return new Double[]{x, y, z};
    }

}
