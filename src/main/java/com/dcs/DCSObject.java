package com.dcs;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;


class CoordKeys {
    String[] keys = new String[]{"lon", "lat", "alt", "roll", "pitch", "yaw", "u_coord",
            "v_coord", "heading"};
}

class Impactor {
    static String id;
    static Double dist;

    public Impactor(String id, Double dist) {
        Impactor.id = id;
        Impactor.dist = dist;
    }
}

class DCSObject extends DcsDict {
    public Map<Object, Object> obj_map = Collections.emptyMap();
    public String id;
    public Instant first_seen;
    public Instant last_seen;
    public String session_id;
    public String grp;
    public String coalition;
    public String country;
    public String color;
    public String type;
    public String platform;
    public String name;
    public String pilot;
    public int alive = 1;
    public Double lat;
    public Double lon;
    public Double alt = 1.0;
    public Double roll;
    public Double pitch;
    public Double yaw;
    public Double u_coord;
    public Double v_coord;
    public Double heading;
    public int updates = 1;
    public String parent;
    public Double parent_dist;
    public ArrayList<Impactor> impacts = new ArrayList<>();
    Boolean exported = false;
    ArrayList<ArrayList<String>> to_update;

    /**
     * @param key        Tacview object id to set for new instance.
     * @param session_id Tacview session id.
     * @param timestamp  Current ref timestamp to use as initial values for last_seen and first_seen.
     */
    public DCSObject(String key, String session_id, Instant timestamp) {
        this.id = key;
        this.session_id = session_id;
        this.last_seen = timestamp;
        this.first_seen = timestamp;
    }

    void markSeen(Instant timestamp) {
        this.last_seen = timestamp;
        this.updates += 1;
    }

    void addImpact(DistanceComparison impact) {
        Impactor new_impact = new Impactor(impact.id, impact.dist);
        this.impacts.add(new_impact);
    }

    public boolean checkExported() {
        return this.exported;
    }

    void setParent(DistanceComparison parent) {
        this.parent = parent.id;
        this.parent_dist = parent.dist;
    }

    public Object getValue(String fieldName) {
        Class<?> cls = this.getClass();
        try {
            Field field = cls.getField(fieldName);
            return field.get(this);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            LOGGER.error("Field {} does not exist!", fieldName);
            return null;
        }
    }

    void parseValuesFromString(String[] obj_split, DCSRef ref) {
        CoordKeys coords = new CoordKeys();
        for (int i = 1; i < obj_split.length; i++) {
            String[] val = obj_split[i].split(Pattern.quote("="));
            if (val.length == 2) {
                if (val[0].equals("T")) {
                    String[] coordinate_elem = val[1].substring(1).split(Pattern.quote("|"));
                    for (int e = 0; e < Math.min(coords.keys.length, coordinate_elem.length); e++) {
                        if (!(coordinate_elem[e] == null || coordinate_elem[e].isEmpty())) {
                            if (coords.keys[e].equals("lat")) {
                                Double c_val = Double.parseDouble(coordinate_elem[e]) + ref.referencelatitude;
                                this.setValue(coords.keys[e], c_val);
                            } else if (coords.keys[e].equals("lon")) {
                                this.setValue(coords.keys[e], Double.valueOf(coordinate_elem[e]));
                                Double c_val = Double.parseDouble(coordinate_elem[e]) + ref.referencelongitude;
                                this.setValue(coords.keys[e], c_val);
                            } else {
                                this.setValue(coords.keys[e], Double.valueOf(coordinate_elem[e]));
                            }
                        }
                    }
                } else {
                    this.setValue(val[0].toLowerCase(), val[1]);
                }
            }
        }
    }
}
