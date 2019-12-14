package com.dcs;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Pattern;


public class TacviewClient extends DCSDb {
    static ImpactTypes impact_types = new ImpactTypes();
    static ParentTypes parent_types = new ParentTypes();
    static Logger LOGGER = LoggerFactory.getLogger(TacviewClient.class);
    static List<String> handshake_params = Arrays.asList(
            "XtraLib.Stream.0",
            "Tacview.RealTimeTelemetry.0",
            "tacview_reader",
            "0");
    static String handshake = String.join("\n", handshake_params) + "\0";
    int total_parents, total_impactors, total_iters, object_updates = 0;
    DCSRef ref;
    HashMap<String, DCSObject> tac_objects;
    String host = "127.0.0.1";
    int port = 42674;
    int max_iter = -1;
    Socket socket;
    BufferedReader input_stream;
    PrintWriter out;

    public TacviewClient(String[] args) {
        if (args.length >= 3 && args[0].equals("host"))
            host = args[1];

        if (args.length >= 3 && args[2].equals("port"))
            port = Integer.parseInt(args[3]);

        if (args.length >= 5 && args[4].equals("max_iter"))
            max_iter = Integer.parseInt(args[5]);

        tac_objects = new HashMap<>();
        ref = new DCSRef();
    }

    public static void main(String[] args) {
        try {
            TacviewClient tacview = new TacviewClient(args);
            tacview.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void connectToTacview() throws IOException {
        LOGGER.info("Starting tacview collector...\nConnecting to: {}:{}\n Max Iters: {}",
                this.host, this.port, this.max_iter);
        this.socket = new Socket(this.host, this.port);
        this.out = new PrintWriter(this.socket.getOutputStream(), true);
        this.input_stream = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));
        LOGGER.info("Connected...sending handshake...");
        this.out.print(handshake);
        this.out.flush();
        LOGGER.info("Handshake accepted...reading stream...");
    }

    private void run() throws Exception {
        try {
            this.connectToTacview();
            this.input_stream
                    .lines()
                    .takeWhile(v -> (v != null && (total_iters < this.max_iter)))
                    .forEach(value -> {
                        if (!this.ref.has_refs) {
                            String[] obj_split = value.split(Pattern.quote(","));
                            if (obj_split[0].equals("0")) {
                                ref.updateRefValue(obj_split);
                            }
                        } else {
                            if (value.substring(0, 1).equals("#")) {
                                ref.updateTimeOffset(value);
                            } else {
                                DCSObject rec = parseMessageToMapOrUpdate(value);
                                total_iters++;
                                if (rec != null && !rec.exported) {
                                    this.insertNewObject(rec);
                                    rec.exported = true;
                                    LOGGER.info("total: {}", this.total_writes);
                                }
                            }
                        }
                    });

            tac_objects.entrySet().
                    stream().
                    filter(rec -> (!rec.getValue().checkExported()))
                    .forEach(rec -> this.insertNewObject(rec.getValue()));

            this.shutDownReader();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @NotNull
    private DCSObject getOrCreateDCSObject(String key) {
        DCSObject obj_dict;
        if (tac_objects.containsKey(key)) {
            obj_dict = tac_objects.get(key);
            obj_dict.markSeen(ref.referencetime);
        } else {
            obj_dict = new DCSObject(key, ref.session_id, ref.referencetime);
            tac_objects.put(key, obj_dict);
        }
        return obj_dict;
    }

    private boolean filter_types(DCSObject rec, @NotNull String compare_type) {
        if (compare_type.equals("parent")) {
            return (!parent_types.checkValueEqualsAny(rec.type));
        } else if (compare_type.equals("impactor")) {
            return impact_types.checkValueEqualsAny(rec.type);
        } else {
            return false;
        }
    }

    @Nullable
    private DistanceComparison checkForClosest(DCSObject current_rec, @NotNull String compare_type) {
        Instant recent_rec_offset = ref.referencetime.plus(1, ChronoUnit.MINUTES);
        if ((compare_type.equals("parent") &&
                (current_rec.updates > 1 ||
                        current_rec.color == null ||
                        !parent_types.checkValueEqualsAny(current_rec.type)))) {
            return null;
        }

        String[] accept_colors;
        if (compare_type.equals("parent")) {
            accept_colors = (current_rec.color.equals("Violet")) ? new String[]{"Red", "Blue"} : new String[]{current_rec.color};
        } else {
            accept_colors = (current_rec.color.equals("Blue")) ? new String[]{"Red"} : new String[]{"Blue"};
        }

        @SuppressWarnings("ComparatorCombinators") Optional<DistanceComparison> possible_comps =
                tac_objects.entrySet()
                        .stream()
                        .parallel()
                        .map(Map.Entry::getValue)
                        .filter(rec -> filter_types(rec, compare_type))
                        .filter(rec -> Arrays.asList(accept_colors).contains(rec.color))
                        .filter(rec -> !rec.type.equals(current_rec.type))
                        .filter(rec -> ((rec.alive == 1) | (rec.last_seen.compareTo(recent_rec_offset) > 0)))
                        .map(rec -> DistanceCalculator.compute_distance(rec, current_rec)).min((l1, l2) -> l1.dist.compareTo(l2.dist));

        if (possible_comps.isPresent()) {
            DistanceComparison closest = possible_comps.get();
            LOGGER.debug("{} lookup for {}-{} -- {}-{}: {}",
                    compare_type, current_rec.type, current_rec.id, closest.id, closest.type, closest.dist);
            if (closest.dist < 100) {
                return closest;
            }
            LOGGER.debug("Rejecting closest {} match for {}-{}: {} {}!",
                    compare_type, current_rec.id, current_rec.type, closest.type, closest.dist);
        }
        LOGGER.debug("Zero possible {} matches for {}-{}", compare_type, current_rec.id, current_rec.type);
        return null;
    }

    @Nullable
    private DCSObject parseMessageToMapOrUpdate(@NotNull String obj) {
        String[] obj_split = obj.trim().split(Pattern.quote(","));
        if (obj_split[0].equals("0")) {
            return null;
        }

        if (obj_split[0].substring(0, 1).equals("-")) {
            DCSObject obj_dict = tac_objects.get(obj_split[0].substring(1));
            obj_dict.alive = 0;
            return null;
        }

        String id_val = obj_split[0];
        DCSObject obj_dict = getOrCreateDCSObject(id_val);
        obj_dict.parseValuesFromString(obj_split, ref);

        DistanceComparison distance = checkForClosest(obj_dict, "parent");
        if (distance == null) {
            LOGGER.debug("No Parent record found for {}...", obj_dict.id);
            return obj_dict;
        }

        total_parents++;
        obj_dict.setParent(distance);
        DCSObject parent = getOrCreateDCSObject(obj_dict.parent);
        if (obj_dict.type.equals("Misc+Shrapnel")) {
            DistanceComparison closest = checkForClosest(parent, "impactor");
            if (closest != null) {
                total_impactors++;
                parent.addImpact(closest);
            }
        }
        LOGGER.debug("Object: {}", obj_dict);
        return obj_dict;
    }

    private void shutDownReader() throws IOException {
        this.flushBatchToDatabase(true);
        this.out.close();
        this.input_stream.close();
        this.socket.close();
        this.getJobStats();
        LOGGER.info("Stream consumed...exiting!");
    }

    private void getJobStats() {
        Instant end_time = Instant.now();
        Duration time_diff = Duration.between(ref.start_time, end_time);
        long seconds_diff = time_diff.toMillis();
        double iter_per_sec = ((double) total_iters / (double) seconds_diff) * 1000.0;
        LOGGER.info("\nTotal lines read: {}" +
                        "\nLines per second: {}" +
                        "\nTotal Keys: {}" +
                        "\nTotal Parents: {}" +
                        "\nTotal Impactors: {}",
                total_iters, iter_per_sec, tac_objects.size(), total_parents, total_impactors);
    }
}
