package eu.centai.hypeq.utils;

/**
 *
 * @author giulia
 */
public class CMDLParser {
    
    public static void parse(String[] args) {

        if (args != null && args.length > 0) {
            parseArgs(args);
        }
    }

    private static void parseArgs(String[] args) {
        for (String arg : args) {
            String[] parts = arg.split("=");
            try {
                parseArg(parts[0], parts[1]);
            } catch (ArrayIndexOutOfBoundsException ex) {
                parseArg(parts[0], null);
            }
        }
    }

    private static void parseArg(String key, String value) {
        if (key.compareToIgnoreCase("dataFolder") == 0) {
            Settings.dataFolder = value;
        } else if (key.compareToIgnoreCase("outputFolder") == 0) {
            Settings.outputFolder = value;
        } else if (key.compareToIgnoreCase("dataFile") == 0) {
            Settings.dataFile = value;
        } else if (key.compareToIgnoreCase("queryFile") == 0) {
            Settings.queryFile = value;
        } else if (key.compareToIgnoreCase("store") == 0) {
            Settings.store = Boolean.valueOf(value);
        } else if (key.compareToIgnoreCase("numLandmarks") == 0) {
            Settings.numLandmarks = Integer.parseInt(value);
        } else if (key.compareToIgnoreCase("landmarkSelection") == 0) {
            Settings.landmarkSelection = value;
        } else if (key.compareToIgnoreCase("samplePerc") == 0) {
            Settings.samplePerc = Double.parseDouble(value);
        } else if (key.compareToIgnoreCase("numQueries") == 0) {
            Settings.numQueries = Integer.parseInt(value);
        } else if (key.compareToIgnoreCase("landmarkAssignment") == 0) {
            Settings.landmarkAssignment = value;
        } else if (key.compareToIgnoreCase("lb") == 0) {
            Settings.lb = Integer.parseInt(value);
        } else if (key.compareToIgnoreCase("maxS") == 0) {
            Settings.maxS = Integer.parseInt(value);
        } else if (key.compareToIgnoreCase("alpha") == 0) {
            Settings.alpha = Double.parseDouble(value);
        } else if (key.compareToIgnoreCase("beta") == 0) {
            Settings.beta = Double.parseDouble(value);
        } else if (key.compareToIgnoreCase("seed") == 0) {
            Settings.seed = Integer.parseInt(value);
        } else if (key.compareToIgnoreCase("isApproximate") == 0) {
            Settings.isApproximate = Boolean.valueOf(value);
        } else if (key.compareToIgnoreCase("kind") == 0) {
            Settings.kind = value;
        } else if (key.compareToIgnoreCase("k") == 0) {
            Settings.k = Integer.parseInt(value);
        }
    }
    
}
