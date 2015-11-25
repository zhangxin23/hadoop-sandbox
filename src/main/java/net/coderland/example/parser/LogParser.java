package net.coderland.example.parser;

import net.coderland.example.parser.fieldparser.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class LogParser {
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final String metaPattern;
    private final InputStream logStream;
    private List<Map<String, Object>> logData;
    private static final List<String> logFieldNames = new ArrayList<String>();

    private static final Pattern escaper = Pattern.compile("([\\[\\]])");
    private static final Pattern extractVariablePattern = Pattern.compile("\\$[a-zA-Z0-9_]*");

    private static final String GZIP_EXTENSION = "gz";
    private static final Set<Character> charactersToEscape = new HashSet<Character>() {
        {
            add('[');
            add(']');
        }
    };
    private static final Map<String, FieldParser> fieldParsers = new HashMap<String, FieldParser>() {
        {
            put("time_local", new LocalTimeFieldParser());
            put("request", new RequestFieldParser());
            put("status", new SimpleLongParser());
            put("body_bytes_sent", new SimpleLongParser());
        }
    };

    public LogParser(String metaPattern) {
        this.logStream = null;
        this.metaPattern = metaPattern;
    }

    public LogParser(InputStream logStream, String metaPattern) {
        this.logStream = logStream;
        this.metaPattern = metaPattern;
    }

    private static String escapeRE(String str) {
        return escaper.matcher(str).replaceAll("\\\\$1");
    }
    
    private Pattern getLogFilePattern(String metaPattern) {
        Matcher matcher = extractVariablePattern.matcher(metaPattern);
        int parsedPosition = 0;
        StringBuilder parsePatternBuilder = new StringBuilder();

        while (matcher.find()) {
            if (parsedPosition < matcher.start()) {
                String residualPattern = metaPattern.substring(parsedPosition, matcher.start());
                parsePatternBuilder.append(escapeRE(residualPattern));
            }
            String logFieldName = metaPattern.substring(matcher.start() + 1, matcher.end());
            logFieldNames.add(logFieldName);
            parsedPosition = matcher.end();
            char splitCharacter = metaPattern.charAt(matcher.end());

            if(logFieldName.equals("request_body")) {
                parsePatternBuilder.append("(.*)");
                continue;
            }

            parsePatternBuilder.append("([^");
            if (charactersToEscape.contains(splitCharacter)) {
                parsePatternBuilder.append("\\");
            }
            parsePatternBuilder.append(splitCharacter);
            parsePatternBuilder.append("]*)");
        }
        parsePatternBuilder.append(metaPattern.substring(parsedPosition, metaPattern.length()));

        Pattern logFilePattern = Pattern.compile(parsePatternBuilder.toString());
        return logFilePattern;
    }
    
    private static InputStream getStreamForFilename(String filename) throws FileNotFoundException, IOException {
        if (filename.endsWith(GZIP_EXTENSION)) {
            return new GZIPInputStream(new FileInputStream(filename));
        } else {
            return new FileInputStream(filename);
        }
    }
    
    public List<Map<String, Object>> getLogData() {
        return logData;
    }

    public Map<String, Object> lineParse(String line) throws Exception{
        Map<String, Object> logLine = new HashMap<>();
        Pattern logFilePattern = getLogFilePattern(metaPattern);
        try {
            if (line != null) {
                Matcher logFileMatcher = logFilePattern.matcher(line);

                if (logFileMatcher.matches()) {
                    for (int i = 1; i <= logFileMatcher.groupCount(); i++) {
                        String logFieldName = logFieldNames.get(i - 1);
                        FieldParser fieldParser = fieldParsers.get(logFieldName);
                        Object fieldValue;
                        if (fieldParser != null) {
                            if (fieldParser instanceof SingleResultFieldParser) {
                                fieldValue = ((SingleResultFieldParser) fieldParser)
                                        .parse(logFileMatcher.group(i));
                            } else {
                                fieldValue = ((MultipleResultFieldParser) fieldParser)
                                        .parse(logFileMatcher.group(i));
                            }
                        } else {
                            fieldValue = logFileMatcher.group(i);
                        }
                        logLine.put(logFieldName, fieldValue);
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("Error parsing log", ex);
            throw new Exception("Error parsing log");
        }

        return logLine;
    }

    public void parse() {
        logData = new ArrayList<>();
        Pattern logFilePattern = getLogFilePattern(metaPattern);
        BufferedReader br;
        try {
            br = new BufferedReader(new InputStreamReader(logStream));
            String line;
            while ((line = br.readLine()) != null) {
                Map<String, Object> logLine = new HashMap<String, Object>();
                Matcher logFileMatcher = logFilePattern.matcher(line);

                if (logFileMatcher.matches()) {
                    for (int i = 1; i < logFileMatcher.groupCount(); i++) {
                        String logFieldName = logFieldNames.get(i - 1);
                        FieldParser fieldParser = fieldParsers.get(logFieldName);
                        Object fieldValue;
                        if (fieldParser != null) {
                            if (fieldParser instanceof SingleResultFieldParser) {
                                fieldValue = ((SingleResultFieldParser) fieldParser)
                                        .parse(logFileMatcher.group(i));
                            } else {
                                fieldValue = ((MultipleResultFieldParser) fieldParser)
                                        .parse(logFileMatcher.group(i));
                            }
                        } else {
                            fieldValue = logFileMatcher.group(i);
                        }
                        logLine.put(logFieldName, fieldValue);
                    }
                    logData.add(logLine);
                }

            }
            br.close();
        } catch (Exception ex) {
            logger.error("Error parsing log", ex);
        }
    }
}
