package net.coderland.example.parser.fieldparser;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Converts a time string from nginx' $local_time into seconds since Epoch time.
 * 
 * @author Jeroen De Swaef
 */
public class LocalTimeFieldParser extends SingleResultFieldParser {
    public static final String DATE_FORMAT = "dd/MMM/yyyy:HH:mm:ssZ";

    @Override
    public Object parse(String input) throws FieldParserException {
        try {
            SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT, Locale.ENGLISH);
            Date d = format.parse(input);
            return d.getTime();
        } catch (ParseException ex) {
            throw new FieldParserException("Cannot convert " + input + " to a valid local_time date", ex);
        }
    }
}
