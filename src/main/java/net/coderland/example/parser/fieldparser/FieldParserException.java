package net.coderland.example.parser.fieldparser;

/**
 * Marker exception for everything that goes wrong parsing a log field.
 * 
 * @author Jeroen De Swaef
 */
public class FieldParserException extends Exception {
    public FieldParserException(String message, Throwable cause) {
        super(message, cause);
    }
}
