package net.coderland.example.parser.fieldparser;

/**
 * FieldParser that only return 1 string result.
 * 
 * @author Jeroen De Swaef <j@recallq.com>
 */
public abstract class SingleResultFieldParser implements FieldParser {
    public abstract Object parse(String input) throws FieldParserException;
}
