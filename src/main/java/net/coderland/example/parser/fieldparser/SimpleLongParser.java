package net.coderland.example.parser.fieldparser;

/**
 * Parser that treats a field as a long.
 * The default is to treat each field as a String.
 * 
 * @author Jeroen De Swaef <j@recallq.com>
 */
public class SimpleLongParser extends SingleResultFieldParser {

    @Override
    public Object parse(String input) throws FieldParserException {
        return Long.parseLong(input);
    }

}
