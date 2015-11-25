package net.coderland.example.parser.fieldparser;

import java.util.Map;

/**
 * FieldParser that returns a map of results.
 * Allows for a parser to return multiple fields.
 *  f.e. "GET /aa" -> method: GET, url: /aa
 * 
 * @author Jeroen De Swaef <j@recallq.com>
 */
public abstract class MultipleResultFieldParser implements FieldParser {
    public abstract Map<String, Object> parse(String input) throws FieldParserException;
}
