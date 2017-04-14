package org.apache.maven.graph;

import java.net.URLDecoder;
import java.util.LinkedList;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.StringReader;
import java.util.List;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

/** Decompresses bz2 file and parses Wikipages on each line. */
public class ParserMap extends Mapper<Object, Text, Text, Text> {
    private static Pattern namePattern;
    private static Pattern linkPattern;
    static {
        // Keep only html pages not containing tilde (~).
        namePattern = Pattern.compile("^([^~]+)$");
        // Keep only html filenames ending relative paths and not containing tilde (~).
        linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
    }
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            SAXParser saxParser = spf.newSAXParser();
            XMLReader xmlReader = saxParser.getXMLReader();
            List<String> linkPageNames = new LinkedList<>();
            xmlReader.setContentHandler(new WikiParser(linkPageNames));
            int delimLoc = value.toString().indexOf(':');
            String pageName = value.toString().substring(0, delimLoc);
            String html = value.toString().substring(delimLoc + 1);
            String output = "[";
            Matcher matcher = namePattern.matcher(pageName);
            linkPageNames.clear();
            if (matcher.find()) {
                try {
                    xmlReader.parse(new InputSource(new StringReader(html)));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                // converting the linked list to a concatenated output string to emit to the reducer
                for(String link:linkPageNames){
                    output += link + ", ";
                }
                if(output.length() > 2){
                    output = output.substring(0, output.length()-2);
                }
                output += "]";
                // emitting each page with it's adjacency list
                context.write(new Text(pageName),new Text(output));
                for(String link:linkPageNames){
                    if(!link.trim().equals(pageName.trim())){
                        // emitting each link in the adjacency list with a empty value to find the dangling nodes.
                        // this is explained in the reducer.
                        context.write(new Text(link),new Text(""));
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    /** Parses a Wikipage, finding links inside bodyContent div element. */
    private static class WikiParser extends DefaultHandler {
        /** List of linked pages; filled by parser. */
        private List<String> linkPageNames;
        /** Nesting depth inside bodyContent div element. */
        private int count = 0;

        public WikiParser(List<String> linkPageNames) {
            super();
            this.linkPageNames = linkPageNames;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            super.startElement(uri, localName, qName, attributes);
            if ("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id")) && count == 0) {
                // Beginning of bodyContent div element.
                count = 1;
            } else if (count > 0 && "a".equalsIgnoreCase(qName)) {
                // Anchor tag inside bodyContent div element.
                count++;
                String link = attributes.getValue("href");
                if (link == null) {
                    return;
                }
                try {
                    // Decode escaped characters in URL.
                    link = URLDecoder.decode(link, "UTF-8");
                } catch (Exception e) {
                    // Wiki-weirdness; use link as is.
                }
                // Keep only html filenames ending relative paths and not containing tilde (~).
                Matcher matcher = linkPattern.matcher(link);
                if (matcher.find()) {
                    // links are added without any duplicates
                    if(!linkPageNames.contains(matcher.group(1))){
                        linkPageNames.add(matcher.group(1));
                    }
                }
            } else if (count > 0) {
                // Other element inside bodyContent div.
                count++;
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            super.endElement(uri, localName, qName);
            if (count > 0) {
                // End of element inside bodyContent div.
                count--;
            }
        }
    }
}