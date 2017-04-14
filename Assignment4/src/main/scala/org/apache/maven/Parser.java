package org.apache.maven;

import java.net.URLDecoder;
import java.util.HashMap;
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
public class Parser {

    public static LinkedList<String> map(String value) throws IOException, InterruptedException {
        Pattern namePattern = Pattern.compile("^([^~]+)$");
        LinkedList<String> linkPageNames = new LinkedList<String>();
        try {
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            SAXParser saxParser = spf.newSAXParser();
            XMLReader xmlReader = saxParser.getXMLReader();

            xmlReader.setContentHandler(new WikiParser(linkPageNames));
            int delimLoc = value.indexOf(':');
            String pageName = value.substring(0, delimLoc);
            String html = value.substring(delimLoc + 1);

            Matcher matcher = namePattern.matcher(pageName);
            linkPageNames.clear();
            if (matcher.find()) {
                try {
                    xmlReader.parse(new InputSource(new StringReader(html)));
                } catch (Exception e) {
                }
                linkPageNames.addFirst(pageName);
            }
        }catch (Exception e){
        }
        return linkPageNames;
    }
    /** Parses a Wikipage, finding links inside bodyContent div element. */                                                                                                                                                                         
    private static class WikiParser extends DefaultHandler {
        Pattern linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
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