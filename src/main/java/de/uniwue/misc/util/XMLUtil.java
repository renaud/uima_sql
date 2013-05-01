package de.uniwue.misc.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.Date;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;


public class XMLUtil {

	private static ByteArrayOutputStream stream = null;

	public static XMLStreamWriter createStreamWriter() {
		if (stream != null) {
			System.out.println("ERROR: there is already an existing stream.");
		}
		stream = new ByteArrayOutputStream();
		XMLOutputFactory xmlFactory = XMLOutputFactory.newInstance();
		try {
			XMLStreamWriter writer = xmlFactory.createXMLStreamWriter(stream);
			writer.writeStartDocument();
			return writer;
		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
		return null;
	}


	public static String writeWriterContent(XMLStreamWriter writer) {
		try {
			writer.writeEndDocument();
			writer.close();
			String xml = stream.toString();
			stream.close();
			stream = null;
			return XMLUtil.getIndented(xml);
		} catch (XMLStreamException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}


	public static void writeWriterContentToFile(XMLStreamWriter writer, File outputFile) {
		try {
			writer.writeEndDocument();
			writer.close();
			String xml = stream.toString();
			stream.close();
			stream = null;
			XMLUtil.writeIndented(xml, outputFile);
		} catch (XMLStreamException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	private static String getIndented(Document aDoc) {
		String outputXml = "";
		try {
			TransformerFactory tf = TransformerFactory.newInstance();
			//      tf.setAttribute("indent-number", new Integer(4));
			Transformer transformer;
			transformer = tf.newTransformer();
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.setOutputProperty(OutputKeys.METHOD, "xml");
			transformer.setOutputProperty(OutputKeys.MEDIA_TYPE, "text/xml");
			transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
			StreamResult result = new StreamResult(new StringWriter());
			DOMSource source = new DOMSource(aDoc);
			transformer.transform(source, result);
			outputXml = result.getWriter().toString();
		} catch (TransformerConfigurationException e) {
			e.printStackTrace();
		} catch (TransformerException e) {
			e.printStackTrace();
		}
		return outputXml;
	}


	public static String getIndented(String inputXML) {
		Document aDoc = getDoc(inputXML);
		return getIndented(aDoc);
	}


	public static void writeIndented(Document aDoc, File file) {
	}


	public static void writeIndented(String inputXML, File file) {
		String intended = getIndented(inputXML);
		try {
			FileUtilsUniWue.saveString2File(intended, file, "UTF-8");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	public static String getDirectTextContent(Node aNode) {
		String result = "";
		NodeList nodeList = aNode.getChildNodes();
		for (int i = 0; i < nodeList.getLength(); i++) {
      Node aSubNode = nodeList.item(i);
      if (aSubNode.getNodeType() == Node.TEXT_NODE) {
      	result += aSubNode.getNodeValue();
      }
		}
		return result;
	}


	public static double getDouble(Node aNode, String attr) {
		return getDouble(aNode, attr, Double.NaN);
	}


	public static double getDouble(Node aNode, String attr, double defaultValue) {
		if (aNode.getAttributes().getNamedItem(attr) != null) {
			String attrString = aNode.getAttributes().getNamedItem(attr).getNodeValue();
			double year = Double.parseDouble(attrString);
			return year;
		} else {
			return defaultValue;
		}
	}


	public static int getInt(Node aNode, String attr, int defaultValue) {
		if (aNode.getAttributes().getNamedItem(attr) != null) {
			String attrString = aNode.getAttributes().getNamedItem(attr).getNodeValue();
			int year = Integer.parseInt(attrString);
			return year;
		} else {
			return defaultValue;
		}
	}


	public static long getLong(Node aNode, String attr, long defaultValue) {
		if (aNode.getAttributes().getNamedItem(attr) != null) {
			String attrString = aNode.getAttributes().getNamedItem(attr).getNodeValue();
			Long year = Long.parseLong(attrString);
			return year;
		} else {
			return defaultValue;
		}
	}


	public static int getInt(Node aNode, String attr) {
		return getInt(aNode, attr, Integer.MIN_VALUE);
	}


	public static long getTimeLong(Node aNode, String attr) {
		if (aNode.getAttributes().getNamedItem(attr) != null) {
			String attrString = aNode.getAttributes().getNamedItem(attr).getNodeValue();
			try {
				Date time;
				time = EnvironmentUniWue.sdf_withTime.parse(attrString);
				return time.getTime();
			} catch (ParseException e) {
				e.printStackTrace();
				return 0;
			}
		} else {
			return 0;
		}
	}


	public static boolean getBoolean(Node aNode, String attr, boolean defaultValue) {
		if (aNode.getAttributes().getNamedItem(attr) != null) {
			String docText = aNode.getAttributes().getNamedItem(attr).getNodeValue();
			if (docText.toLowerCase().equals("true")) {
				return true;
			}
			if (docText.toLowerCase().equals("false")) {
				return false;
			}
			System.out.println("strange boolean value '" + docText + "'");
			return defaultValue;
		} else {
			return defaultValue;
		}
	}


	public static String getString(Node aNode, String attr) {
		return getString(aNode, attr, null);
	}


	public static String getString(Node aNode, String attr, String aDefault) {
		if (aNode.getAttributes().getNamedItem(attr) != null) {
			String docText = aNode.getAttributes().getNamedItem(attr).getNodeValue();
			return docText;
		} else {
			return aDefault;
		}
	}


	public static Document getDoc(String xml) {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db;

		dbf.setValidating(false);
		try {
			dbf.setFeature("http://xml.org/sax/features/namespaces", false);
			dbf.setFeature("http://xml.org/sax/features/validation", false);
			dbf.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
			dbf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
			db = dbf.newDocumentBuilder();
			InputSource source = new InputSource(new ByteArrayInputStream(xml.getBytes("utf-8")));
			Document doc = db.parse(source);
			return doc;
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}


	public static NodeList getNodeList(File aFile) {
		try {
			String xmlString = FileUtilsUniWue.file2String(aFile, "UTF-8");
			return getNodeList(xmlString);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}


	public static NodeList getNodeList(String anXMLString) {
		NodeList nodeList;

		Document doc = getDoc(anXMLString);
		doc.getDocumentElement().normalize();
		nodeList = doc.getChildNodes();
		return nodeList;
	}


}
