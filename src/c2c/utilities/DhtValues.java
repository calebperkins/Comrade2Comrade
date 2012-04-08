package c2c.utilities;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.util.Iterator;
import java.util.LinkedList;

import c2c.stages.MapReduceStage;

import bamboo.db.StorageManager;
import bamboo.dht.Dht;

/**
 * Makes parsing a GET value cleaner, because Bamboo does not use generics
 * for the Dht.GetResp values yet, and they are ByteBuffers.
 * 
 * @author Caleb Perkins
 * 
 */
public class DhtValues implements Iterable<String> {
	private static final CharsetDecoder decoder = MapReduceStage.CHARSET.newDecoder();
	private LinkedList<Dht.GetValue> values;
	private StorageManager.Key placemark;

	private class GetRespIterator implements Iterator<String> {
		private Iterator<Dht.GetValue> raw = values.iterator();

		@Override
		public boolean hasNext() {
			return raw.hasNext();
		}

		@Override
		public String next() {
			ByteBuffer buffer = raw.next().value;
			try {
				String data = decoder.decode(buffer).toString();

				// dirty hack to allow duplicates
				return data.split(c2c.stages.MapReduceStage.DELIMITER, 2)[1];
			} catch (CharacterCodingException e) {
				// TODO handle this better. Should be a fatal error?
				System.err.println(e);
				return "";
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

	
	@SuppressWarnings("unchecked")
	public DhtValues(Dht.GetResp resp) {
		values = resp.values;
		placemark = resp.placemark;
	}
	
	public void append(Dht.GetResp resp) {
		@SuppressWarnings("unchecked")
		LinkedList<Dht.GetValue> x = resp.values;
		
		values.addAll(x);
		
		placemark = resp.placemark;
	}
	
	public boolean hasNext() {
		return !placemark.equals(StorageManager.ZERO_KEY); 
	}
	
	public StorageManager.Key getPlacemark() {
		return placemark;
	}

	@Override
	public Iterator<String> iterator() {
		return new GetRespIterator();
	}

}
