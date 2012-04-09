package c2c.utilities;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Iterator;
import java.util.LinkedList;

import c2c.payloads.KeyPayload;
import c2c.payloads.Value;

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
	private LinkedList<Dht.GetValue> values;
	private StorageManager.Key placemark;
	private KeyPayload key;

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
				return new Value(buffer).value;
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
		key = (KeyPayload) resp.user_data;
	}
	
	public void append(Dht.GetResp resp) {
		@SuppressWarnings("unchecked")
		LinkedList<Dht.GetValue> x = resp.values;
		
		values.addAll(x);
		
		placemark = resp.placemark;
	}
	
	public boolean hasMore() {
		return !placemark.equals(StorageManager.ZERO_KEY); 
	}
	
	public StorageManager.Key getPlacemark() {
		return placemark;
	}
	
	public String getKey() {
		return key.data;
	}

	@Override
	public Iterator<String> iterator() {
		return new GetRespIterator();
	}

}
