package c2c.stages;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import c2c.events.JobDone;
import c2c.events.JobRequest;
import c2c.payloads.KeyValue;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;

/**
 * Handles a client (someone who wants to perform a job). Reads from an input
 * file and writes to an output file.
 * 
 * @author Caleb Perkins
 * 
 */
public class ClientStage extends MapReduceStage {
	public static final long app_id = bamboo.router.Router
			.app_id(ClientStage.class);

	private static String class_name;
	private static String input_file;
	private static String output_file;

	private JsonWriter writer;

	public static void main(String[] args) throws Exception {
		if (args.length == 2 || args.length > 4) {
			System.err
					.println("Arguments: config_file class_name input_file output_file");
			System.exit(1);
		}
		String[] config = { args[0] };
		if (args.length == 4) {
			class_name = args[1];
			input_file = args[2];
			output_file = args[3];
		}
		bamboo.lss.DustDevil.main(config);
	}

	public ClientStage() throws Exception {
		super(null, KeyValue.class, JobDone.class);

		// Configure built-in stages to be less noisy
		Logger.getLogger(bamboo.lss.ASyncCoreImpl.class).setLevel(Level.WARN);
		Logger.getLogger(bamboo.db.StorageManager.class).setLevel(Level.WARN);
		Logger.getLogger(bamboo.dmgr.DataManager.class).setLevel(Level.WARN);
		Logger.getLogger(bamboo.dht.Dht.class).setLevel(Level.WARN);
	}

	@Override
	public void init(ConfigDataIF config) throws Exception {
		super.init(config);
		if (class_name != null) {
			JobRequest req = new JobRequest(class_name);
			parseInputFile(req);

			writer = new JsonWriter(new OutputStreamWriter(
					new FileOutputStream(output_file), "UTF-8"));
			writer.beginObject();

			classifier.dispatch_later(req, 1000);
		}
	}

	private void parseInputFile(JobRequest req) throws JsonIOException,
			JsonSyntaxException, IOException {
		JsonReader reader = new JsonReader(new FileReader(input_file));
		reader.beginObject();
		while (reader.hasNext()) {
			req.add(reader.nextName(), reader.nextString());
		}
		reader.endObject();
		reader.close();
	}

	@Override
	protected long getAppID() {
		return app_id;
	}

	@Override
	protected void handleOperationalEvent(QueueElementIF event) {
		if (writer == null)
			BUG("This should not be a client.");
		if (event instanceof KeyValue) {
			writeResult((KeyValue) event);
		} else if (event instanceof JobDone) {
			finishJob((JobDone) event);
		} else {
			BUG("Unknown event: " + event);
		}
	}

	private void finishJob(JobDone job) {
		logger.info("Job done! Results written to " + output_file);
		try {
			writer.endObject();
			writer.close();
		} catch (IOException e) {
			logger.fatal("Could not close result file. File may be corrupted.");
		}
	}

	private void writeResult(KeyValue payload) {
		try {
			writer.name(payload.key.data).value(payload.value);
		} catch (IOException e) {
			logger.fatal("Could not write to file.");
		}
	}

}
