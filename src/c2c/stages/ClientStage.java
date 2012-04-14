package c2c.stages;

import java.io.FileReader;
import java.util.Map.Entry;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import c2c.events.JobRequest;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;

public class ClientStage extends MapReduceStage {
	public static final long app_id = bamboo.router.Router
			.app_id(ClientStage.class);

	private static String class_name;
	private static String input_file;

	public static void main(String[] args) throws Exception {
		if (args.length == 2 || args.length > 3) {
			System.err.println("Arguments: config_file class_name input_file");
			System.exit(1);
		}
		String[] config = { args[0] };
		if (args.length == 3) {
			class_name = args[1];
			input_file = args[2];
		}
		bamboo.lss.DustDevil.main(config);
	}

	public ClientStage() throws Exception {
		super(null);
	}

	@Override
	public void init(ConfigDataIF config) throws Exception {
		super.init(config);
		if (class_name != null) {
			JobRequest req = new JobRequest(class_name);
			final JsonParser parser = new JsonParser();
			final JsonElement jsonElement = parser.parse(new FileReader(
					input_file));
			final JsonObject jsonObject = jsonElement.getAsJsonObject();

			for (final Entry<String, JsonElement> entry : jsonObject.entrySet()) {
				final String key = entry.getKey();
				final String value = entry.getValue().getAsString();
				req.add(key, value);
			}
			classifier.dispatch_later(req, 1000);
		}

	}

	@Override
	protected long getAppID() {
		return app_id;
	}

	@Override
	protected void handleOperationalEvent(QueueElementIF event) {
	}

}
