import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;

import com.beust.jcommander.JCommander;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.recipes.storage.CassandraChunkedStorageProvider;
import com.netflix.astyanax.recipes.storage.ChunkedStorage;
import com.netflix.astyanax.recipes.storage.ChunkedStorageProvider;
import com.netflix.astyanax.recipes.storage.ObjectMetadata;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class Main {

	public static void main(String[] args) throws Exception {

		CliOptions options = new CliOptions();
		JCommander jComm = new JCommander(options, args);

		if (options.help) {
			jComm.usage();
			System.exit(0);
		}

		AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
				.forCluster(options.custerName)
				.forKeyspace(options.keyspaceName)
				.withAstyanaxConfiguration(
						new AstyanaxConfigurationImpl()
								.setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE))
				.withConnectionPoolConfiguration(
						new ConnectionPoolConfigurationImpl("MyConnectionPool")
								.setPort(9160).setMaxConnsPerHost(1)
								.setSeeds(options.seeds))
				.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
				.buildKeyspace(ThriftFamilyFactory.getInstance());

		context.start();
		Keyspace keyspace = context.getClient();

		ChunkedStorageProvider provider = new CassandraChunkedStorageProvider(
				keyspace, options.columnFamily);

		InputStream inputStream = null;

		if (options.file != null) {
			System.out.println(String.format("Upload file %s", options.file));
			File fileToUpload = new File(options.file);
			inputStream = new FileInputStream(fileToUpload);
			options.fileName = options.file;
			options.size = (int) fileToUpload.length();
		} else {
			System.out.println(String.format("Upload random bytes (%d)",
					options.size));
			byte[] bytes = new byte[options.size];
			byte A = 'A';
			Arrays.fill(bytes, A);
			inputStream = new ByteArrayInputStream(bytes);
		}

		System.out.println(String.format(
				"Uploading file %s, size=%d, chunk_size=%d, threads=%d",
				options.fileName, options.size, options.chunkSize,
				options.concurrency));

		long now = System.currentTimeMillis();
		ObjectMetadata meta = ChunkedStorage
				.newWriter(provider, options.fileName, inputStream)
				.withChunkSize(options.chunkSize)
				.withConcurrencyLevel(options.concurrency).call();
		long duration = System.currentTimeMillis() - now;

		System.out.println(String.format(
				"Succesfully uploaded %s, num_chunks=%d, duration=%sms",
				options.fileName, meta.getChunkCount(), duration));

	}
}
