import java.io.ByteArrayInputStream;
import java.util.Arrays;

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
		AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
				.forCluster("Test Cluster")
				.forKeyspace("testing")
				.withAstyanaxConfiguration(
						new AstyanaxConfigurationImpl()
								.setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE))
				.withConnectionPoolConfiguration(
						new ConnectionPoolConfigurationImpl("MyConnectionPool")
								.setPort(9160).setMaxConnsPerHost(1)
								.setSeeds("127.0.0.1:9160"))
				.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
				.buildKeyspace(ThriftFamilyFactory.getInstance());

		context.start();
		Keyspace keyspace = context.getClient();

		ChunkedStorageProvider provider = new CassandraChunkedStorageProvider(
				keyspace, "storage");

		byte[] bytes = new byte[1_000_000];
		byte A = 'A';
		Arrays.fill(bytes, A);

		ObjectMetadata meta = ChunkedStorage
				.newWriter(provider, "foobar", new ByteArrayInputStream(bytes))
				.withChunkSize(0x1000) // Optional chunk size to override
										// the default for this provider
				.withConcurrencyLevel(8) // Optional. Upload chunks in 8 threads
				.withTtl(60) // Optional TTL for the entire object
				.call();

		System.out.println(meta.getChunkCount());
		System.out.println(meta.getChunkSize());

	}
}
