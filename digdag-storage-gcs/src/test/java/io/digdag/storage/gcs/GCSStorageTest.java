package io.digdag.storage.gcs;

import com.google.common.base.Strings;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.Storage;
import io.digdag.spi.StorageObjectSummary;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import static io.digdag.client.DigdagClient.objectMapper;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.is;
import static org.jboss.resteasy.plugins.providers.ProviderHelper.readString;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class GCSStorageTest
{
    private static final String GCS_TEST_SERVICE_ACCOUNT_KEY_FILENAME
            = System.getenv("GCS_TEST_SERVICE_ACCOUNT_KEY_FILENAME");

    private static final String GCS_TEST_BUCKET_NAME
            = System.getenv("GCS_TEST_BUCKET_NAME");

    private final GCSStorageFactory gcsStorageFactory;
    private final Config config;

    private Storage storage;

    // Called for every value returned by storageFactories()
    public GCSStorageTest(GCSStorageFactory storageFactory, Config config)
    {
        this.gcsStorageFactory = storageFactory;
        this.config = config;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> storageFactories()
    {
        List<Object[]> factoriesAndConfigs = new ArrayList<>();

        if (!Strings.isNullOrEmpty(GCS_TEST_SERVICE_ACCOUNT_KEY_FILENAME)) {
            Config remoteConfig = createConfig(GCS_TEST_BUCKET_NAME, GCS_TEST_SERVICE_ACCOUNT_KEY_FILENAME);
            factoriesAndConfigs.add(new Object[]{new GCSStorageFactory(), remoteConfig});
        }

        Config localConfig = createConfig("localbucket", null);
        factoriesAndConfigs.add(new Object[]{new LocalGCSStorageFactory(), localConfig});

        return factoriesAndConfigs;
    }

    @Before
    public void setUp()
    {
        this.storage = gcsStorageFactory.newStorage(config);
    }

    @Test
    public void putGet()
            throws Exception
    {
        storage.put("key/file/1", 3, contents("xxx"));
        storage.put("key/file/2", 1, contents("a"));
        storage.put("key/file/3", 4, contents("data"));
        assertThat(readString(storage.open("key/file/1").getContentInputStream()), is("xxx"));
        assertThat(readString(storage.open("key/file/2").getContentInputStream()), is("a"));
        assertThat(readString(storage.open("key/file/3").getContentInputStream()), is("data"));
    }

    @Test
    public void listAll()
            throws Exception
    {
        storage.put("key/file/1", 3, contents("xxx"));
        storage.put("key/file/2", 1, contents("1"));
        storage.put("key/file/3", 2, contents("12"));

        List<StorageObjectSummary> all = new ArrayList<>();
        storage.list("key/", all::addAll);
        all.sort(Comparator.comparing(StorageObjectSummary::getKey));

        assertThat(all.size(), is(3));
        assertThat(all.get(0).getKey(), is("key/file/1"));
        assertThat(all.get(0).getContentLength(), is(3L));
        assertThat(all.get(1).getKey(), is("key/file/2"));
        assertThat(all.get(1).getContentLength(), is(1L));
        assertThat(all.get(2).getKey(), is("key/file/3"));
        assertThat(all.get(2).getContentLength(), is(2L));
    }

    @Test
    public void listWithPrefix()
            throws Exception
    {
        storage.put("key1", 1, contents("0"));
        storage.put("test/file/1", 1, contents("1"));
        storage.put("test/file/2", 1, contents("1"));

        List<StorageObjectSummary> all = new ArrayList<>();
        storage.list("test", all::addAll);
        all.sort(Comparator.comparing(StorageObjectSummary::getKey));

        assertThat(all.size(), is(2));
        assertThat(all.get(0).getKey(), is("test/file/1"));
        assertThat(all.get(1).getKey(), is("test/file/2"));
    }

    private static Storage.UploadStreamProvider contents(String data)
    {
        return () -> new ByteArrayInputStream(data.getBytes(UTF_8));
    }

    private static Config createConfig(String bucket, String serviceAccountKeyFilename)
    {
        ConfigFactory cf = new ConfigFactory(objectMapper());
        return cf.create()
                .set("bucket", bucket)
                .set("service-account-key-filename", serviceAccountKeyFilename);
    }
}
