package io.digdag.storage.gcs;

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelperWithMoreFields;
import io.digdag.spi.Storage;
import io.digdag.spi.StorageObjectSummary;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static io.digdag.core.storage.StorageManager.encodeHex;
import static io.digdag.util.Md5CountInputStream.digestMd5;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.is;
import static org.jboss.resteasy.plugins.providers.ProviderHelper.readString;
import static org.junit.Assert.assertThat;

public class GCSStorageTest {
    private GCSStorage storage;

    @Before
    public void setUp()
    {
         com.google.cloud.storage.Storage gStorage = LocalStorageHelperWithMoreFields.getOptions().toBuilder().build().getService();
         storage = new GCSStorage(null, gStorage, "bucketName");

         //todo: test retries for all methods, test with real GCS server
    }

    @Test
    public void putReturnsMd5AsEtag()
            throws Exception
    {
        String checksum1 = storage.put("key1", 10, contents("0123456789"));
        String checksum2 = storage.put("key2", 5, contents("01234"));
        assertThat(checksum1, is(md5hex("0123456789")));
        assertThat(checksum2, is(md5hex("01234")));
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
        storage.list("key", all::addAll);
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

    private static String md5hex(String data)
    {
        return md5hex(data.getBytes(UTF_8));
    }

    private static String md5hex(byte[] data)
    {
        return encodeHex(digestMd5(data));
    }
}
