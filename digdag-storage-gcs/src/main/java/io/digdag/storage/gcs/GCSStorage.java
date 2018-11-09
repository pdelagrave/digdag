package io.digdag.storage.gcs;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.client.config.Config;
import io.digdag.spi.StorageFileNotFoundException;
import io.digdag.spi.StorageObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.spi.Storage;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

public class GCSStorage
    implements Storage
{
    private static Logger logger = LoggerFactory.getLogger(GCSStorage.class);

    private final Config config;
    private final com.google.cloud.storage.Storage gStorage;
    private final String bucket;
    private final ExecutorService uploadExecutor;

    public GCSStorage(final Config config, com.google.cloud.storage.Storage gStorage, String bucket)
    {
        checkArgument(!isNullOrEmpty(bucket), "bucket is null or empty");
        logger.info("{}", gStorage.list(bucket).getValues().iterator().next().getName());
        this.config = config;
        this.gStorage = gStorage;
        this.bucket = bucket;
        this.uploadExecutor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                    .setNameFormat("storage-gcs-upload-transfer-%d")
                    .build());
        // TODO check the existence of the bucket so that following
        //      any GET or PUT don't get 404 Not Found error.
    }

    @Override
    public StorageObject open(String key) throws StorageFileNotFoundException {
        if (1==2)
            throw new StorageFileNotFoundException("");
        return null;
    }

    @Override
    public String put(String key, long contentLength, UploadStreamProvider payload) throws IOException {
        if (1==2)
            throw new IOException();
        return null;
    }

    @Override
    public void list(String keyPrefix, FileListing callback) {

    }
}
