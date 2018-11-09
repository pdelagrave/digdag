package io.digdag.storage.gcs;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.common.base.Throwables;
import io.digdag.client.config.Config;
import io.digdag.spi.Storage;
import io.digdag.spi.StorageObject;
import io.digdag.spi.StorageObjectSummary;
import io.digdag.util.RetryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Collections;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

//TODO: implement and test retries for all methods, test with real GCS server
public class GCSStorage
        implements Storage
{
    private static Logger logger = LoggerFactory.getLogger(GCSStorage.class);

    private final com.google.cloud.storage.Storage gStorage;
    private final String bucket;

    public GCSStorage(final Config config, com.google.cloud.storage.Storage gStorage, String bucket)
    {
        checkArgument(!isNullOrEmpty(bucket), "bucket is null or empty");
        this.gStorage = gStorage;
        this.bucket = bucket;
    }

    private RetryExecutor uploadRetryExecutor()
    {
        return RetryExecutor.retryExecutor();
    }

    private RetryExecutor getRetryExecutor()
    {
        return RetryExecutor.retryExecutor();
    }

    @Override
    public StorageObject open(String key)
    {
        checkArgument(key != null, "key is null");

        Blob blob = gStorage.get(bucket, key);

        return new StorageObject(new ByteArrayInputStream(blob.getContent()), blob.getSize());
    }

    @Override
    public String put(String key, long contentLength, UploadStreamProvider payload)
            throws IOException
    {
        checkArgument(key != null, "key is null");

        BlobInfo blobInfo = BlobInfo.newBuilder(bucket, key).build();

        try {
            return uploadRetryExecutor()
                    .onRetry((exception, retryCount, retryLimit, retryWait) -> {
                        logger.warn("Retrying uploading file bucket " + bucket + " key " + key + " error: " + exception);
                    })
                    .retryIf((exception) -> {
                        if (exception instanceof IOException || exception instanceof InterruptedException) {
                            return false;
                        }
                        return true;
                    })
                    .runInterruptible(() -> {
                        try (InputStream in = payload.open()) {
                            // create(InputStream) is deprecated "because it cannot safely retry, given that it
                            // accepts an {@link InputStream} which can only be consumed once."
                            // UploadStreamProvider.open() is a lambda that we know so far is only returning
                            // newly created InputStream on every calls with full random access to the entire
                            // data source , making it safe to retry.
                            Blob blob = gStorage.create(blobInfo, in);
                            return blob.getEtag();
                        }
                    });
        } catch (InterruptedException ex) {
            throw Throwables.propagate(ex);
        } catch (RetryExecutor.RetryGiveupException ex) {
            Throwable cause = ex.getCause();
            Throwables.propagateIfInstanceOf(cause, IOException.class);
            throw Throwables.propagate(cause);
        }
    }

    @Override
    public void list(String keyPrefix, FileListing callback)
    {
        Page<Blob> blobs = gStorage.list(bucket, BlobListOption.prefix(keyPrefix));
        for (Blob blob : blobs.iterateAll()) {
            callback.accept(Collections.singletonList(
                    StorageObjectSummary.builder()
                            .key(blob.getName())
                            .contentLength(blob.getSize())
                            .lastModified(Instant.ofEpochSecond(blob.getUpdateTime()))
                            .build()
                    )
            );
        }
    }
}
