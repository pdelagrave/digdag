package io.digdag.storage.gcs;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelperWithMoreFields;
import io.digdag.client.config.Config;
import io.digdag.spi.Storage;

public class LocalGCSStorageFactory extends GCSStorageFactory
{
    @Override
    public Storage newStorage(Config config)
    {
        return new GCSStorage(
                config,
                LocalStorageHelperWithMoreFields
                        .getOptions()
                        .toBuilder()
                        .setCredentials(NoCredentials.getInstance())
                        .build()
                        .getService(),
                config.get("bucket", String.class));
    }
}
