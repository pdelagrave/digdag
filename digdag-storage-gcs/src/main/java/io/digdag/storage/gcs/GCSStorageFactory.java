package io.digdag.storage.gcs;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.StorageOptions;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.Storage;
import io.digdag.spi.StorageFactory;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class GCSStorageFactory
    implements StorageFactory
{
    @Override
    public String getType()
    {
        return "gcs";
    }

    @Override
    public Storage newStorage(Config config)
    {
        // called from io.digdag.core.storage.StorageManager.create(java.lang.String, io.digdag.client.config.Config, java.lang.String)
        //archive.   gcs.service-account-key-filename
        //log-server.gcs.service-account-key-filename
        String serviceAccountKeyFilename = config.get("service-account-key-filename", String.class);

        // todo: do validation for all the steps where failure is likely and throw the right exception based on what other storage factories are throwing in similar situations.
        ServiceAccountCredentials credentials;
        try {
            credentials = ServiceAccountCredentials.fromStream(new FileInputStream(serviceAccountKeyFilename));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        com.google.cloud.storage.Storage gStorage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();

        String bucket = config.get("bucket", String.class);

        return new GCSStorage(config, gStorage, bucket);
    }
}
