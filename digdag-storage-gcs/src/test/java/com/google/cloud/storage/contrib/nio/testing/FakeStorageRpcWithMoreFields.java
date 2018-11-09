package com.google.cloud.storage.contrib.nio.testing;

import com.google.api.client.util.DateTime;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.storage.StorageException;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Map;

public class FakeStorageRpcWithMoreFields
        extends FakeStorageRpc
{
    /**
     * @param throwIfOption if true, we throw when given any option
     */
    public FakeStorageRpcWithMoreFields(boolean throwIfOption)
    {
        super(throwIfOption);
    }

    public StorageObject create(StorageObject object, InputStream content, Map<Option, ?> options)
    {
        byte[] bytes;
        try {
            bytes = ByteStreams.toByteArray(content);
        } catch (IOException e) {
            throw new StorageException(e);
        }

        // The more fields:
        object.setSize(BigInteger.valueOf(bytes.length));
        object.setUpdated(new DateTime(Instant.now().getEpochSecond()));

        // Using the md5 as the etag for now (although the real GCS isn't using that)
        // As long as the etag is always different when the content if different we're ok
        // https://cloud.google.com/storage/docs/hashes-etags
        // https://tools.ietf.org/html/rfc7232#section-2.3
        object.setEtag(md5hex(bytes));

        return super.create(object, new ByteArrayInputStream(bytes), options);
    }

    private static String md5hex(byte[] data)
    {
        try {
            return BaseEncoding.base16().lowerCase().omitPadding().encode(
                    MessageDigest.getInstance("MD5").digest(data)
            );
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("Failed to initialize MD5 digest", ex);
        }
    }
}
