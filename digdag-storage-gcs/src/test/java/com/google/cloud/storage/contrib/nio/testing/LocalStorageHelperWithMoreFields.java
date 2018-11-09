package com.google.cloud.storage.contrib.nio.testing;

import com.google.cloud.spi.ServiceRpcFactory;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.spi.v1.StorageRpc;

/**
 * Implementation copied from final class com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
 */
public class LocalStorageHelperWithMoreFields {

    // used for testing. Will throw if you pass it an option.
    private static final FakeStorageRpcWithMoreFields instance = new FakeStorageRpcWithMoreFields(true);

    private LocalStorageHelperWithMoreFields() {}

    /**
     * Returns a {@link StorageOptions} that use the static FakeStorageRpcWithEtags instance, and resets it
     * first so you start from a clean slate. That instance will throw if you pass it any option.
     */
    public static StorageOptions getOptions() {
        instance.reset();
        return StorageOptions.newBuilder()
                .setProjectId("dummy-project-for-testing")
                .setServiceRpcFactory(
                        new ServiceRpcFactory<StorageOptions>() {
                            @Override
                            public StorageRpc create(StorageOptions options) {
                                return instance;
                            }
                        })
                .build();
    }

    /**
     * Returns a {@link StorageOptions} that creates a new FakeStorageRpcWithEtags instance with the given
     * option.
     */
    public static StorageOptions customOptions(final boolean throwIfOptions) {
        return StorageOptions.newBuilder()
                .setProjectId("dummy-project-for-testing")
                .setServiceRpcFactory(
                        new ServiceRpcFactory<StorageOptions>() {
                            @Override
                            public StorageRpc create(StorageOptions options) {
                                return new FakeStorageRpcWithMoreFields(throwIfOptions);
                            }
                        })
                .build();
    }
}
