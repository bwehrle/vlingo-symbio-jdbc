package io.vlingo.symbio.store.common;

import io.vlingo.symbio.store.DataFormat;

public interface BootstrapProvider {

    DbBootstrap getBootstrap(final DataFormat dataFormat);
}
