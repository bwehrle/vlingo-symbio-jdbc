// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.common.jdbc;

import java.io.Closeable;
import java.sql.PreparedStatement;

public class CachedStatement<T> implements Closeable {
  public final T data;
  public final PreparedStatement preparedStatement;

  public CachedStatement(final PreparedStatement preparedStatement, final T data) {
    this.preparedStatement = preparedStatement;
    this.data = data;
  }

  @Override
  public void close() {
    try {
      if (preparedStatement != null && !preparedStatement.isClosed()) {
        preparedStatement.close();
      }
    } catch (Exception ignored) {

    }
  }
}
