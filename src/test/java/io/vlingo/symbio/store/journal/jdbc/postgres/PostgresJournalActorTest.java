// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.journal.jdbc.postgres;

import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.DbBootstrap;
import io.vlingo.symbio.store.common.PostgresBootstrap;
import io.vlingo.symbio.store.journal.jdbc.JDBCJournalActorTest;

public class PostgresJournalActorTest extends JDBCJournalActorTest {
    @Override
    protected DbBootstrap getBootStrap() {
        return new PostgresBootstrap().getBootstrap(DataFormat.Text);
    }
}
