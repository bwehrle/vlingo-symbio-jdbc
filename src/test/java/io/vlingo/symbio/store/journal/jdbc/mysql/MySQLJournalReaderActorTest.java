// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.journal.jdbc.mysql;

import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.DbBootstrap;
import io.vlingo.symbio.store.common.MySqlBootstrap;
import io.vlingo.symbio.store.journal.jdbc.JDBCJournalReaderActorTest;

public class MySQLJournalReaderActorTest extends JDBCJournalReaderActorTest {
    @Override
    protected DbBootstrap getBootStrap() {
        return new MySqlBootstrap().getBootstrap(DataFormat.Text);
    }
}
