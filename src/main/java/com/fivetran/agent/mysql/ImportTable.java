/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql;

import com.fivetran.agent.mysql.output.ColumnDefinition;
import com.fivetran.agent.mysql.source.TableRef;

import java.util.List;
import java.util.Optional;

public interface ImportTable {

    Rows rows(TableRef table,
              List<ColumnDefinition> selectColumns,
              Optional<PagingParams> pagingParams);

    class PagingParams {
        public final List<String> orderByColumns;
        public final List<String> startAfterValue;
        public final long limitRows;

        public PagingParams(List<String> orderByColumns, List<String> startAfterValue, long limitRows) {
            this.orderByColumns = orderByColumns;
            this.startAfterValue = startAfterValue;
            this.limitRows = limitRows;
        }
    }
}
