package com.twitter.presto.gateway.query;

import com.twitter.presto.gateway.RequestInfo;
import io.prestosql.spi.resourcegroups.QueryType;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Statement;

import java.util.Optional;

import static com.twitter.presto.gateway.query.QueryCategory.BATCH;
import static com.twitter.presto.gateway.query.QueryCategory.INTERACTIVE;
import static com.twitter.presto.gateway.query.QueryCategory.REALTIME;

public class QueryClassifier
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    private QueryClassifier() {}

    public static QueryCategory classify(RequestInfo requestInfo)
    {
        String queryString = requestInfo.getQuery();
        Statement statement = SQL_PARSER.createStatement(queryString, new ParsingOptions());
        Optional<QueryType> type = StatementUtils.getQueryType(statement.getClass());

        if (type.isPresent()) {
            switch (type.get()) {
                case DATA_DEFINITION:
                case DESCRIBE:
                case EXPLAIN:
                    return REALTIME;
                case SELECT:
                case INSERT:
                case DELETE:
                case ANALYZE:
                    if (requestInfo.getSource().contains("schedule")) {
                        return BATCH;
                    }
                    return INTERACTIVE;
                default:
                    // by default query is distributed to BATCH
                    return BATCH;
            }
        }

        // by default query is distributed to BATCH
        return BATCH;
    }
}
