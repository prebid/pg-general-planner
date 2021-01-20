package org.prebid.pg.gp.server.jdbc;

import com.fasterxml.jackson.databind.JavaType;
import com.google.common.io.Resources;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.junit5.VertxExtension;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

@ExtendWith(VertxExtension.class)
public abstract class DataAccessClientTestBase {
    protected static final String JDBC_URL = "jdbc:h2:mem:test";
    protected static final String HOST_NAME = "MyMac";
    protected static Vertx vertx;
    protected static JDBCClient jdbcClient;
    protected Connection connection;

    @BeforeAll
    static void prepare() throws Exception {
        vertx = Vertx.vertx();
        jdbcClient = jdbcClient();
    }

    @AfterAll
    public static void cleanUp() throws Exception {
        vertx.close();
    }

    @BeforeEach
    void setUpBeforeEach() throws Exception {
        connection = DriverManager.getConnection(JDBC_URL);
        connection.createStatement().execute(getCreateTableSql());
        beforeEach();
    }

    abstract protected String getCreateTableSql();

    abstract protected String getTableName();

    abstract protected void beforeEach();

    @AfterEach
    void cleanUpAfterEach() throws Exception {
        try {
            connection.createStatement().execute(String.format("DROP TABLE %s;", getTableName()));
        } catch(Exception ex) {
            // do nothing
        } finally {
            connection.close();
        }
    }

    protected static JDBCClient jdbcClient() {
        return JDBCClient.createShared(vertx,
                new JsonObject()
                        .put("url", JDBC_URL)
                        .put("driver_class", "org.h2.Driver")
                        .put("max_pool_size", 10));
    }

    protected Future<SQLConnection> connect() {
        final Future<SQLConnection> future = Future.future();
        jdbcClient.getConnection(future);
        return future;
    }

    protected <T> List<T> loadItems(String path, String fileName, Class<T> clazz) throws Exception {
        String fullPath = path + "/" + fileName;
        final File statsFile = new File(Resources.getResource(fullPath).toURI());
        final String content = FileUtils.readFileToString(statsFile, "UTF-8");
        JavaType type = Json.mapper.getTypeFactory().constructCollectionType(List.class, clazz);
        return Json.mapper.readValue(content, type);
    }
}
