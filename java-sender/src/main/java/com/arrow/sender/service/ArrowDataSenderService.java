package com.arrow.sender.service;

import jakarta.annotation.PreDestroy;
import okhttp3.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.util.Text;
import org.springframework.stereotype.Service;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
// ... 已有的 import ...
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class ArrowDataSenderService {

    private static final Logger logger = LoggerFactory.getLogger(ArrowDataSenderService.class);


    private final OkHttpClient httpClient = new OkHttpClient.Builder()
            .connectTimeout(10, TimeUnit.SECONDS)
            .writeTimeout(10, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .build();
    private static final MediaType ARROW_STREAM = MediaType.get("application/vnd.apache.arrow.stream");

    // 新增成员变量（设置合理的内存上限）
    private final BufferAllocator rootAllocator = new RootAllocator(256L * 1024 * 1024); // 256MB 内存限制

    // 新增销毁方法
    @PreDestroy
    public void cleanup() {
        if (rootAllocator != null) {
            rootAllocator.close();
            logger.info("BufferAllocator 已释放");
        }
    }

    public String sendArrowData(String targetUrl) throws IOException {
        try (BufferAllocator requestAllocator = rootAllocator.newChildAllocator("request", 0, 64 * 1024 * 1024);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            Schema schema = createSchema();
            byte[] arrowDataBytes = generateAndSerializeData(requestAllocator, schema, baos);
            return sendHttpRequest(targetUrl, arrowDataBytes);

        } catch (Exception e) {
            logger.error("发送Arrow数据失败 | URL: {}, 错误: {}", targetUrl, e.getMessage(), e);
            throw new IOException("发送Arrow数据失败: " + e.getMessage(), e);
        }
    }

    private Schema createSchema() {
        return new Schema(Arrays.asList(
                new Field("id", FieldType.nullable(Types.MinorType.INT.getType()), null),
                new Field("name", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null)
        ));
    }

    private byte[] generateAndSerializeData(BufferAllocator allocator, Schema schema, ByteArrayOutputStream baos)
            throws IOException {

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
             IntVector idVector = (IntVector) root.getVector("id");
             VarCharVector nameVector = (VarCharVector) root.getVector("name")) {

            generateTestData(root, idVector, nameVector);
            serializeArrowData(root, baos);

            return baos.toByteArray();
        }
    }

    private void generateTestData(VectorSchemaRoot root, IntVector idVector, VarCharVector nameVector) {
        int rowCount = 5;
        root.setRowCount(rowCount);

        idVector.allocateNew(rowCount);
        nameVector.allocateNew(rowCount);

        for (int i = 0; i < rowCount; i++) {
            idVector.set(i, i + 1);
            nameVector.set(i, new Text("Name_" + (i + 1)));
        }

        idVector.setValueCount(rowCount);
        nameVector.setValueCount(rowCount);
    }

    private void serializeArrowData(VectorSchemaRoot root, ByteArrayOutputStream baos) throws IOException {
        try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, baos)) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }
    }

    private String sendHttpRequest(String targetUrl, byte[] arrowDataBytes) throws IOException {
        RequestBody body = RequestBody.create(arrowDataBytes, ARROW_STREAM);
        Request request = new Request.Builder()
                .url(targetUrl)
                .post(body)
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                logger.error("HTTP请求失败 | URL: {}, 状态码: {}", targetUrl, response.code());
                throw new IOException("服务器返回错误: " + response.code());
            }
            assert response.body() != null;
            return response.body().string();
        }
    }
}
