package com.arrow.sender.service;

import jakarta.annotation.PreDestroy;
import okhttp3.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.*;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.*;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.util.Text;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class ArrowDataSenderService {
    private static final Logger logger = LoggerFactory.getLogger(ArrowDataSenderService.class);

    // 内存分配器配置
    private final BufferAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE); // 512MB总内存
    private final OkHttpClient httpClient = new OkHttpClient.Builder()
            .connectTimeout(10, TimeUnit.SECONDS)
            .writeTimeout(10, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .build();
    private static final MediaType ARROW_STREAM = MediaType.get("application/vnd.apache.arrow.stream");

    @PreDestroy
    public void cleanup() {
        if (rootAllocator != null) {
            rootAllocator.close();
            logger.info("BufferAllocator 已释放");
        }
    }

    public String sendArrowData(String targetUrl) throws IOException {
        try (BufferAllocator requestAllocator = rootAllocator.getRoot();
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
                new Field("name", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null),
                new Field("address", FieldType.nullable(new ArrowType.Struct()), Arrays.asList(
                        new Field("city", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null),
                        new Field("zipcode", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null)
                )),
                new Field("scores", FieldType.nullable(new ArrowType.List()), Arrays.asList(
                        new Field("element", FieldType.nullable(Types.MinorType.INT.getType()), null)
                )),
                new Field("attributes", FieldType.nullable(new ArrowType.Map(false)), Arrays.asList(
                        new Field("entries", FieldType.notNullable(new ArrowType.Struct()), Arrays.asList(
                                new Field("key", FieldType.notNullable(Types.MinorType.VARCHAR.getType()), null),
                                new Field("value", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null)
                        ))
                ))
        ));
    }

    private byte[] generateAndSerializeData(BufferAllocator allocator, Schema schema, ByteArrayOutputStream baos) throws IOException {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            IntVector idVector = (IntVector) root.getVector("id");
            VarCharVector nameVector = (VarCharVector) root.getVector("name");
            StructVector addressVector = (StructVector) root.getVector("address");
            ListVector scoresVector = (ListVector) root.getVector("scores");
            MapVector attributesVector = (MapVector) root.getVector("attributes");

            VarCharVector cityVector = (VarCharVector) addressVector.getChild("city");
            VarCharVector zipcodeVector = (VarCharVector) addressVector.getChild("zipcode");
            IntVector scoresElementVector = (IntVector) scoresVector.getDataVector();
            StructVector mapEntriesVector = (StructVector) attributesVector.getDataVector();
            VarCharVector mapKeyVector = (VarCharVector) mapEntriesVector.getChild("key");
            VarCharVector mapValueVector = (VarCharVector) mapEntriesVector.getChild("value");

            generateTestData(root, idVector, nameVector, addressVector, cityVector, zipcodeVector,
                    scoresVector, scoresElementVector, attributesVector, mapKeyVector, mapValueVector);

            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, baos)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            return baos.toByteArray();
        }
    }

    private void generateTestData(VectorSchemaRoot root,
                                  IntVector idVector,
                                  VarCharVector nameVector,
                                  StructVector addressVector,
                                  VarCharVector cityVector,
                                  VarCharVector zipcodeVector,
                                  ListVector scoresVector,
                                  IntVector scoresElementVector,
                                  MapVector attributesVector,
                                  VarCharVector mapKeyVector,
                                  VarCharVector mapValueVector) throws IOException {
        int rowCount = 100;
        root.setRowCount(rowCount);

        // 初始化所有向量
        idVector.allocateNew(rowCount);
        nameVector.allocateNew(rowCount);
        addressVector.allocateNew();
        cityVector.allocateNew(rowCount);
        zipcodeVector.allocateNew(rowCount);
        scoresVector.allocateNew();
        scoresElementVector.allocateNew(rowCount * 3);
        attributesVector.allocateNew();
        mapKeyVector.allocateNew(rowCount * 2);
        mapValueVector.allocateNew(rowCount * 2);

        // 使用Writer处理复杂类型
        try (UnionListWriter scoresWriter = scoresVector.getWriter();
             BaseWriter.MapWriter mapWriter = attributesVector.getWriter()) {

            for (int i = 0; i < rowCount; i++) {
                // 基础字段
                idVector.set(i, i + 1);
                nameVector.set(i, new Text("User_" + (i + 1)));

                // 地址结构
                cityVector.set(i, new Text("City_" + (i % 5 + 1)));
                zipcodeVector.set(i, new Text(String.format("%05d", 10000 + i)));
                addressVector.setIndexDefined(i);

                // 分数列表
                scoresWriter.setPosition(i);
                scoresWriter.startList();
                for (int j = 0; j < 3; j++) {
                    scoresWriter.writeInt((i + 1) * 10 + j);
                }
                scoresWriter.endList();

                // 属性Map
                mapWriter.setPosition(i);
                mapWriter.startMap();
                for (int j = 0; j < 2; j++) {
                    mapWriter.startEntry();
                    mapWriter.key().varChar().writeVarChar("key" + (j + 1));
                    mapWriter.value().varChar().writeVarChar("value" + (i + 1) + "-" + (j + 1));
                    mapWriter.endEntry();
                }
                mapWriter.endMap();
            }
        } catch (Exception e) {
            logger.error("生成测试数据失败 | 错误: {}", e.getMessage(), e);
            throw new IOException("生成测试数据失败: " + e.getMessage(), e);
        }

        // 设置所有值数量
        idVector.setValueCount(rowCount);
        nameVector.setValueCount(rowCount);
        cityVector.setValueCount(rowCount);
        zipcodeVector.setValueCount(rowCount);
        scoresElementVector.setValueCount(rowCount * 3);
        scoresVector.setValueCount(rowCount);
        mapKeyVector.setValueCount(rowCount * 2);
        mapValueVector.setValueCount(rowCount * 2);
        attributesVector.setValueCount(rowCount);
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
            return response.body().string();
        }
    }
}
