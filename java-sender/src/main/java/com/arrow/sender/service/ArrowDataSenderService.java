package com.arrow.sender.service;

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

@Service
public class ArrowDataSenderService {

    private final OkHttpClient httpClient = new OkHttpClient.Builder()
            .connectTimeout(10, TimeUnit.SECONDS)
            .writeTimeout(10, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .build();
    private static final MediaType ARROW_STREAM = MediaType.get("application/vnd.apache.arrow.stream");


    public String sendArrowData(String targetUrl) throws IOException {
        // In a real app, manage allocator lifecycle carefully.
        // For simplicity here, using try-with-resources for a one-off send.
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            // Define the schema: id (Int32), name (VarChar)
            Schema schema = new Schema(Arrays.asList(
                    new Field("id", FieldType.nullable(Types.MinorType.INT.getType()), null),
                    new Field("name", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null)
            ));
            // Create vectors and populate data
            int rowCount = 5;
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                 IntVector idVector = (IntVector) root.getVector("id");
                 VarCharVector nameVector = (VarCharVector) root.getVector("name")) {
                root.setRowCount(rowCount);
                idVector.allocateNew(rowCount);
                nameVector.allocateNew(rowCount);
                for (int i = 0; i < rowCount; i++) {
                    idVector.set(i, i + 1);
                    nameVector.set(i, new Text("Name_" + (i + 1)));
                }
                idVector.setValueCount(rowCount);
                nameVector.setValueCount(rowCount);
                // Serialize to Arrow IPC Stream format
                try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, baos)) {
                    writer.start();
                    writer.writeBatch();
                    writer.end();
                }
                byte[] arrowDataBytes = baos.toByteArray();
                // Send using OkHttp
                RequestBody body = RequestBody.create(arrowDataBytes, ARROW_STREAM);
                Request request = new Request.Builder()
                        .url(targetUrl)
                        .post(body)
                        .build();
                try (Response response = httpClient.newCall(request).execute()) {
                    if (!response.isSuccessful()) {
                        throw new IOException("Unexpected code " + response);
                    }
                    return response.body().string();
                }
            } // VectorSchemaRoot, Vectors implicitly released if allocator is closed, but explicit Release() is better in long-running allocators
        } catch (Exception e) {
            throw new IOException("Failed to send Arrow data", e);
        }
    }
}
