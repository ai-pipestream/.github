# Connector Upload Methods

The `connector-intake-service` supports three methods for uploading documents, each optimized for different use cases.

## Method 1: Full File Upload (Unary RPC)

**RPC**: `uploadDocument(UploadDocumentRequest) -> DocumentResponse`

**Use Case**: Files up to 2GB, simple uploads, small to medium files

**Protocol**: Single request/response - the entire file is sent in one gRPC message

**Content**: `DocumentData.hasRawData()` with full file bytes

**Benefits**: 
- Simple implementation
- No streaming overhead
- Immediate response
- Best for files under 100MB

**Example Usage**:
```java
UploadDocumentRequest request = UploadDocumentRequest.newBuilder()
    .setConnectorId("connector-123")
    .setApiKey("api-key")
    .setDocument(DocumentData.newBuilder()
        .setSourceId("file:///path/to/file.pdf")
        .setFilename("file.pdf")
        .setPath("/path/to")
        .setMimeType("application/pdf")
        .setSizeBytes(fileSize)
        .setRawData(ByteString.copyFrom(fileBytes))
        .build())
    .build();

DocumentResponse response = client.uploadDocument(request);
```

## Method 2: Synchronous Chunked Streaming (Client-Side Streaming)

**RPC**: `streamDocuments(stream DocumentIntakeRequest) -> DocumentIntakeResponse`

**Use Case**: Medium to large files, ordered streaming, real-time processing

**Protocol**: Client sends all chunks in sequence via client-side streaming, server returns ONE final response after all chunks are processed

**Content**: `DocumentData.hasChunk()` with `StreamingChunk` messages using header/data/footer protocol

**Benefits**: 
- Memory efficient
- Handles large files without loading entire file into memory
- Maintains order automatically
- Best for files 100MB - 2GB

**Protocol Details**:
1. **Header Chunk**: First chunk contains `Blob` with storage reference and metadata
2. **Data Chunks**: Middle chunks contain raw file content (`bytes raw_data`)
3. **Footer Chunk**: Last chunk contains `BlobMetadata` with final SHA256, size, S3 ETag

**Example Usage**:
```typescript
// Send SessionStart first
await stream.queueDocument({
  sessionInfo: {
    case: 'sessionStart',
    value: {
      connectorId: 'connector-123',
      apiKey: 'api-key',
      crawlId: 'crawl-456'
    }
  }
});

// Send header chunk
await stream.queueDocument({
  sessionInfo: {
    case: 'document',
    value: {
      sourceId: 'file.pdf',
      filename: 'file.pdf',
      content: {
        case: 'chunk',
        value: headerChunk  // StreamingChunk with header
      }
    }
  }
});

// Send data chunks in order
for (const chunk of dataChunks) {
  await stream.queueDocument({
    sessionInfo: {
      case: 'document',
      value: {
        content: {
          case: 'chunk',
          value: chunk  // StreamingChunk with raw_data
        }
      }
    }
  });
}

// Send footer chunk
await stream.queueDocument({
  sessionInfo: {
    case: 'document',
    value: {
      content: {
        case: 'chunk',
        value: footerChunk  // StreamingChunk with footer
      }
    }
  }
});

// Complete stream and get final response
const response = await stream.complete();
```

## Method 3: Async Chunked Upload (New Protocol with Redis)

**Use Case**: Very large files, unreliable networks, parallel chunk generation, real streams

**Protocol**: Three-step process with Redis-backed storage for out-of-order chunk handling

**Benefits**: 
- Out-of-order processing
- Retry capability
- Handles network issues gracefully
- Enables real stream reassembly
- Best for files over 2GB or unreliable network conditions

### Step 1: Header Handshake

**RPC**: `startChunkedUpload(StartChunkedUploadRequest) -> StartChunkedUploadResponse`

Returns upload reference/ID needed for subsequent chunk uploads.

**Example**:
```java
StartChunkedUploadRequest request = StartChunkedUploadRequest.newBuilder()
    .setConnectorId("connector-123")
    .setApiKey("api-key")
    .setSourceId("file:///path/to/large-file.zip")
    .setFilename("large-file.zip")
    .setPath("/path/to")
    .setMimeType("application/zip")
    .setExpectedSizeBytes(5L * 1024 * 1024 * 1024)  // 5GB
    .setExpectedChunkCount(500)  // Optional hint
    .build();

StartChunkedUploadResponse response = client.startChunkedUpload(request);
String uploadId = response.getUploadId();  // Use this for chunks
```

### Step 2: Chunk Uploads

**RPC**: `uploadAsyncChunk(AsyncChunkedUploadChunkRequest) -> AsyncChunkedUploadChunkResponse`

Individual chunk uploads can be called out-of-order, as fast as needed, with variable chunk sizes. Chunks are stored in Redis immediately.

**Example**:
```java
// Can be called in parallel, out of order
for (int i = 0; i < totalChunks; i++) {
    AsyncChunkedUploadChunkRequest chunkRequest = AsyncChunkedUploadChunkRequest.newBuilder()
        .setUploadId(uploadId)
        .setChunkNumber(i)
        .setChunkData(ByteString.copyFrom(chunkBytes[i]))
        .setChunkSizeBytes(chunkBytes[i].length)
        .build();
    
    // Fire and forget - can be parallel
    client.uploadAsyncChunk(chunkRequest);
}
```

### Step 3: Footer Completion

**RPC**: `completeChunkedUpload(CompleteChunkedUploadRequest) -> CompleteChunkedUploadResponse`

Signals all chunks have been sent. Includes final SHA256 from sender. Server responds with chunk status (missing/errored/timeout) and reassembles if all chunks are present.

**Example**:
```java
CompleteChunkedUploadRequest request = CompleteChunkedUploadRequest.newBuilder()
    .setUploadId(uploadId)
    .setFinalSha256(calculatedSHA256)
    .setFinalSizeBytes(totalSize)
    .setTotalChunksSent(totalChunks)
    .build();

CompleteChunkedUploadResponse response = client.completeChunkedUpload(request);

if (response.getSuccess()) {
    String documentId = response.getDocumentId();
    String s3Key = response.getS3Key();
} else {
    // Handle missing chunks
    List<Integer> missing = response.getMissingChunksList();
    List<Integer> errored = response.getErroredChunksList();
    List<Integer> needsResend = response.getNeedsResendChunksList();
    
    // Retry missing/errored chunks
    for (int chunkNum : needsResend) {
        // Re-upload chunk
    }
}
```

## Choosing the Right Method

| Method | File Size | Network | Use Case |
|--------|-----------|---------|----------|
| Method 1 (Full File) | < 100MB | Reliable | Simple uploads, small files |
| Method 2 (Sync Chunks) | 100MB - 2GB | Reliable | Standard large file uploads |
| Method 3 (Async Chunks) | > 2GB | Unreliable/Parallel | Very large files, unreliable networks, parallel generation |

## Implementation Notes

- **Method 1** uses existing `processRawDocument` logic
- **Method 2** uses existing `streamDocuments` with `StreamingChunkProcessor`
- **Method 3** uses new `AsyncChunkStorageService` with Redis for chunk storage and reassembly
- All methods support session management and authentication
- All methods integrate with `repository-service` for S3 storage
- All methods support metadata enrichment and document tracking

