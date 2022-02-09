package com.example.gcpstoragebucketpoc.web;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.springframework.http.HttpHeaders.CONTENT_DISPOSITION;
import static org.springframework.http.MediaType.TEXT_PLAIN;
import static org.springframework.http.MediaType.TEXT_PLAIN_VALUE;

@RestController
@RequestMapping
@Slf4j
public class BucketController {

    final Storage storage;

    // nome do bucket a ser parametrizado
    private static final String BUCKET_NAME = "furnarius-rufus-bucket";

    // path para o csv de teste
    private static final String FILE_PATH = "csv/test-file.csv";

    public BucketController(Storage storage) {
        this.storage = storage;
    }

    @PostMapping(value = "/{partnerHash}/{fileName}")
    @ResponseStatus(HttpStatus.CREATED)
    public void uploadLargeFile(@PathVariable String partnerHash, @PathVariable String fileName) {
        log.info("START - upload large file");

        final BlobId blobId = getBlobId(partnerHash, fileName);
        final BlobInfo blobInfo = getBlobInfo(blobId);
        byte[] content = getContent();
        tryUpload(content, blobInfo);

        log.info("END - upload large file");
    }

    @GetMapping(value = "/{partnerHash}/{fileName}")
    public ResponseEntity<byte[]> downloadFile(
            @PathVariable String partnerHash,
            @PathVariable String fileName) {
        log.info("START - download file");

        final BlobId blobId = getBlobId(partnerHash, fileName);
        final Blob blob = storage.get(blobId);
        return ResponseEntity.ok().contentType(TEXT_PLAIN)
                .header(CONTENT_DISPOSITION, "attachment;filename=" + fileName)
                .body(blob.getContent(Blob.BlobSourceOption.generationMatch()));
    }

    private void tryUpload(byte[] content, BlobInfo blobInfo) {
        try (WriteChannel writer = storage.writer(blobInfo)) {
            writer.write(ByteBuffer.wrap(content, 0, content.length));
        } catch (Exception ex) {
            log.error(ex.getMessage());
        }
    }

    private BlobInfo getBlobInfo(BlobId blobId) {
        return BlobInfo.newBuilder(blobId).setContentType(TEXT_PLAIN_VALUE).build();
    }

    private BlobId getBlobId(String partnerHash, String fileName) {
        return BlobId.of(BUCKET_NAME, getFullQualifiedName(partnerHash, fileName));
    }

    @SneakyThrows
    private byte[] getContent() {
        return Files.readAllBytes(Paths.get(FILE_PATH));
    }

    private String getFullQualifiedName(final String folderName, final String filename) {
        return folderName.concat("/").concat(filename);
    }
}
