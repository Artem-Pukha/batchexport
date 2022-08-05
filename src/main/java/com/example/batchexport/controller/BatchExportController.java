package com.example.batchexport.controller;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.specialized.BlobInputStream;
import com.google.common.io.ByteStreams;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@Controller
@RequestMapping("/api")
public class BatchExportController {

    @Value("${azure.blob.endpoint}")
    private String endpoint;
    @Value("${azure.blob.connection-string}")
    private String connectionString;
    @Value("${azure.blob.name}")
    private String containerName;


    @RequestMapping(value = "/batchExport/azure/report", method = RequestMethod.GET)
    public HttpEntity<byte[]> getReportContentsFromCloud(@RequestParam String name) throws IOException {
        BlobServiceClientBuilder credentialBuilder = new BlobServiceClientBuilder().endpoint(endpoint);
        credentialBuilder.connectionString(connectionString);

        BlobServiceClient blobServiceClient = credentialBuilder.buildClient();
        BlobContainerClient container = blobServiceClient.getBlobContainerClient(containerName);
        BlobClient blobClient = container.getBlobClient(name);

        InputStream raw = blobClient.openInputStream();
        byte[] data = ByteStreams.toByteArray(raw);
        raw.close();

        HttpHeaders header = new HttpHeaders();
        header.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        header.set(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=report.zip");
        header.setContentLength(data.length);
        return new HttpEntity<>(data, header);
    }

    @RequestMapping(value = "/blob/names", method = RequestMethod.GET)
    public List<String> getBlobNamesInAzure() {

        BlobServiceClientBuilder credentialBuilder = new BlobServiceClientBuilder().endpoint(endpoint);
        credentialBuilder.connectionString(connectionString);

        BlobServiceClient blobServiceClient = credentialBuilder.buildClient();
        BlobContainerClient container = blobServiceClient.getBlobContainerClient(containerName);

        List<String> blobUris = new ArrayList<>();
        for (BlobItem listBlobItem : container.listBlobs()) {
            String blobName = listBlobItem.getName();//Get file names in blob
            String uRI = container.getBlobClient(blobName).getBlobUrl(); //Get file URI
            System.out.println("blobNames:" + blobName);
            System.out.println("blobURI:" + uRI);
            blobUris.add(uRI);
        }

            return blobUris;
    }
}
