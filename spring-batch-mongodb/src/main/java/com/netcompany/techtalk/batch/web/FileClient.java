package com.netcompany.techtalk.batch.web;

import com.netcompany.techtalk.batch.step.*;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**Created by Pawel Gawedzki on 23-Jan-2019.*/
class FileClient implements DataClient {

    private static Logger logger = LoggerFactory.getLogger(FileClient.class);

    @Override
    public String getSchemaJson(QueryColumnBuilder queryColumnBuilder, String url) throws Exception {
        Path jsonSchemaPath = getSchemaFilePath(queryColumnBuilder);
        logger.info("Json schema file loaded [{}]", jsonSchemaPath.toString());
        return IOUtils.toString(jsonSchemaPath.toUri(), "UTF-8");
    }

    @Override
    public void downloadDataToFile(UrlBuilder urlBuilder, String filePath) throws Exception {
        Path csvFilePath = getCsvFilePath(filePath);
        logger.info("Path to csv resource found [{}].", csvFilePath.toString());
        Path csvDirectory = Paths.get(filePath.substring(0, filePath.lastIndexOf("/")));
        if (!Files.exists(csvDirectory)) {
            Files.createDirectory(csvDirectory);
        }
        Files.copy(csvFilePath, Paths.get(filePath));
        logger.info("File downloaded to directory [{}].", filePath);
    }

    private Path getSchemaFilePath(QueryColumnBuilder queryColumnBuilder) {
        Path path = null;
        String query = queryColumnBuilder.build();
        if (query.contains(OperationalRouteStep.DATA_SET_NAME)) {
            path = Paths.get("data", "operationalRoutesSchema.json");
        } else if (query.contains(EquipmentCargoStep.DATA_SET_NAME)) {
            path = Paths.get("data", "equipmentCargoSchema.json");
        } else if (query.contains(CargoConditioningStep.DATA_SET_NAME)) {
            path = Paths.get("data", "cargoConditioningSchema.json");
        } else if (query.contains(HaulageInfoStep.DATA_SET_NAME)) {
            path = Paths.get("data", "haulageInfoSchema.json");
        } else if (query.contains(HaulageEquipmentStep.DATA_SET_NAME)) {
            path = Paths.get("data", "haulageEquipmentsSchema.json");
        } else if (query.contains(TrackingBookingStep.DATA_SET_NAME)) {
            path = Paths.get("data", "truckingBookingsSchema.json");
        }
        return path;
    }

    private Path getCsvFilePath(String filePath) {
        Path path = null;
        if (filePath.contains(OperationalRouteStep.DATA_SET_NAME)) {
            path = Paths.get("data", "operationalRoutes.csv");
        } else if (filePath.contains(EquipmentCargoStep.DATA_SET_NAME)) {
            path = Paths.get("data", "equipmentCargo.csv");
        } else if (filePath.contains(CargoConditioningStep.DATA_SET_NAME)) {
            path = Paths.get("data", "cargoConditioning.csv");
        } else if (filePath.contains(HaulageInfoStep.DATA_SET_NAME)) {
            path = Paths.get("data", "haulageInfo.csv");
        } else if (filePath.contains(HaulageEquipmentStep.DATA_SET_NAME)) {
            path = Paths.get("data", "haulageEquipments.csv");
        } else if (filePath.contains(TrackingBookingStep.DATA_SET_NAME)) {
            path = Paths.get("data", "truckingBookings.csv");
        }
        return path;
    }
}
