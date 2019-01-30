package com.netcompany.techtalk.batch.mapper;

import org.apache.commons.io.IOUtils;
import org.codehaus.jettison.json.JSONException;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;

/**Created by Pawel Gawedzki on 23-Jan-2019.*/
public class ColumnMapperTest {

    @Test
    public void givenCargoConditioningSchema_whenMap_thenReturnMapColumnType() throws IOException, JSONException {
        Path path = Paths.get("data", "cargoConditioningSchema.json");
        String json = IOUtils.toString(path.toUri(), "UTF-8");

        Map<String, String> map = ColumnMapper.map(json);

        assertThat(map.size(), is(equalTo(55)));
    }
}