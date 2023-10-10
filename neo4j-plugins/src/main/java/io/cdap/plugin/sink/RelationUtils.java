/*
 * Copyright © 2023 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.sink;

import io.cdap.plugin.sink.objects.RelationDto;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RelationUtils {
    private static final String COMMA_DELIMITER = ",";
    private static final String PIPE_DELIMITER = "|";
    public static String serialize(RelationDto relationDto) {
        return String.join(PIPE_DELIMITER, relationDto.getElementName(),
                relationDto.getDirection(), relationDto.getRelationName());
    }

    public static String serialize(List<RelationDto> relations) {
        return relations.stream().map(RelationUtils::serialize).collect(Collectors.joining(COMMA_DELIMITER));
    }

    public static RelationDto deserialize(String hash) {
        String[] items = hash.split("\\|");
        if (items.length != 3) {
            throw new IllegalArgumentException("Passed hash as serialized RelationDto is invalid: " + hash);
        }
        return RelationDto.RelationDtoBuilder.aRelationObj()
                .withElementName(items[0])
                .withDirection(items[1])
                .withRelationName(items[2])
                .build();
    }

    public static List<RelationDto> deserializeList(String relationsStr) {
        return Stream.of(relationsStr.split(COMMA_DELIMITER))
                .map(RelationUtils::deserialize).collect(Collectors.toList());
    }
}
