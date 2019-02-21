/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.types;

import io.crate.Streamer;
import io.crate.common.collections.MapComparator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ObjectType extends DataType<Map<String, Object>> implements Streamer<Map<String, Object>> {

    public static final ObjectType INSTANCE = new ObjectType();
    public static final int ID = 12;
    public static final String NAME = "object";

    public static class Builder {

        LinkedHashMap<String, DataType> innerTypes = new LinkedHashMap<>();

        public Builder setInnerType(String key, DataType innerType) {
            innerTypes.put(key, innerType);
            return this;
        }

        public ObjectType build() {
            return new ObjectType(innerTypes);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    @Nullable
    private Map<String, DataType> innerTypes;

    /**
     * Constructor used for the {@link org.elasticsearch.common.io.stream.Streamable}
     * interface which initializes the fields after object creation.
     */
    public ObjectType() {
    }

    private ObjectType(@Nullable Map<String, DataType> innerTypes) {
        this.innerTypes = innerTypes;
    }

    public Map<String, DataType> innerTypes() {
        return innerTypes;
    }

    public DataType resolveInnerType(List<String> path) {
        DataType innerType = this;
        for (String el : path) {
            if (innerType.id() == ID) {
                Map<String, DataType> innerTypes = ((ObjectType) innerType).innerTypes;
                if (innerTypes == null) {
                    return UndefinedType.INSTANCE;
                }
                innerType = innerTypes.get(el);
                if (innerType == null) {
                    return UndefinedType.INSTANCE;
                }
            }
        }
        return innerType;
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.ObjectType;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Streamer<Map<String, Object>> streamer() {
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> value(Object value) {
        if (value instanceof String) {
            return mapFromJSONString((String) value);
        }
        return (Map<String, Object>) value;
    }

    @Override
    public Object hashableValue(Object value) throws IllegalArgumentException, ClassCastException {
        if (value instanceof Map) {
            Map<String, Object> m = (Map<String, Object>) value;
            HashMap<String, Object> result = new HashMap<>();
            for (Map.Entry<String, Object> entry : m.entrySet()) {
                result.put(entry.getKey(), hashableValue(entry.getValue()));
            }
            return result;
        } else if (value instanceof Collection) {
            Collection collection = (Collection) value;
            ArrayList<Object> result = new ArrayList<>(collection.size());
            for (Object o : collection) {
                result.add(hashableValue(o));
            }
            return result;
        } else if (value.getClass().isArray()) {
            Object[] arr = (Object[]) value;
            ArrayList<Object> result = new ArrayList<>(arr.length);
            for (Object o : arr) {
                result.add(hashableValue(o));
            }
            return result;
        } else {
            return value;
        }
    }

    private static Map<String,Object> mapFromJSONString(String value) {
        try {
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                value
            );
            return parser.map();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int compareValueTo(Map<String, Object> val1, Map<String, Object> val2) {
        return MapComparator.compareMaps(val1, val2);
    }

    // TODO: require type info from each child and then use typed streamer for contents of the map
    // ?

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> readValueFrom(StreamInput in) throws IOException {
        if (innerTypes == null) {
            return (Map<String, Object>) in.readGenericValue();
        } else {
            int size = in.readVInt();
            HashMap<String, Object> m = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                String key = in.readString();

                DataType innerType = innerTypes.get(key);
                assert innerType != null : "Cannot find inner-type definition for key " + key;

                Object val = innerType.streamer().readValueFrom(in);
                m.put(key, val);
            }
            return m;
        }
    }

    @Override
    public void writeValueTo(StreamOutput out, Map<String, Object> v) throws IOException {
        if (innerTypes == null) {
            out.writeGenericValue(v);
        } else {
            out.writeVInt(v.size());
            for (Map.Entry<String, Object> entry : v.entrySet()) {
                String key = entry.getKey();
                out.writeString(key);

                DataType innerType = innerTypes.get(key);
                assert innerType != null : "Cannot find inner-type definition for key " + key;

                innerType.streamer().writeValueTo(out, entry.getValue());
            }
        }
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            int typesSize = in.readVInt();
            innerTypes = new LinkedHashMap<>(typesSize);
            for (int i = 0; i < typesSize; i++) {
                String key = in.readString();
                DataType type = DataTypes.fromStream(in);
                innerTypes.put(key, type);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (innerTypes == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(innerTypes.size());
            for (Map.Entry<String, DataType> entry : innerTypes.entrySet()) {
                out.writeString(entry.getKey());
                DataTypes.toStream(entry.getValue(), out);
            }
        }
    }
}
