package com.format.csvformat;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Objects;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.csv.CsvRowSchemaConverter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

@PublicEvolving
public final class CsvCustomRowDeserializationSchema implements DeserializationSchema<Row> {
    private static final long serialVersionUID = 2135553495874539201L;
    private final TypeInformation<Row> typeInfo;
    private RowTypeInfo rowTypeInfo;
    private CsvCustomRowDeserializationSchema.RuntimeConverter runtimeConverter;
    private final CsvSchema csvSchema;
    private final ObjectReader objectReader;
    private final boolean ignoreParseErrors;

    private CsvCustomRowDeserializationSchema(RowTypeInfo typeInfo, CsvSchema csvSchema, boolean ignoreParseErrors) {
        this.typeInfo = typeInfo;
        this.rowTypeInfo = typeInfo;
        this.runtimeConverter = createRowRuntimeConverter(typeInfo, ignoreParseErrors, true);
        this.csvSchema = csvSchema;
        this.objectReader = (new CsvMapper()).readerFor(JsonNode.class).with(csvSchema);
        this.ignoreParseErrors = ignoreParseErrors;
    }

    public Row deserialize(byte[] message) throws IOException {
        try {
            JsonNode root = (JsonNode) this.objectReader.readValue(message);
            System.out.println(root);
            return (Row) this.runtimeConverter.convert(root);
        } catch (Throwable var3) {
            Row row = null;
            try {
                String[] arr = new String(message).split(",");
                TypeInformation<?>[] fieldTypes = rowTypeInfo.getFieldTypes();
                String[] fieldNames = rowTypeInfo.getFieldNames();
                new Row(fieldNames.length);
                for (int i = 0; i < fieldTypes.length; i++) {
                    if (fieldTypes[i].equals(Types.STRING)) {
                        row.setField(i, arr[i]);
                    } else if (fieldTypes[i].equals(Types.LONG)) {
                        row.setField(i, Long.valueOf(arr[i]));
                    } else if (fieldTypes[i].equals(Types.INT)) {
                        row.setField(i, Integer.valueOf(arr[i]));
                    }
                }
                return row;
            } catch (Throwable var4) {
                if (this.ignoreParseErrors) {
                    return row;
                } else {
                    throw new IOException("Failed to deserialize CSV row '" + new String(message) + "'.", var3);
                }
            }
        }
    }

    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    public TypeInformation<Row> getProducedType() {
        return this.typeInfo;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && o.getClass() == this.getClass()) {
            CsvCustomRowDeserializationSchema that = (CsvCustomRowDeserializationSchema) o;
            CsvSchema otherSchema = that.csvSchema;
            return this.typeInfo.equals(that.typeInfo) && this.ignoreParseErrors == that.ignoreParseErrors && this.csvSchema.getColumnSeparator() == otherSchema.getColumnSeparator() && this.csvSchema.allowsComments() == otherSchema.allowsComments() && this.csvSchema.getArrayElementSeparator().equals(otherSchema.getArrayElementSeparator()) && this.csvSchema.getQuoteChar() == otherSchema.getQuoteChar() && this.csvSchema.getEscapeChar() == otherSchema.getEscapeChar() && Arrays.equals(this.csvSchema.getNullValue(), otherSchema.getNullValue());
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(new Object[]{this.typeInfo, this.ignoreParseErrors, this.csvSchema.getColumnSeparator(), this.csvSchema.allowsComments(), this.csvSchema.getArrayElementSeparator(), this.csvSchema.getQuoteChar(), this.csvSchema.getEscapeChar(), this.csvSchema.getNullValue()});
    }

    private static CsvCustomRowDeserializationSchema.RuntimeConverter createRowRuntimeConverter(RowTypeInfo rowTypeInfo, boolean ignoreParseErrors, boolean isTopLevel) {
        TypeInformation<?>[] fieldTypes = rowTypeInfo.getFieldTypes();
        String[] fieldNames = rowTypeInfo.getFieldNames();
        CsvCustomRowDeserializationSchema.RuntimeConverter[] fieldConverters = createFieldRuntimeConverters(ignoreParseErrors, fieldTypes);
        return assembleRowRuntimeConverter(ignoreParseErrors, isTopLevel, fieldNames, fieldConverters);
    }

    static CsvCustomRowDeserializationSchema.RuntimeConverter[] createFieldRuntimeConverters(boolean ignoreParseErrors, TypeInformation<?>[] fieldTypes) {
        CsvCustomRowDeserializationSchema.RuntimeConverter[] fieldConverters = new CsvCustomRowDeserializationSchema.RuntimeConverter[fieldTypes.length];

        for (int i = 0; i < fieldTypes.length; ++i) {
            fieldConverters[i] = createNullableRuntimeConverter(fieldTypes[i], ignoreParseErrors);
        }

        return fieldConverters;
    }

    private static CsvCustomRowDeserializationSchema.RuntimeConverter assembleRowRuntimeConverter(boolean ignoreParseErrors, boolean isTopLevel, String[] fieldNames, CsvCustomRowDeserializationSchema.RuntimeConverter[] fieldConverters) {
        int rowArity = fieldNames.length;
        return (node) -> {
            int nodeSize = node.size();
            validateArity(rowArity, nodeSize, ignoreParseErrors);
            Row row = new Row(rowArity);
            for (int i = 0; i < Math.min(rowArity, nodeSize); ++i) {
                if (isTopLevel) {
                    row.setField(i, fieldConverters[i].convert(node.get(fieldNames[i])));
                } else {
                    row.setField(i, fieldConverters[i].convert(node.get(i)));
                }
            }

            return row;
        };
    }

    private static CsvCustomRowDeserializationSchema.RuntimeConverter createNullableRuntimeConverter(TypeInformation<?> info, boolean ignoreParseErrors) {
        CsvCustomRowDeserializationSchema.RuntimeConverter valueConverter = createRuntimeConverter(info, ignoreParseErrors);
        return (node) -> {
            if (node.isNull()) {
                return null;
            } else {
                try {
                    return valueConverter.convert(node);
                } catch (Throwable var4) {
                    if (!ignoreParseErrors) {
                        throw var4;
                    } else {
                        return null;
                    }
                }
            }
        };
    }

    private static CsvCustomRowDeserializationSchema.RuntimeConverter createRuntimeConverter(TypeInformation<?> info, boolean ignoreParseErrors) {
        if (info.equals(Types.VOID)) {
            return (node) -> {
                return null;
            };
        } else if (info.equals(Types.STRING)) {
            return JsonNode::asText;
        } else if (info.equals(Types.BOOLEAN)) {
            return (node) -> {
                return Boolean.valueOf(node.asText().trim());
            };
        } else if (info.equals(Types.BYTE)) {
            return (node) -> {
                return Byte.valueOf(node.asText().trim());
            };
        } else if (info.equals(Types.SHORT)) {
            return (node) -> {
                return Short.valueOf(node.asText().trim());
            };
        } else if (info.equals(Types.INT)) {
            return (node) -> {
                return Integer.valueOf(node.asText().trim());
            };
        } else if (info.equals(Types.LONG)) {
            return (node) -> {
                return Long.valueOf(node.asText().trim());
            };
        } else if (info.equals(Types.FLOAT)) {
            return (node) -> {
                return Float.valueOf(node.asText().trim());
            };
        } else if (info.equals(Types.DOUBLE)) {
            return (node) -> {
                return Double.valueOf(node.asText().trim());
            };
        } else if (info.equals(Types.BIG_DEC)) {
            return (node) -> {
                return new BigDecimal(node.asText().trim());
            };
        } else if (info.equals(Types.BIG_INT)) {
            return (node) -> {
                return new BigInteger(node.asText().trim());
            };
        } else if (info.equals(Types.SQL_DATE)) {
            return (node) -> {
                return Date.valueOf(node.asText());
            };
        } else if (info.equals(Types.SQL_TIME)) {
            return (node) -> {
                return Time.valueOf(node.asText());
            };
        } else if (info.equals(Types.SQL_TIMESTAMP)) {
            return (node) -> {
                return Timestamp.valueOf(node.asText());
            };
        } else if (info.equals(Types.LOCAL_DATE)) {
            return (node) -> {
                return Date.valueOf(node.asText()).toLocalDate();
            };
        } else if (info.equals(Types.LOCAL_TIME)) {
            return (node) -> {
                return Time.valueOf(node.asText()).toLocalTime();
            };
        } else if (info.equals(Types.LOCAL_DATE_TIME)) {
            return (node) -> {
                return Timestamp.valueOf(node.asText()).toLocalDateTime();
            };
        } else if (info instanceof RowTypeInfo) {
            RowTypeInfo rowTypeInfo = (RowTypeInfo) info;
            return createRowRuntimeConverter(rowTypeInfo, ignoreParseErrors, false);
        } else if (info instanceof BasicArrayTypeInfo) {
            return createObjectArrayRuntimeConverter(((BasicArrayTypeInfo) info).getComponentInfo(), ignoreParseErrors);
        } else if (info instanceof ObjectArrayTypeInfo) {
            return createObjectArrayRuntimeConverter(((ObjectArrayTypeInfo) info).getComponentInfo(), ignoreParseErrors);
        } else if (info instanceof PrimitiveArrayTypeInfo && ((PrimitiveArrayTypeInfo) info).getComponentType() == Types.BYTE) {
            return createByteArrayRuntimeConverter(ignoreParseErrors);
        } else {
            throw new RuntimeException("Unsupported type information '" + info + "'.");
        }
    }

    private static CsvCustomRowDeserializationSchema.RuntimeConverter createObjectArrayRuntimeConverter(TypeInformation<?> elementType, boolean ignoreParseErrors) {
        Class<?> elementClass = elementType.getTypeClass();
        CsvCustomRowDeserializationSchema.RuntimeConverter elementConverter = createNullableRuntimeConverter(elementType, ignoreParseErrors);
        return (node) -> {
            int nodeSize = node.size();
            Object[] array = (Object[]) ((Object[]) Array.newInstance(elementClass, nodeSize));

            for (int i = 0; i < nodeSize; ++i) {
                array[i] = elementConverter.convert(node.get(i));
            }

            return array;
        };
    }

    private static CsvCustomRowDeserializationSchema.RuntimeConverter createByteArrayRuntimeConverter(boolean ignoreParseErrors) {
        return (node) -> {
            try {
                return node.binaryValue();
            } catch (IOException var3) {
                if (!ignoreParseErrors) {
                    throw new RuntimeException("Unable to deserialize byte array.", var3);
                } else {
                    return null;
                }
            }
        };
    }

    static void validateArity(int expected, int actual, boolean ignoreParseErrors) {
        if (expected != actual && !ignoreParseErrors) {
            throw new RuntimeException("Row length mismatch. " + expected + " fields expected but was " + actual + ".");
        }
    }

    interface RuntimeConverter extends Serializable {
        Object convert(JsonNode var1);
    }

    @PublicEvolving
    public static class Builder {
        private final RowTypeInfo typeInfo;
        private CsvSchema csvSchema;
        private boolean ignoreParseErrors;

        public Builder(TypeInformation<Row> typeInfo) {
            Preconditions.checkNotNull(typeInfo, "Type information must not be null.");
            if (!(typeInfo instanceof RowTypeInfo)) {
                throw new IllegalArgumentException("Row type information expected.");
            } else {
                this.typeInfo = (RowTypeInfo) typeInfo;
                this.csvSchema = CsvRowSchemaConverter.convert((RowTypeInfo) typeInfo);
            }
        }

        public CsvCustomRowDeserializationSchema.Builder setFieldDelimiter(char delimiter) {
            this.csvSchema = this.csvSchema.rebuild().setColumnSeparator(delimiter).build();
            return this;
        }

        public CsvCustomRowDeserializationSchema.Builder setAllowComments(boolean allowComments) {
            this.csvSchema = this.csvSchema.rebuild().setAllowComments(allowComments).build();
            return this;
        }

        public CsvCustomRowDeserializationSchema.Builder setArrayElementDelimiter(String delimiter) {
            Preconditions.checkNotNull(delimiter, "Array element delimiter must not be null.");
            this.csvSchema = this.csvSchema.rebuild().setArrayElementSeparator(delimiter).build();
            return this;
        }

        public CsvCustomRowDeserializationSchema.Builder setQuoteCharacter(char c) {
            this.csvSchema = this.csvSchema.rebuild().setQuoteChar(c).build();
            return this;
        }

        public CsvCustomRowDeserializationSchema.Builder setEscapeCharacter(char c) {
            this.csvSchema = this.csvSchema.rebuild().setEscapeChar(c).build();
            return this;
        }

        public CsvCustomRowDeserializationSchema.Builder setNullLiteral(String nullLiteral) {
            Preconditions.checkNotNull(nullLiteral, "Null literal must not be null.");
            this.csvSchema = this.csvSchema.rebuild().setNullValue(nullLiteral).build();
            return this;
        }

        public CsvCustomRowDeserializationSchema.Builder setIgnoreParseErrors(boolean ignoreParseErrors) {
            this.ignoreParseErrors = ignoreParseErrors;
            return this;
        }

        public CsvCustomRowDeserializationSchema build() {
            return new CsvCustomRowDeserializationSchema(this.typeInfo, this.csvSchema, this.ignoreParseErrors);
        }
    }
}

