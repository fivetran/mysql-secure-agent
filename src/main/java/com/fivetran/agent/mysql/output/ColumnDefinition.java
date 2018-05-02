package com.fivetran.agent.mysql.output;

public class ColumnDefinition {
    public final String name;
    /** The type of the column, as reported by MySQL */
    public final String type;
    /** The primary key, or the first unique index if the primary key is not present */
    public final boolean key;
    // TODO consider putting in sort and dist keys in later versions of agent

    // TODO Ordinal position, character encoding, and set and enum values are necessary for binlog decoding, but not for output

    // TODO Consider writing 2 queries, one for internal and one for external table information

    @Override
    public String toString() {
        return "ColumnDefinition{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", key=" + key +
                '}';
    }

    public ColumnDefinition(String name, String type, boolean key) {
        this.name = name;
        this.type = type;
        this.key = key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnDefinition that = (ColumnDefinition) o;

        if (key != that.key) return false;
        if (!name.equals(that.name)) return false;
        return type.equals(that.type);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + (key ? 1 : 0);
        return result;
    }
}
