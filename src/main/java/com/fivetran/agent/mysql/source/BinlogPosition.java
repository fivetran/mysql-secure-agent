/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.source;

public class BinlogPosition {
    public final String file;
    public final long position;

    public BinlogPosition() {
        file = null;
        position = 0;
    }

    public BinlogPosition(String file, long position) {
        this.file = file;
        this.position = position;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BinlogPosition that = (BinlogPosition) o;

        if (position != that.position) return false;
        if (!file.equals(that.file)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = file.hashCode();
        result = 31 * result + (int) (position ^ (position >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "BinlogPosition{" +
                "file='" + file + '\'' +
                ", position=" + position +
                '}';
    }
}