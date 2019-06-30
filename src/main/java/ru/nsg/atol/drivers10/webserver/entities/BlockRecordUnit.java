package ru.nsg.atol.drivers10.webserver.entities;

import ru.atol.drivers10.webserver.entities.BlockRecord;

public class BlockRecordUnit extends BlockRecord {
    private String unit;

    public BlockRecordUnit(String uuid, long documentNumber, String unit) {
        super(uuid, documentNumber);
        this.unit = unit;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }
}
