package ru.nsg.atol.drivers10.webserver.entities;

import ru.atol.drivers10.webserver.entities.Task;

public class TaskUnit extends Task {
    private String unit;

    public TaskUnit(){
        super();
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }
}
