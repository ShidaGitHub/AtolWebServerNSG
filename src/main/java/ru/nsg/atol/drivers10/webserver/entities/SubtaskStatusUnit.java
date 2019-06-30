package ru.nsg.atol.drivers10.webserver.entities;

import javafx.scene.input.DataFormat;
import org.json.simple.JSONObject;
import ru.atol.drivers10.webserver.entities.SubtaskStatus;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class SubtaskStatusUnit extends SubtaskStatus {
    private String uuid;
    private int number;
    private int sendCode;
    private String sendRes;

    public SubtaskStatusUnit(){
        super();
    }

    public String getUuid() {
        return uuid;
    }
    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public int getNumber() {
        return number;
    }
    public void setNumber(int number) {
        this.number = number;
    }

    public int getSendCode() {
        return sendCode;
    }
    public void setSendCode(int sendCode) {
        this.sendCode = sendCode;
    }

    public String getSendRes() {
        return sendRes;
    }
    public void setSendRes(String sendRes) {
        this.sendRes = sendRes;
    }

    public String getJsonString(){
        JSONObject object = new JSONObject();
        object.put("uuid", getUuid());
        object.put("number", getNumber());
        DateFormat df = new SimpleDateFormat("yyyy.MM.dd");
        object.put("timestamp", df.format(getTimestamp()));
        object.put("status", getStatus());
        object.put("errorCode", getErrorCode());
        object.put("errorDescription", getErrorDescription());
        object.put("resultData", getResultData());
        return object.toJSONString();
    }
}
