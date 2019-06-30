package ru.nsg.atol.drivers10.webserver.servlets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;
import ru.atol.drivers10.webserver.Utils;
import ru.atol.drivers10.webserver.db.DBException;
import ru.atol.drivers10.webserver.db.DBInstance;
import ru.atol.drivers10.webserver.db.NotUniqueKeyException;
import ru.atol.drivers10.webserver.entities.Task;
import ru.atol.drivers10.webserver.servlets.JsonTaskListServlet;
import ru.atol.drivers10.webserver.settings.Settings;
import ru.nsg.atol.drivers10.webserver.db.DBInstanceUnit;
import ru.nsg.atol.drivers10.webserver.entities.TaskUnit;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.Set;

public class JsonTaskUnitListServlet extends JsonTaskListServlet {
    private static Logger logger = LogManager.getLogger(JsonTaskUnitListServlet.class);
    public JsonTaskUnitListServlet() {}

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setStatus(201);
        JSONObject json = null;
        String requestJson = null;
        String body = Utils.readFromReader(new BufferedReader(new InputStreamReader(req.getInputStream(), StandardCharsets.UTF_8)));

        try {
            json = (JSONObject) JSONValue.parseWithException(body);
            if (!json.containsKey("uuid") || !json.containsKey("request") || !json.containsKey("unit")) {
                resp.sendError(400, "\"uuid\" and / or \"request\" and / or \"unit\" not found");
                return;
            }
            requestJson = json.get("request").toString();
        } catch (ParseException var12) {
            logger.error(var12.getMessage(), var12);
            resp.sendError(400, var12.getMessage());
            return;
        }

        JSONArray subTasks;
        if (json.get("request") instanceof JSONArray) {
            subTasks = (JSONArray)json.get("request");
        } else {
            subTasks = new JSONArray();
            subTasks.add(json.get("request"));
        }

        int fiscalTasksCount = 0;

        for(int i = 0; i < subTasks.size(); ++i) {
            JSONObject subtask = (JSONObject)subTasks.get(i);
            if (subtask.containsKey("type") && Utils.isFiscalOperation((String)subtask.get("type"))) {
                ++fiscalTasksCount;
            }
        }

        try {
            JSONObject settings = Settings.load();
            Set devices = ((HashMap) settings.get("devices")).keySet();
            final String jsonUnit = String.valueOf(json.get("unit"));
            if (devices.stream().filter(key -> key.equals(jsonUnit.trim())).count() == 0){
                resp.sendError(404, "No \"unit\" " + jsonUnit);
                return;
            }
        } catch (ParseException e) {
            logger.error(e.getMessage(), e);
            resp.sendError(400, e.getMessage());
            return;
        }

        if (fiscalTasksCount > 1) {
            String error = String.format("Too many fiscal sub-tasks - %d", fiscalTasksCount);
            logger.error(error);
            resp.sendError(400, error);
        } else {
            TaskUnit task = new TaskUnit();
            task.setUuid((String)json.get("uuid"));
            task.setUnit((String)json.get("unit"));
            task.setData(requestJson);
            task.setTimestamp(new Date());
            logger.info(String.format("%s %s [%s]", req.getMethod(), req.getRequestURI(), body));

            try {
                DBInstanceUnit.db.addTaskUnit(task);
            } catch (NotUniqueKeyException var10) {
                logger.error(var10.getMessage(), var10);
                resp.sendError(409, var10.getMessage());
            } catch (DBException var11) {
                logger.error(var11.getMessage(), var11);
                resp.sendError(500, var11.getMessage());
            }

            resp.setStatus(201);
        }
    }
}
