package ru.nsg.atol.drivers10.webserver.servlets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import ru.atol.drivers10.webserver.Utils;
import ru.atol.drivers10.webserver.db.DBException;
import ru.atol.drivers10.webserver.db.DBInstance;
import ru.atol.drivers10.webserver.db.NotUniqueKeyException;
import ru.atol.drivers10.webserver.entities.SubtaskStatus;
import ru.atol.drivers10.webserver.entities.Task;
import ru.atol.drivers10.webserver.servlets.JsonTaskServlet;
import ru.nsg.atol.drivers10.webserver.db.DBInstanceUnit;
import ru.nsg.atol.drivers10.webserver.entities.TaskUnit;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class JsonTaskUnitServlet extends JsonTaskServlet {
    private static Logger logger = LogManager.getLogger(JsonTaskUnitServlet.class);

    public JsonTaskUnitServlet(){}

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        List subTasks = null;

        try {
            String uuid = req.getPathInfo().split("/")[1];
            subTasks = DBInstanceUnit.db.getTaskUnitStatus(uuid);
        } catch (NotUniqueKeyException var12) {
            logger.error(var12.getMessage(), var12);
            resp.sendError(409, var12.getMessage());
            return;
        } catch (DBException var13) {
            logger.error(var13.getMessage(), var13);
            resp.sendError(500, var13.getMessage());
            return;
        } catch (ArrayIndexOutOfBoundsException var14) {
            logger.error(var14.getMessage(), var14);
            resp.sendError(404, "No UUID");
            return;
        }

        if (subTasks == null || subTasks.isEmpty()) {
            resp.sendError(404);
        } else {
            logger.info(String.format("%s %s", req.getMethod(), req.getRequestURI()));
            JSONArray results = new JSONArray();
            Iterator var5 = subTasks.iterator();

            while(var5.hasNext()) {
                SubtaskStatus s = (SubtaskStatus)var5.next();
                JSONObject o = new JSONObject();
                o.put("status", Utils.getStatusString(s.getStatus()));
                o.put("errorCode", s.getErrorCode());
                o.put("errorDescription", s.getErrorDescription());
                JSONParser parser = new JSONParser();
                JSONObject subTaskResult = null;

                try {
                    subTaskResult = (JSONObject)parser.parse(s.getResultData());
                } catch (ParseException var11) {
                }

                o.put("result", subTaskResult);
                results.add(o);
            }

            JSONObject response = new JSONObject();
            response.put("results", results);
            resp.setContentType("application/json");
            resp.setCharacterEncoding("UTF-8");
            resp.getWriter().write(response.toJSONString());
            logger.info(String.format("%d %s", resp.getStatus(), response.toJSONString()));
        }
    }

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        List subTasks = null;

        TaskUnit task;
        String uuid;
        try {
            uuid = req.getPathInfo().split("/")[1];
            subTasks = DBInstanceUnit.db.getTaskUnitStatus(uuid);
            task = DBInstanceUnit.db.getTaskUnit(uuid);
        } catch (NotUniqueKeyException var11) {
            logger.error(var11.getMessage(), var11);
            resp.sendError(409, var11.getMessage());
            return;
        } catch (DBException var12) {
            logger.error(var12.getMessage(), var12);
            resp.sendError(500, var12.getMessage());
            return;
        } catch (ArrayIndexOutOfBoundsException var13) {
            logger.error(var13.getMessage(), var13);
            resp.sendError(404, "No UUID");
            return;
        }

        if (subTasks != null && task != null) {
            logger.info(String.format("%s %s", req.getMethod(), req.getRequestURI()));
            if (task.isReady()) {
                resp.sendError(405, "Task done or canceled");
            } else {
                for(int i = 0; i < subTasks.size(); ++i) {
                    if (((SubtaskStatus)subTasks.get(i)).getStatus() != 0) {
                        resp.sendError(405, "Task in progress");
                        return;
                    }
                }

                try {
                    DBInstanceUnit.db.cancelTaskUnit(task.getUuid());
                } catch (DBException var10) {
                    logger.error(var10.getMessage(), var10);
                }

                SubtaskStatus status = new SubtaskStatus();
                status.setStatus(6);

                for(int i = 0; i < subTasks.size(); ++i) {
                    try {
                        DBInstanceUnit.db.updateSubTaskStatus(uuid, i, status);
                    } catch (DBException var9) {
                        logger.error(var9.getMessage(), var9);
                    }
                }

                resp.setStatus(200);
            }
        } else {
            resp.sendError(404);
        }
    }
}
