package ru.nsg.atol.drivers10.webserver.workers;

import javafx.scene.input.DataFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import ru.atol.drivers10.fptr.Fptr;
import ru.atol.drivers10.fptr.IFptr;
import ru.atol.drivers10.webserver.Utils;
import ru.atol.drivers10.webserver.db.DBException;
import ru.atol.drivers10.webserver.db.DBInstance;
import ru.atol.drivers10.webserver.entities.BlockRecord;
import ru.atol.drivers10.webserver.entities.SubtaskStatus;
import ru.atol.drivers10.webserver.entities.Task;
import ru.atol.drivers10.webserver.settings.Settings;
import ru.atol.drivers10.webserver.workers.DriverWorker;
import ru.nsg.atol.drivers10.webserver.db.DBInstanceUnit;
import ru.nsg.atol.drivers10.webserver.entities.BlockRecordUnit;
import ru.nsg.atol.drivers10.webserver.entities.SubtaskStatusUnit;
import ru.nsg.atol.drivers10.webserver.entities.TaskUnit;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

public class DriverWorkerUnit extends DriverWorker {
    private static Logger logger = LogManager.getLogger(DriverWorkerUnit.class);
    private IFptr fptr = new Fptr();
    private HashMap<Integer, Boolean> timeQuery = new HashMap<>();
    private LocalDateTime startDate;

    public DriverWorkerUnit(String name) {
        super();
        setName(name);
        startDate = LocalDateTime.now();

        for (int i = 0; i <=24; i++){
            timeQuery.put(i, false);
        }
    }

    private String loadDriverSettings() throws IOException, ParseException {
        JSONObject settings = (JSONObject) Settings.load().get("devices");
        settings = (JSONObject)settings.get(getName());
        settings.put("Model", String.valueOf(500));
        return settings.toString();
    }

    public void run() {
        boolean opened = false;
        short sleepTimeout = 100;

        while(!this.isInterrupted()) {
            try {
                Thread.sleep((long)sleepTimeout);
            } catch (InterruptedException var18) {
                return;
            }

            if (!opened) {
                try {
                    fptr.setSettings(loadDriverSettings());
                } catch (Exception var17) {
                    var17.printStackTrace();
                    return;
                }

                this.fptr.open();
                opened = this.fptr.isOpened();
            }

            if (!opened) {
                sleepTimeout = 5000;
                SimpleDateFormat dateFormat0 = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
                if(ChronoUnit.SECONDS.between(startDate, LocalDateTime.now()) > 60 * 5) { //5 минут на первоначальную загрузку
                    //Раз в час отослать предупреждение
                    if (!timeQuery.get(LocalTime.now().getHour())) {
                        IntStream.rangeClosed(0, 24).forEachOrdered(h -> timeQuery.put(h, false));
                        timeQuery.put(LocalTime.now().getHour(), true);

                        SubtaskStatusUnit driverChecker = new SubtaskStatusUnit();
                        driverChecker.setTimestamp(new Date());
                        driverChecker.setStatus(2);
                        driverChecker.setErrorCode(431);
                        SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
                        driverChecker.setErrorDescription("Поток " + getName() + " касса не отвечает - " + dateFormat.format(new Date()));
                        logger.info("Поток {} касса не отвечает - {}", getName(), dateFormat.format(new Date()));
                        driverChecker.setResultData("");
                        driverChecker.setUuid(UUID.randomUUID().toString());
                        driverChecker.setNumber(0);
                        driverChecker.setSendCode(0);
                        driverChecker.setSendRes("");

                        try {
                            DBInstanceUnit.db.addSubTaskUnitOnly(driverChecker);
                        } catch (DBException e) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                }
            } else {
                sleepTimeout = 100;

                int i;
                SubtaskStatus status;
                JSONObject subtask;
                try {
                    BlockRecordUnit blockState = DBInstanceUnit.db.getBlockUnitState(DriverWorkerUnit.this.getName());
                    if (!blockState.getUuid().isEmpty()) {
                        logger.info(String.format("Обнаружена блокировка очереди задачей '%s'", blockState.getUuid()));
                        this.fptr.setParam(65622, 5L);
                        if (this.fptr.fnQueryData() != 0) {
                            logger.warn("Не удалось восстановить состояние, продолжаем попытки...");
                            continue;
                        }

                        boolean closed = this.fptr.getParamInt(65598) > blockState.getDocumentNumber();
                        this.fptr.continuePrint();
                        logger.info(String.format("Соединение восстановленно, задача '%s' %s", blockState.getUuid(), closed ? "выполнена" : "не выполнена"));
                        List<SubtaskStatus> subtasks = DBInstanceUnit.db.getTaskUnitStatus(blockState.getUuid());
                        TaskUnit task = DBInstanceUnit.db.getTaskUnit(blockState.getUuid());

                        for(i = 0; i < subtasks.size(); ++i) {
                            status = subtasks.get(i);
                            if (status.getStatus() == 5) {
                                status.setStatus(closed ? 2 : 3);
                                if (closed) {
                                    status.setErrorCode(0);
                                    status.setErrorDescription("Ошибок нет");
                                    subtask = new JSONObject();
                                    subtask.put("type", "getLastFiscalParams");
                                    subtask.put("forReceipt", Utils.isReceipt((String)((JSONObject)task.getDataJson().get(i)).get("type")));
                                    this.fptr.setParam(65645, subtask.toString());
                                    if (this.fptr.processJson() == 0) {
                                        status.setResultData(this.fptr.getParamString(65645));
                                    }
                                }

                                DBInstanceUnit.db.updateSubTaskStatus(blockState.getUuid(), i, status);
                                break;
                            }
                        }

                        logger.info("Обработка задачи {} завершена, разблокируем очередь в {}", blockState.getUuid(), DriverWorkerUnit.this.getName());
                        DBInstanceUnit.db.setTaskUnitReady(blockState.getUuid());
                        DBInstanceUnit.db.unblockDBUnit(DriverWorkerUnit.this.getName());
                    }
                } catch (DBException var20) {
                    logger.error(var20.getMessage(), var20);
                    continue;
                }

                TaskUnit task;
                try {
                    task = DBInstanceUnit.db.getNextTaskUnit(DriverWorkerUnit.this.getName());
                } catch (DBException var19) {
                    logger.error(var19.getMessage(), var19);
                    continue;
                }

                if (task != null) {
                    logger.info(String.format("Найдена задача с id = '%s' в '%s'", task.getUuid(), task.getUnit()));
                    JSONArray subTasks = task.getDataJson();
                    if (subTasks == null) {
                        logger.error("Ошибка разбора JSON");
                    } else {
                        logger.info(String.format("Подзадач - %d", subTasks.size()));
                        boolean wasError = false;
                        boolean blocked = false;

                        for(i = 0; i < subTasks.size(); ++i) {
                            logger.info(String.format("Подзадача #%d...", i + 1));
                            status = new SubtaskStatus();
                            status.setStatus(1);

                            try {
                                DBInstanceUnit.db.updateSubTaskStatus(task.getUuid(), i, status);
                            } catch (DBException var16) {
                                logger.error(var16.getMessage(), var16);
                            }

                            if (wasError) {
                                status.setStatus(4);
                                status.setErrorCode(502);
                                status.setErrorDescription("Выполнение прервано из-за предыдущих ошибок");
                            } else {
                                subtask = (JSONObject)subTasks.get(i);
                                long lastDocumentNumber = -1L;
                                if (subtask.containsKey("type") && Utils.isFiscalOperation((String)subtask.get("type"))) {
                                    this.fptr.setParam(65622, 5L);
                                    if (this.fptr.fnQueryData() < 0) {
                                        status.setStatus(3);
                                        status.setErrorCode(this.fptr.errorCode());
                                        status.setErrorDescription(this.fptr.errorDescription());
                                        wasError = true;
                                    }

                                    lastDocumentNumber = this.fptr.getParamInt(65598);
                                }

                                if (!wasError) {
                                    this.fptr.setParam(65645, subtask.toJSONString());
                                    if (this.fptr.processJson() >= 0) {
                                        status.setStatus(2);
                                        status.setResultData(this.fptr.getParamString(65645));
                                    } else {
                                        if (isNeedBlock(this.fptr.errorCode()) && lastDocumentNumber != -1L) {
                                            try {
                                                DBInstanceUnit.db.blockDBU(new BlockRecordUnit(task.getUuid(), lastDocumentNumber, DriverWorkerUnit.this.getName()));
                                            } catch (DBException var15) {
                                                logger.error(var15.getMessage(), var15);
                                            }

                                            blocked = true;
                                            status.setStatus(5);
                                        } else {
                                            status.setStatus(3);
                                        }

                                        wasError = true;
                                    }

                                    status.setErrorCode(this.fptr.errorCode());
                                    status.setErrorDescription(this.fptr.errorDescription());
                                }
                            }

                            try {
                                switch(status.getStatus()) {
                                    case 2:
                                        logger.info(String.format("Подзадача #%d выполнена без ошибок", i + 1));
                                        break;
                                    case 3:
                                        logger.info(String.format("Подзадача #%d завершена с ошибкой", i + 1));
                                        break;
                                    case 4:
                                        logger.info(String.format("Подзадача #%d прервана по причине предыдущих ошибок", i + 1));
                                        break;
                                    case 5:
                                        logger.info(String.format("Подзадача #%d заблокировала очередь", i + 1));
                                }

                                DBInstanceUnit.db.updateSubTaskStatus(task.getUuid(), i, status);
                            } catch (DBException var14) {
                                logger.error(var14.getMessage(), var14);
                            }
                        }

                        if (!blocked) {
                            try {
                                DBInstanceUnit.db.setTaskUnitReady(task.getUuid());
                                logger.info(String.format("Обработка задачи '%s' завершена, unit = '%s'", task.getUuid(), DriverWorkerUnit.this.getName()));
                            } catch (DBException var13) {
                                logger.error(var13.getMessage(), var13);
                            }
                        }
                    }

                }
            }
        }

    }

    private boolean isNeedBlock(int error) {
        switch(error) {
            case 2:
            case 3:
            case 4:
                return true;
            case 15:
                return true;
            case 115:
            case 116:
            case 117:
            case 118:
            case 119:
            case 120:
            case 121:
            case 122:
            case 124:
            case 133:
            case 134:
            case 135:
            case 136:
            case 137:
            case 138:
            case 141:
            case 142:
            case 159:
                return true;
            default:
                return false;
        }
    }
}
