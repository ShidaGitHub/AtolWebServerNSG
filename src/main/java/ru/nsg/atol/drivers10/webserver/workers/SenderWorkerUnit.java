package ru.nsg.atol.drivers10.webserver.workers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import ru.atol.drivers10.webserver.db.DBException;
import ru.atol.drivers10.webserver.settings.Settings;
import ru.nsg.atol.drivers10.webserver.db.DBInstanceUnit;
import ru.nsg.atol.drivers10.webserver.entities.SubtaskStatusUnit;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class SenderWorkerUnit  extends Thread {
    private static Logger logger = LogManager.getLogger(SenderWorkerUnit.class);
    private HttpClient httpClient;

    public SenderWorkerUnit() {}

    @Override
    public void run() {
        short sleepTimeout = 100;
        httpClient = new HttpClient();
        httpClient.setConnectTimeout(3000);
        try {
            httpClient.start();
        }catch (Exception startEx){
            logger.error("can' create HttpClient", startEx);
        }

        while(!this.isInterrupted()) {
            try {
                Thread.sleep((long)sleepTimeout);
            } catch (InterruptedException var18) {
                return;
            }

            List<SubtaskStatusUnit> subtaskList;
            try {
                subtaskList = DBInstanceUnit.db.getNextSubTaskListToSend();
            } catch (DBException var19) {
                logger.error(var19.getMessage(), var19);
                continue;
            }
            if (subtaskList != null){
                logger.info("Найдено для отправки {}", subtaskList.size());

                try {
                    JSONObject settings = (JSONObject) Settings.load().get("resultsSend");
                    String uri = String.valueOf(settings.get("uri")).trim();
                    String login = String.valueOf(settings.get("login"));
                    String password = String.valueOf(settings.get("password"));

                    if (!httpClient.isRunning())
                        httpClient.start();


                    Request request = httpClient.newRequest(uri);
                    request.timeout(3, TimeUnit.SECONDS);
                    request.header(HttpHeader.ACCEPT, "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
                    request.header(HttpHeader.ACCEPT_ENCODING, "gzip, deflate");
                    request.header(HttpHeader.AUTHORIZATION, "Basic " + Base64.getEncoder().encodeToString((login + ":" + password).getBytes(Charset.forName("UTF-8"))));
                    request.header(HttpHeader.CONNECTION, "keep-alive");
                    request.header(HttpHeader.CACHE_CONTROL, "max-age=0, no-cache");
                    request.header(HttpHeader.PRAGMA, "no-cache");
                    request.method(HttpMethod.POST);

                    StringBuilder content = new StringBuilder("[");
                    content.append(subtaskList.stream().map(subUnit -> subUnit.getJsonString()).collect(Collectors.joining(",")));
                    content.append("]");
                    request.content(new StringContentProvider(content.toString(), Charset.forName("UTF-8")));

                    ContentResponse contentResponse = request.send();

                    subtaskList.forEach(unit -> {
                        unit.setSendCode(contentResponse.getStatus());
                        unit.setSendRes(contentResponse.getContentAsString());
                    });
                    DBInstanceUnit.db.updateSubTaskSendStatus(subtaskList);
                    if (contentResponse.getStatus() != 200)
                        throw new ExecutionException(new RuntimeException(contentResponse.getContentAsString()));

                    logger.info("Отправлено {}", subtaskList.size());
                    sleepTimeout = 100;
                } catch (ParseException | IOException ex) {
                    logger.error("Не удалось прочитать настройки", ex);
                    SenderWorkerUnit.this.interrupt();
                } catch (InterruptedException | ExecutionException ex0) {
                    logger.error("Не удалось отправить данные", ex0);
                    sleepTimeout = 1000;
                } catch (TimeoutException ex1) {
                    logger.error("TimeoutException", ex1);
                    sleepTimeout = 100;
                } catch (DBException ex1) {
                    logger.error("Не удалось считать данные", ex1);
                    SenderWorkerUnit.this.interrupt();
                } catch (Exception ex2) {
                    logger.error("Не удалось подключиться", ex2);
                    SenderWorkerUnit.this.interrupt();
                }
            }
        }

        if (!httpClient.isRunning()) {
            try {
                httpClient.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
