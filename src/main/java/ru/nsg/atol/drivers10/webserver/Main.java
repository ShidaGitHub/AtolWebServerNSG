package ru.nsg.atol.drivers10.webserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.jmx.gui.Client;
import org.apache.logging.log4j.jmx.gui.ClientGui;
import org.eclipse.jetty.jsp.JettyJspServlet;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.eclipse.jetty.webapp.Configuration;
import org.json.simple.JSONObject;
import ru.atol.drivers10.webserver.Version;
import ru.atol.drivers10.webserver.servlets.AboutServlet;
import ru.atol.drivers10.webserver.servlets.SettingsServlet;
import ru.atol.drivers10.webserver.servlets.TasksStatisticsServlet;
import ru.atol.drivers10.webserver.settings.Settings;
import ru.nsg.atol.drivers10.webserver.db.DBInstanceUnit;
import ru.nsg.atol.drivers10.webserver.servlets.JsonTaskUnitListServlet;
import ru.nsg.atol.drivers10.webserver.servlets.JsonTaskUnitServlet;
import ru.nsg.atol.drivers10.webserver.workers.DriverWorkerUnit;
import ru.nsg.atol.drivers10.webserver.workers.SenderWorkerUnit;
import sun.management.jmxremote.ConnectorBootstrap;

import javax.imageio.ImageIO;
import javax.management.MBeanServer;
import javax.management.remote.*;
import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.EnumSet;
import java.util.HashMap;

public class Main extends ru.atol.drivers10.webserver.Main {
    private static Logger logger = LogManager.getLogger(Main.class);
    private static Server server;

    private static HashMap<String, DriverWorkerUnit> driverMap = new HashMap();
    private static SenderWorkerUnit senderWorker;


    public Main() {
        super();
    }

    public static void main(String[] args) throws Exception {
        Jetty2Log4j2Bridge jetty2Log4j2Bridge = new Jetty2Log4j2Bridge(Main.class.getName());
        org.eclipse.jetty.util.log.Log.setLog(jetty2Log4j2Bridge);

        String jarPath = Paths.get("").toAbsolutePath().toString();
        //db
        System.setProperty("db.directory", jarPath + File.separator + "db" + File.separator);

        //settings
        File settings = new File(jarPath + File.separator +"conf" + File.separator + "settings.json");
        if (!settings.exists()) {
            try {
                settings.getParentFile().mkdirs();
                if (settings.createNewFile()){
                    Files.copy(Main.class.getResourceAsStream("/settings.json"), settings.toPath(), StandardCopyOption.REPLACE_EXISTING);
                }
            }catch (IOException ioEx){
                logger.error("Can't create " + settings.getAbsolutePath(), ioEx);
                System.exit(0);
            }
        }
        System.setProperty("settings.file", settings.toURI().toString());
        createTray();

        if (args.length == 0) {
            start(args);
            ru.atol.drivers10.webserver.Main.stop(args);
        } else if ("start".equals(args[0])) {
            start(args);
        } else if ("stop".equals(args[0])) {
            ru.atol.drivers10.webserver.Main.stop(args);
        }

    }


    public static void start(String[] args) throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                stop((String[])null);
            } catch (Exception var1) {
                logger.error(var1.getMessage(), var1);
            }

        }));

        logger.info(String.format("Запуск сервера ККТ v. %s...", (new Version()).getVersion()));
        logger.info("Инициализация БД...");
        DBInstanceUnit.db.init();

        logger.info("OK");
        logger.info("Запуск сервера");
        server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(Integer.parseInt(((JSONObject)Settings.load().get("web")).get("port").toString()));
        server.addConnector(connector);

        Configuration.ClassList classList = Configuration.ClassList.setServerDefault(server);
        classList.addBefore("org.eclipse.jetty.webapp.JettyWebXmlConfiguration", new String[]{"org.eclipse.jetty.annotations.AnnotationConfiguration"});
        URI baseUri = getWebRootResourceUri();
        FilterHolder filterHolder = new FilterHolder(CrossOriginFilter.class);
        filterHolder.setInitParameter("allowedOrigins", "*");
        filterHolder.setInitParameter("allowedMethods", "GET,POST,PUT,DELETE,HEAD,OPTIONS");
        ServletContextHandler servletContextHandler = new ServletContextHandler(1);
        servletContextHandler.setContextPath("/");
        servletContextHandler.setResourceBase(baseUri.toASCIIString());
        enableEmbeddedJspSupport(servletContextHandler);
        servletContextHandler.addServlet(SettingsServlet.class, "/settings");
        servletContextHandler.addServlet(AboutServlet.class, "/about");
        servletContextHandler.addServlet(JsonTaskUnitListServlet.class, "/requests");
        servletContextHandler.addServlet(JsonTaskUnitServlet.class, "/requests/*");
        servletContextHandler.addServlet(TasksStatisticsServlet.class, "/stat/requests");
        servletContextHandler.addFilter(filterHolder, "/*", (EnumSet)null);
        ServletHolder holderDefault = new ServletHolder("default", DefaultServlet.class);
        holderDefault.setInitParameter("resourceBase", baseUri.toASCIIString());
        holderDefault.setInitParameter("dirAllowed", "false");
        servletContextHandler.addServlet(holderDefault, "/");
        HandlerList handlers = new HandlerList();
        handlers.setHandlers(new Handler[]{servletContextHandler});
        server.setHandler(handlers);
        server.start();
        logger.info("OK");

        JSONObject settings = Settings.load();
        ((HashMap) settings.get("devices")).keySet().stream().filter(key -> !key.equals("main")).forEach(key -> {
            if ((Boolean)((JSONObject)settings.get("common")).get("is_active")){
                DriverWorkerUnit driverWorker = new DriverWorkerUnit(key.toString());
                driverMap.put(key.toString(), driverWorker);
                driverWorker.start();
                logger.info("driverWorker for {} {}", key.toString(), " started");
            }
        });
        if(settings.containsKey("resultsSend")){
            senderWorker = new SenderWorkerUnit();
            senderWorker.setName("resSender");
            senderWorker.start();
            logger.info("resSender started");
        }

        server.join();
    }

    public static void stop(String[] args) throws Exception {
        logger.info("Завершение работы...");
        if (server != null && server.isRunning()) {
            logger.info("Завершение сервера...");
            Handler[] var1 = server.getHandlers();
            int var2 = var1.length;

            for(int var3 = 0; var3 < var2; ++var3) {
                Handler handler = var1[var3];
                handler.stop();
            }

            server.stop();
            server.getThreadPool().join();
            logger.info("OK");
        }

        driverMap.forEach((s, driverWorkerUnit) -> {
            if (driverWorkerUnit != null && driverWorkerUnit.isAlive()) {
                logger.info("Завершение потока работы с ККТ " + driverWorkerUnit.getName());
                driverWorkerUnit.interrupt();
                try {
                    driverWorkerUnit.join();
                } catch (InterruptedException e) {
                    logger.error("error " + driverWorkerUnit.getName(), e);
                }
                logger.info("OK");
            }
        });
        driverMap.clear();
        if (senderWorker != null) {
            logger.info("Завершение потока отправки ");
            senderWorker.interrupt();
            try {
                senderWorker.join(10000);
            } catch (InterruptedException e) {
                logger.error("error " + senderWorker.getName(), e);
            }
            logger.info("OK");
        }
    }

    private static void enableEmbeddedJspSupport(ServletContextHandler servletContextHandler) throws IOException {
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        File scratchDir = new File(tempDir.toString(), "atol-web-server-tmp");
        if (!scratchDir.exists() && !scratchDir.mkdirs()) {
            throw new IOException("Unable to create scratch directory: " + scratchDir);
        } else {
            servletContextHandler.setAttribute("javax.servlet.context.tempdir", scratchDir);
            ClassLoader jspClassLoader = new URLClassLoader(new URL[0], ru.atol.drivers10.webserver.Main.class.getClassLoader());
            servletContextHandler.setClassLoader(jspClassLoader);
            servletContextHandler.addBean(new ru.atol.drivers10.webserver.Main.JspStarter(servletContextHandler));
            ServletHolder holderJsp = new ServletHolder("jsp", JettyJspServlet.class);
            holderJsp.setInitOrder(0);
            holderJsp.setInitParameter("logVerbosityLevel", "DEBUG");
            holderJsp.setInitParameter("fork", "false");
            holderJsp.setInitParameter("xpoweredBy", "false");
            holderJsp.setInitParameter("compilerTargetVM", "8");
            holderJsp.setInitParameter("compilerSourceVM", "8");
            holderJsp.setInitParameter("keepgenerated", "true");
            holderJsp.setInitParameter("classpath", System.getProperty("java.class.path"));
            servletContextHandler.addServlet(holderJsp, "*.jsp");
        }
    }

    private static URI getWebRootResourceUri() throws FileNotFoundException, URISyntaxException {
        URL indexUri = ru.atol.drivers10.webserver.Main.class.getResource("/webapp/");
        if (indexUri == null) {
            throw new FileNotFoundException("Unable to find resource /webapp/");
        } else {
            return indexUri.toURI();
        }
    }

    private static void createTray(){
        if (GraphicsEnvironment.isHeadless()) {
            logger.info("SystemTray is not supported");
            return;
        }
        if (!SystemTray.isSupported()) {
            logger.info("SystemTray is not supported");
            return;
        }
        final PopupMenu popup = new PopupMenu();

        MenuItem item00 = new MenuItem("Open settings");
        item00.addActionListener(e -> {
            try {
                URI uriSet = new URI(System.getProperty("settings.file"));
                File settingsFile = new File(uriSet);
                if (System.getProperty("os.name").toLowerCase().contains("windows")) {
                    String cmd = "rundll32 url.dll,FileProtocolHandler " + settingsFile.getCanonicalPath();
                    Runtime.getRuntime().exec(cmd);
                }
                else {
                    Desktop.getDesktop().edit(settingsFile);
                }
            } catch (URISyntaxException | IOException openEx) {
                logger.error("can't open file", openEx);
            }
        });
        popup.add(item00);

        MenuItem item0 = new MenuItem("Restart");
        item0.addActionListener(e -> {
            try {
                stop(null);
                start(null);
            }catch (Exception ex){
                logger.error(ex);
            }

            System.exit(0);
        });
        popup.add(item0);

        MenuItem item1 = new MenuItem("Exit");
        item1.addActionListener(e -> {
            try {
                stop(null);
            }catch (Exception ex){
                logger.error(ex);
            }

            System.exit(0);
        });
        popup.add(item1);

        final TrayIcon trayIcon;
        try {
            trayIcon = new TrayIcon(ImageIO.read(Main.class.getResourceAsStream("/tray.png")));
            final SystemTray tray = SystemTray.getSystemTray();
            tray.add(trayIcon);
            trayIcon.setPopupMenu(popup);
        } catch (IOException | AWTException e) {
            logger.warn("TrayIcon could not be added.");
        }
    }
}
