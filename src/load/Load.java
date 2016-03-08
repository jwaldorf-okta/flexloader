/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package load;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author jwaldorf
 */
public class Load {

    public static String get(String resource, String token) {
        System.out.println(new Date() + " get " + resource);
        boolean tryAgain = true;
        String result = "";
        URL url;
        HttpURLConnection conn;
        BufferedReader rd;
        String line;
        try {
            url = new URL(resource);
            while (tryAgain) {
                result = "";
                try {
                    conn = (HttpURLConnection) url.openConnection();
                    conn.setConnectTimeout(10000);
                    conn.setReadTimeout(10000);
                    conn.setRequestProperty("Accept", "application/json");
                    conn.setRequestProperty("Content-Type", "application/json");
                    conn.setRequestProperty("Authorization", "SSWS " + token);
                    conn.setRequestMethod("GET");

                    String ret = conn.getResponseMessage();
                    int retCode = conn.getResponseCode();
                    if (retCode == 200) {
                        tryAgain = false;
                        rd = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
                        while ((line = rd.readLine()) != null) {
                            result += line;
                        }
                        rd.close();
                    } else if (retCode == 404) {
                        tryAgain = false;
                        rd = new BufferedReader(new InputStreamReader(conn.getErrorStream(), "UTF-8"));
                        while ((line = rd.readLine()) != null) {
                            result += line;
                        }
                        rd.close();
                    } else {
                        tryAgain = true;
                        rd = new BufferedReader(new InputStreamReader(conn.getErrorStream(), "UTF-8"));
                        while ((line = rd.readLine()) != null) {
                            result += line;
                        }
                        rd.close();
                        System.out.println(new Date() + " GET " + resource + " RETURNED " + retCode + ":" + ret);
                        System.out.println(new Date() + " ERRORSTREAM = " + result);
                    }
                } catch (SocketTimeoutException e) {
                    tryAgain = true;
                    System.out.println(new Date() + " GET " + resource + " " + e.getLocalizedMessage());
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            result = e.getLocalizedMessage();
            System.out.println(new Date() + " GET " + resource + " " + e.getLocalizedMessage());
            e.printStackTrace();
        }
        System.out.println(new Date() + " RESULT " + result);
        return result;
    }

    public static String delete(String resource, String token) {
        System.out.println(new Date() + " delete " + resource);
        boolean tryAgain = true;
        String result = "";
        URL url;
        HttpURLConnection conn;
        BufferedReader rd;
        String line;
        try {
            url = new URL(resource);
            while (tryAgain) {
                result = "";
                try {
                    conn = (HttpURLConnection) url.openConnection();
                    conn.setConnectTimeout(10000);
                    conn.setReadTimeout(10000);
                    conn.setRequestProperty("Accept", "application/json");
                    conn.setRequestProperty("Content-Type", "application/json");
                    conn.setRequestProperty("Authorization", "SSWS " + token);
                    conn.setRequestMethod("DELETE");
                    String ret = conn.getResponseMessage();
                    int retCode = conn.getResponseCode();
                    if (retCode == 200 || retCode == 204) {
                        tryAgain = false;
                    } else if (retCode == 404) {
                        tryAgain = false;
                        rd = new BufferedReader(new InputStreamReader(conn.getErrorStream(), "UTF-8"));
                        while ((line = rd.readLine()) != null) {
                            result += line;
                        }
                        rd.close();
                    } else {
                        tryAgain = true;
                        rd = new BufferedReader(new InputStreamReader(conn.getErrorStream(), "UTF-8"));
                        while ((line = rd.readLine()) != null) {
                            result += line;
                        }
                        rd.close();
                        System.out.println(new Date() + " GET " + resource + " RETURNED " + retCode + ":" + ret);
                        System.out.println(new Date() + " ERRORSTREAM = " + result);
                    }
                } catch (SocketTimeoutException e) {
                    tryAgain = true;
                    System.out.println(new Date() + " GET " + resource + " " + e.getLocalizedMessage());
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            result = e.getLocalizedMessage();
            System.out.println(new Date() + " GET " + resource + " " + e.getLocalizedMessage());
            e.printStackTrace();
        }
        System.out.println(new Date() + " RESULT " + result);
        return result;
    }

    public static String post(String resource, String token, String data) {
        System.out.println(new Date() + " post " + resource + " " + data);
        String res = "";
        try {
            URL url = new URL(resource);
            HttpURLConnection conn;
            conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(100000);
            conn.setReadTimeout(100000);
            conn.setDoOutput(true);
            conn.setRequestProperty("Authorization", "SSWS " + token);
            conn.setRequestProperty("Content-Type", "application/json");

            conn.setRequestMethod("POST");
            DataOutputStream wr = new DataOutputStream(conn.getOutputStream());
            wr.write(data.getBytes("UTF-8"));
            wr.flush();
            wr.close();
            String line;
            if (conn.getResponseCode() == 200 || conn.getResponseCode() == 201) {
                BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                while ((line = rd.readLine()) != null) {
                    res += line;
                }
                rd.close();
            } else if (conn.getResponseCode() == 204) {
            } else {
                BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
                while ((line = rd.readLine()) != null) {
                    res += line;
                }
                rd.close();
                System.out.println(new Date() + " POST " + url.toString() + " OF " + data + " RETURNS " + conn.getResponseCode() + ":" + conn.getResponseMessage());
                System.out.println(new Date() + " ERRORSTREAM = " + res);
            }
        } catch (Exception e) {
            System.out.println(new Date() + " POST " + resource + " OF " + data + " LOCALIZEDMESSAGE " + e.getLocalizedMessage());
            res = e.getLocalizedMessage();
            e.printStackTrace();
        }
        System.out.println(new Date() + " RESULT " + res);
        return res;
    }

    public static List<String> getAll(String resource, String token) throws IOException {
        List<String> ret = new LinkedList<String>();
        URL url;
        HttpURLConnection conn;
        url = new URL(resource);
        while (true) {
            try {
                conn = (HttpURLConnection) url.openConnection();
                conn.setReadTimeout(1000 * 60 * 5);
                conn.setRequestProperty("Authorization", "SSWS " + token);
                conn.setRequestMethod("GET");
                int retVal = conn.getResponseCode();
                if (retVal == 200) {
                    JSONArray sArray = new JSONArray(new JSONTokener(conn.getInputStream()));
                    for (int i = 0; i < sArray.length(); i++) {
                        ret.add(sArray.getJSONObject(i).getString("id"));
                    }
                    if (conn.getInputStream() != null) {
                        conn.getInputStream().close();
                    }
                    Map<String, List<String>> map = conn.getHeaderFields();
                    String link;
                    if (map.containsKey("Link")) {
                        List<String> l = map.get("Link");
                        boolean found = false;
                        for (int i = 0; i < l.size(); i++) {
                            String val = map.get("Link").get(i);
                            String[] pair = val.split(";");
                            if (pair[1].contains("next")) {
                                link = pair[0].substring(1, pair[0].length() - 1);
                                url = new URL(link);
                                found = true;
                                break;
                            }
                        }
                        if (found == false) {
                            break;
                        }
                    } else {
                        break;
                    }
                } else {
                    BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
                    String line;
                    StringBuilder result = new StringBuilder("");
                    while ((line = rd.readLine()) != null) {
                        result.append(line);
                    }
                    System.out.println(new Date() + "Result = " + conn.getResponseCode() + ":" + conn.getResponseMessage());
                    System.out.println("" + new Date() + result);
                    rd.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return ret;
    }

    public static String getApp(String resource, String label, String token) {
        String ret = "";
        URL url;
        HttpURLConnection conn;
        try {
            url = new URL(resource);
            while (true) {
                conn = (HttpURLConnection) url.openConnection();
                conn.setReadTimeout(1000 * 60 * 5);
                conn.setRequestProperty("Authorization", "SSWS " + token);
                conn.setRequestMethod("GET");
                int retVal = conn.getResponseCode();
                if (retVal == 200) {
                    JSONArray sArray = new JSONArray(new JSONTokener(conn.getInputStream()));
                    for (int i = 0; i < sArray.length(); i++) {
                        if (sArray.getJSONObject(i).getString("label").equals(label)) {
                            return sArray.getJSONObject(i).getString("id");
                        }
                    }
                    if (conn.getInputStream() != null) {
                        conn.getInputStream().close();
                    }
                    Map<String, List<String>> map = conn.getHeaderFields();
                    String link;
                    if (map.containsKey("Link")) {
                        List<String> l = map.get("Link");
                        boolean found = false;
                        for (int i = 0; i < l.size(); i++) {
                            String val = map.get("Link").get(i);
                            String[] pair = val.split(";");
                            if (pair[1].contains("next")) {
                                link = pair[0].substring(1, pair[0].length() - 1);
                                url = new URL(link);
                                found = true;
                                break;
                            }
                        }
                        if (found == false) {
                            break;
                        }
                    } else {
                        break;
                    }
                } else {
                    BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
                    String line;
                    StringBuilder result = new StringBuilder("");
                    while ((line = rd.readLine()) != null) {
                        result.append(line);
                    }
                    System.out.println(new Date() + "Result = " + conn.getResponseCode() + ":" + conn.getResponseMessage());
                    System.out.println("" + new Date() + result);
                    rd.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        System.out.println(new Date() + " Starting: " + args[0] + "," + args[1] + "," + args[2]
                + "," + args[3] + "," + args[4]);
        String urlPrefix = args[0];
        String token = args[1];
        String templateApp = args[2];
        String inputFileString = args[3];
        String domain = args[4];
        String emailAlternative = "";
        if (args.length > 5) {
            emailAlternative = args[5];
            System.out.println(new Date() + " Do email alternative to " + emailAlternative);
        }
        while (true) {
            try {
                BufferedReader br = new BufferedReader(new FileReader(inputFileString + ".csv"));
                String line = br.readLine();
                while (line != null) {
                    int v = 0;
                    String[] values = new String[7];
                    values[0] = "";
                    values[1] = "";
                    values[2] = "";
                    values[3] = "";
                    values[4] = "";
                    values[5] = "";
                    values[6] = "";
                    for (int i = 0; i < line.length(); i++) {
                        char c = line.charAt(i);
                        if (c == ',') {
                            v++;
                        } else if (c == '/') {
                            i++;
                            values[v] += line.charAt(i);
                        } else {
                            values[v] += c;
                        }
                    }
                    System.out.println(new Date() + " Processing: "
                            + values[0] + ","
                            + values[1] + ","
                            + values[2] + ","
                            + values[3] + ","
                            + values[4] + ","
                            + values[5] + ","
                            + values[6]);
                    String email = values[1];
                    boolean adUser = false;
                    if (email.contains("flextronics")) {
                        adUser = true;
                        String name = email.substring(0, email.indexOf("@"));
                        email = name + domain;
                    }
                    System.out.println(new Date() + " email = " + email);
                    String appName = values[4];
                    String action = values[5];
                    String newemail = values[6];
                    if (action.equalsIgnoreCase("create")) {
                        String val = get(urlPrefix + "/users/" + email, token);
                        JSONObject obj = new JSONObject(val);
                        String id = "";
                        if (obj.has("errorCode")) {
                            if (adUser) {
                                System.out.println(new Date() + " AD User does not exist.");
                                PrintWriter pw = new PrintWriter(new FileOutputStream(new File(inputFileString + "-failed.csv"), true));
                                pw.println(line);
                                pw.close();
                                continue;
                            }
                            if (obj.getString("errorCode").equals("E0000007")) {
                                String ret = post(urlPrefix + "/users", token,
                                        "{\"profile\":{"
                                        + "\"login\":\"" + email + "\","
                                        + "\"firstName\":\"" + values[2] + "\","
                                        + "\"lastName\":\"" + values[3] + "\","
                                        + "\"email\":\""
                                        + ((emailAlternative.length() == 0) ? email : emailAlternative)
                                        + "\","
                                        + "\"userType\":\"External\""
                                        + "}}");
                                JSONObject o = new JSONObject(ret);
                                if (o.has("errorCode")) {
                                    System.out.println(new Date() + " Unexpected Error.");
                                    PrintWriter pw = new PrintWriter(new FileOutputStream(new File(inputFileString + "-failed.csv"), true));
                                    pw.println(line);
                                    pw.close();
                                    continue;
                                }
                                id = o.getString("id");
                            } else {
                                System.out.println(new Date() + " Unexpected Error.");
                                PrintWriter pw = new PrintWriter(new FileOutputStream(new File(inputFileString + "-failed.csv"), true));
                                pw.println(line);
                                pw.close();
                                continue;
                            }
                        } else {
                            id = obj.getString("id");
                            if (obj.getString("status").equals("DEPROVISIONED")
                                    || obj.getString("status").equals("STAGED")) {
                                String href = obj.getJSONObject("_links").getJSONObject("activate").getString("href");
                                post(href, token, "{}");
                            }
                        }
                        String appID = "";
                        {
                            appID = getApp(urlPrefix + "/apps", appName, token);
                            if (appID.length() == 0) {
                                String tmp = get(urlPrefix + "/apps/" + templateApp, token);
                                JSONObject tempObj = new JSONObject(tmp);
                                tempObj.put("label", appName);
                                tmp = post(urlPrefix + "/apps", token, tempObj.toString());
                                appID = getApp(urlPrefix + "/apps", appName, token);
                            }
                        }
                        val = get(urlPrefix + "/apps/" + appID + "/users/" + id, token);
                        obj = new JSONObject(val);
                        if (obj.has("errorCode")) {
                            if (obj.getString("errorCode").equals("E0000007")) {
                                String ret = post(urlPrefix + "/apps/" + appID + "/users", token,
                                        "{\"id\":\"" + id + "\",\"scope\": \"USER\","
                                        + "\"credentials\":{\"userName\":\"" + values[0] + "\"}}}");
                            } else {
                                System.out.println(new Date() + " Unexpected Error.");
                                PrintWriter pw = new PrintWriter(new FileOutputStream(new File(inputFileString + "-failed.csv"), true));
                                pw.println(line);
                                pw.close();
                                continue;
                            }
                        }
                    } else if (action.equalsIgnoreCase("update")) {
                        String val = get(urlPrefix + "/users/" + email, token);
                        JSONObject obj = new JSONObject(val);
                        String id = obj.getString("id");
                        val = post(urlPrefix + "/users/" + id, token, "{\"profile\":{\"email\":\""
                                + newemail + "\", \"login\":\"" + newemail + "\"}}");
                        if ((new JSONObject(val)).has("errorCode")) {
                            System.out.println(new Date() + " Unexpected Error.");
                            PrintWriter pw = new PrintWriter(new FileOutputStream(new File(inputFileString + "-failed.csv"), true));
                            pw.println(line);
                            pw.close();
                            continue;
                        }
                    } else if (action.equalsIgnoreCase("remove")) {
                        String val = get(urlPrefix + "/users/" + email, token);
                        JSONObject obj = new JSONObject(val);
                        String id = "";
                        id = obj.getString("id");
                        String appID = getApp(urlPrefix + "/apps", appName, token);
                        if (appID.length() > 0) {
                            delete(urlPrefix + "/apps/" + appID + "/users/" + id, token);
                            if (!adUser) {
                                String appLinks = get(urlPrefix + "/users/" + id + "/appLinks", token);
                                JSONArray a = new JSONArray(appLinks);
                                if (a.length() == 0) {
                                    String href = obj.getJSONObject("_links").getJSONObject("deactivate").getString("href");
                                    post(href, token, "{}");
                                }
                            }
                        } else {
                            System.out.println(new Date() + " " + appName + " does not exist!");
                        }
                    }
                    line = br.readLine();
                }
                br.close();
                File oldName = new File(inputFileString + ".csv");
                File newName = new File(inputFileString + "-" + System.currentTimeMillis() + ".csv");
                if (oldName.renameTo(newName)) {
                    System.out.println(new Date() + " renamed");
                } else {
                    System.out.println(new Date() + " could not rename file.");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                Thread.sleep(5 * 60 * 1000);
            } catch (Exception ee) {
                ee.printStackTrace();
            }
        }
    }
}
