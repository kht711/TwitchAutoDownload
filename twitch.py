import requests
import json
import datetime
import traceback
import uuid
import time
import threading
import subprocess
import os
import codecs
import configparser

import websocket

# Input Directory
dist = "twitchDownload"
commonLogFile = "log/common.log"

# Input auth token or token.ini file
CLIENT_ID = "auth_token"


headers = {
    "Content-Type": "application/json",
    "Client-ID": CLIENT_ID
}
MAX_LOG_LINE = 1000
TIME = 60
downloadIdList = set()
idDict = {}
loginDict = {}
downloadingList = {}


class WebSocketTwitch:
    def __init__(self, id):
        wssUrl = "wss://pubsub-edge.twitch.tv/"
        self.id = id
        self.login = idDict[self.id]
        self.logFile = os.path.join(os.getcwd(), dist, self.login, "{0}.log".format(self.login))
        self.ws = websocket.WebSocketApp(
            url=wssUrl,
            on_message=lambda ws, msg: self.on_message(ws, msg),
            on_error=lambda ws, msg: self.on_error(ws, msg),
            on_close=lambda ws, code, msg: self.on_close(ws, code, msg)
        )
        try:
            self.ws.on_open = lambda ws: self.on_open(ws)
        except KeyboardInterrupt:
            self.ws.close()

    def getPayload(self):
        topicsList = ["video-playback-by-id.{0}".format(self.id)]
        return {
            "type": "LISTEN",
            "nonce": str(uuid.uuid4()),
            "data": {
                "topics": topicsList,
                "auth_token": CLIENT_ID
            }
        }

    def getCurrentTime(self):
        dt_now = datetime.datetime.now()
        return dt_now.strftime("%Y-%m-%d %H:%M:%S")

    def writeLog(self, message):
        if not os.path.exists(os.path.dirname(self.logFile)):
            os.makedirs(os.path.dirname(self.logFile))

        if not os.path.exists(self.logFile):
            w = codecs.open(self.logFile, "w", "utf-8", "strict")
            w.close()

        lines = getLastLog(self.logFile, MAX_LOG_LINE - 2)
        w = codecs.open(self.logFile, "w", "utf-8", "strict")
        w.writelines(lines)

        w.write(message)
        w.write("\n")
        w.close()

    def on_message(self, ws, message):
        global downloadIdList
        global downloadingList

        try:
            self.writeLog("[{0}] {1}".format(self.getCurrentTime(), message))
            msgObj = json.loads(message)
            if "data" in msgObj:
                data = msgObj["data"]
                message = json.loads(data["message"])
                if message["type"] == "stream-down":
                    if self.login in downloadingList:
                        del downloadingList[self.login]
                    self.writeLog("[{0}] {1} download End!".format(self.getCurrentTime(), self.login))

                    if self.login not in downloadIdList:
                        self.writeLog("[{0}] {1} is Delete Download List!".format(self.getCurrentTime(), self.login))
                        if self.id in idDict:
                            del idDict[self.id]
                        if self.login in loginDict:
                            del loginDict[self.login]
                        ws.close()
                elif message["type"] in ["stream-up", "viewcount"]:
                    if "topic" in data:
                        topic = data["topic"]
                        twitchId = topic.split(".")[1]

                        if idDict[twitchId] not in downloadingList:
                            ws.close()
                            status = False
                            tryCountList = [0, 0]
                            if message["type"] == "stream-up":
                                time.sleep(4)
                            while True:
                                status, obj = getLoginToId(idDict[twitchId])
                                if obj["data"]["user"]["stream"]:
                                    status = download(idDict[twitchId], self.logFile)
                                    if status:
                                        break
                                    tryCountList[1] += 1
                                    if tryCountList[1] >= 3:
                                        # reconnect
                                        thread = threading.Thread(target=connect, args=(twitchId, ))
                                        thread.start()
                                        break
                                else:
                                    tryCountList[0] += 1
                                    self.writeLog("[{0}] {1} is Not live!".format(self.getCurrentTime(), self.login))
                                    if tryCountList[0] >= 10:
                                        # reconnect
                                        thread = threading.Thread(target=connect, args=(twitchId, ))
                                        thread.start()
                                        break
                                    time.sleep(0.5)
        except Exception:
            self.writeLog("[{0}] Error! {1}".format(self.getCurrentTime(), traceback.format_exc()))

    def on_error(self, ws, error):
        self.writeLog("[{0}] Websocket Error! {1}".format(self.getCurrentTime(), error))

    def on_close(self, ws, close_status_code, close_msg):
        if self.login not in downloadingList:
            self.writeLog("[{0}] Close! And {1} is no Reconnect".format(self.getCurrentTime(), self.login))
        else:
            self.writeLog("[{0}] Close! Reconnect...".format(self.getCurrentTime()))
            # reconnect
            thread = threading.Thread(target=connect, args=(self.id, ))
            thread.start()

    def on_open(self, ws):
        self.writeLog("[{0}] ID:{1} Connect Success!".format(self.getCurrentTime(), self.id))
        payload = self.getPayload()
        self.ws.send(json.dumps(payload))
        self.writeLog("[{0}] Send! {1}".format(self.getCurrentTime(), payload))

    def run_forever(self):
        self.ws.run_forever()


def getLoginToId(login):
    param = """
        query {{
            user(login: "{0}") {{
                id
                stream {{
                    id
                }}
            }}
        }}
    """
    userParam = param.format(login)

    try:
        res = requests.post('https://gql.twitch.tv/gql', json={"query": userParam}, headers=headers)
        res_json = json.loads(res.text)

        if res_json:
            if "data" in res_json:
                data = res_json["data"]
                if "user" in data:
                    user = data["user"]
                    if not user:
                        return (True, None)
                    else:
                        return (True, res_json)
        return (False, None)
    except requests.exceptions.ConnectionError:
        lines = getLastLog(commonLogFile, MAX_LOG_LINE - 2)
        w = codecs.open(commonLogFile, "w", "utf-8", "strict")
        w.writelines(lines)

        w.write("Connection Error! {0}\n".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        w.close()
        return (False, None)
    except Exception:
        lines = getLastLog(commonLogFile, MAX_LOG_LINE - 3)
        w = codecs.open(commonLogFile, "w", "utf-8", "strict")
        w.writelines(lines)

        w.write("Error! {0}\n".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        w.write(traceback.format_exc())
        w.write("Error End!\n")
        w.close()
        return (False, None)


def getLastLog(file, num):
    f = codecs.open(file, "r", "utf-8", "strict")
    lines = f.readlines()
    f.close()

    if len(lines) >= num:
        lines = lines[-num:]
    return lines


def connect(id):
    ws_client = WebSocketTwitch(id)
    ws_client.run_forever()


def download(user_info, logFile):
    global downloadingList
    global dist
    url = "https://www.twitch.tv/{0}".format(user_info)

    try:
        lines = getLastLog(logFile, MAX_LOG_LINE - 2)
        w = codecs.open(logFile, "w", "utf-8", "strict")
        w.writelines(lines)

        w.write("[{1}] downloading {0}...\n".format(user_info, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        w.close()

        path = os.path.join(os.getcwd(), dist, user_info)
        if not os.path.exists(path):
            os.makedirs(path)

        filename = ""
        try:
            filename = subprocess.check_output(["yt-dlp", url, "--print", "%(filename)s"], text=True, stderr=subprocess.STDOUT)
            filename = filename.strip().replace(" ", "_")
        except subprocess.CalledProcessError as e:
            if "live" in e.output:
                lines = getLastLog(logFile, MAX_LOG_LINE - 2)
                w = codecs.open(logFile, "w", "utf-8", "strict")
                w.writelines(lines)

                w.write("[{1}] {0} download Fail! Retry...\n".format(user_info, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                w.close()
                return False

        downloadCmdList = ["yt-dlp", url, "--abort-on-unavailable-fragment", "--cookies-from-browser", "chromium", "-P", path]
        if filename != "":
            downloadCmdList.append("-o")
            downloadCmdList.append(filename)
        pro = subprocess.Popen(downloadCmdList)
        downloadingList[user_info] = filename
        # reconnect
        thread = threading.Thread(target=connect, args=(loginDict[user_info], ))
        thread.start()
        # wait
        pro.wait()
        return True
    except Exception:
        lines = getLastLog(logFile, MAX_LOG_LINE - 2)
        w = codecs.open(logFile, "w", "utf-8", "strict")
        w.writelines(lines)

        w.write("[{1}] download Error! {0}...\n".format(user_info, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        w.write(traceback.format_exc())
        w.close()
        return True


def readTwitchList():
    global downloadIdList
    while True:
        absTwitchListTxtFile = os.path.join(os.getcwd(), "twitchList.txt")
        f = codecs.open(absTwitchListTxtFile, "r", "utf-8", "strict")
        lines = f.readlines()
        f.close()

        # new List
        newList = set()
        for line in lines:
            # comment ignore line
            if line.find("//") == 0:
                continue
            line = line.strip()
            newList.add(line)

        newConnectList = list(newList - downloadIdList)
        for login in newConnectList:
            while True:
                flag, obj = getLoginToId(login)
                if flag:
                    if obj is None:
                        lines = getLastLog(absCommonLogFile, MAX_LOG_LINE - 2)
                        w = codecs.open(absCommonLogFile, "w", "utf-8", "strict")
                        w.writelines(lines)

                        w.write("{0} is not Found!\n".format(login))
                        w.close()
                    else:
                        twitchId = obj["data"]["user"]["id"]
                        idDict[twitchId] = login
                        loginDict[login] = twitchId
                        thread = threading.Thread(target=connect, args=(twitchId, ))
                        thread.start()
                    break
                else:
                    time.sleep(TIME)
        downloadIdList = newList
        time.sleep(TIME)


os.chdir(os.path.dirname(os.path.abspath(__file__)))

absTokenFile = os.path.join(os.getcwd(), "token.ini")
if not os.path.exists(absTokenFile):
    config = configparser.RawConfigParser()
    config.add_section("AUTH_TOKEN")
    config.set("AUTH_TOKEN", "token", "")
    w = codecs.open(absTokenFile, "w", "utf-8", "strict")
    config.write(w)
    w.close()
configRead = configparser.ConfigParser()
configRead.read(absTokenFile, encoding="utf-8")
getToken = configRead.get("AUTH_TOKEN", "token")
if getToken != "":
    CLIENT_ID = getToken
    headers["Client-ID"] = CLIENT_ID

absCommonLogFile = os.path.join(os.getcwd(), commonLogFile)
if not os.path.exists(absCommonLogFile):
    if not os.path.exists(os.path.dirname(absCommonLogFile)):
        os.makedirs(os.path.dirname(absCommonLogFile))
    w = codecs.open(absCommonLogFile, "w", "utf-8", "strict")
    w.close()

absTwitchListTxtFile = os.path.join(os.getcwd(), "twitchList.txt")
if not os.path.exists(absTwitchListTxtFile):
    w = codecs.open(absTwitchListTxtFile, "w", "utf-8", "strict")
    w.write("// Input Twitch Login ID\n")
    w.close()

readTwitchListThread = threading.Thread(target=readTwitchList)
readTwitchListThread.start()
