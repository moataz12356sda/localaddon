import asyncore
import binascii
import json
import threading
from datetime import datetime
import paho.mqtt.client as mqtt
import socket
from influxdb import InfluxDBClient
import base64
import json
import requests
import time
with open('/data/options.json', 'r') as config_file:    config = json.load(config_file)

# Get the username and password from the configuration
userName = config.get('username')
password = config.get('password')

print(f"Username: {userName}")
print(f"Password: {password}")
login_uri = 'https://iot.skarpt.net/java_bk/login'
add_readings_uri = 'https://iot.skarpt.net/java_bk/reports/addReadingsList'




full_packet_list = []
ServerActive = True
responsePacket = ''
response2 = ''


DATABASE_PORT = '8086'
USERNAME_DATABASE = str(open("config/USERNAME_DATABASE.txt", "r").read()).strip()
PASSWORD_DATABASE = str(open("config/PASSWORD_DATABASE.txt", "r").read()).strip()
INTERNAL_BACKUP_DATABASE_NAME = str(open("config/INTERNAL_BACKUP_DATABASE_NAME.txt", "r").read()).strip()
INTERNAL_DATABASE_NAME = str(open("config/INTERNAL_DATABASE_NAME.txt", "r").read()).strip()
DATABASE_IP = str(open("config/DATABASE_IP.txt", "r").read()).strip()
measurement = str(open("config/measurement.txt", "r").read()).strip()

def http_request(url, method, headers, timeout=30, body=None):
    try:
        result = requests.request(method=method, url=url, verify=False, headers=headers, json=body, timeout=timeout)
        return result
    except Exception as ex:
        print(ex.__cause__)

        return False
def login():
    global token
    basic_auth = userName + ':' + password
    encoded_u = base64.b64encode(basic_auth.encode()).decode()
    header = {"Authorization": "Basic %s" % encoded_u}
    result = http_request(url=login_uri,
                          method='get',
                          headers=header)

    if not result:
        print(result)
        print("error on login")
        print(result.content)
        return
    print(result.content)
    json_string = result.content.decode('utf8').replace("'", '"')
    json_object = json.loads(json_string)
    token = json_object['entity'][0]['token']
    print(token)
    return token

#
# def SendJsonToServer(json_object):
#     global token
#     try:
#         if token == "":
#             token = login()
#         result = http_request(url=add_readings_uri,
#                               method='post',
#                               body=json_object,
#                               headers={"TOKEN": token})
#         if not result:
#             print(result)
#             print("error on sending")
#             print(result.content)
#             return False
#         print(result.content)
#         if result.status_code != 200:
#             token = login()
#             result = http_request(url=add_readings_uri,
#                                   method='post',
#                                   body=json_object,
#                                   headers={"TOKEN": token})
#             if not result:
#                 print(result)
#                 print("error on sending")
#                 print(result.content)
#                 return False
#             print(result.content)
#             if result.status_code != 200:
#                 return False
#     except Exception as exc:
#         print("ERROR : %s" % exc)
#         return False
#     return True
#
def SendJsonToServer(json_object):
    global token
    try:
        if token == "":
            token = login()

        while True:
            result = http_request(url=add_readings_uri,
                                  method='post',
                                  body=json_object,
                                  headers={"TOKEN": token})

            if result and result.status_code == 200:
                print("Packet successfully sent!")
                print(result.content)
                return True  # Packet successfully sent, exit loop

            else:
                print("Failed to send packet, retrying...")
                print("Error details:", result.content if result else "No response")
                token = login()  # Retry login to refresh token if needed
                time.sleep(5)  # Wait for 5 seconds before retrying

    except Exception as exc:
        print(f"ERROR : {str(exc)}")
        return False

def ConvertRTCtoTime(RTC):
    Year, Month, Day, Hours, Min, Sec = RTC[0:2], RTC[2:4], RTC[4:6], RTC[6:8], RTC[8:10], RTC[10:12]
    Year, Month, Day, Hours, Min, Sec = int(Year, 16), int(Month, 16), int(Day, 16), int(Hours, 16), int(Min, 16), int(
        Sec, 16)
    print("Date is ", Year, "/", Month, "/", Day)
    print("Time is ", Hours, "/", Min, "/", Sec)
    Date = str(Year) + "/" + str(Month) + "/" + str(Day)
    Time = str(Hours) + "/" + str(Min) + "/" + str(Sec)
    # return  Year, Month, Day, Hours, Min, Sec
    return Date, Time




def ConvertSensorsToReadings_server(packet):
    readings = []
    sensorfound = False
    NumberOfSensors = 0
    Sensorhexlist = []
    Packetindex = packet[-12:-8]
    print(Packetindex)
    #Update_ACK(str(int(Packetindex, 16)))
    Packetsensorlength = packet[76:80]
    if Packetsensorlength == "0000":
        return 0
    if int(Packetsensorlength, 16) != 0:
        sensorfound = True
        NumberOfSensors = packet[82:84]
        NumberOfSensors = int(NumberOfSensors, 16)
        print("Number Of Sensors", NumberOfSensors, "Sensor")
        result = 0
        for i in range(NumberOfSensors):
            i = i + result
            Sensorhexlist.append(packet[86 + i:108 + i])
            result += 21
    RTC = packet[40:52]
    date, time = ConvertRTCtoTime(RTC)
    GatewayBattary = packet[68:72]
    GatewayBattary = int(GatewayBattary, 16) / 100
    GatewayPower = packet[72:76]
    GatewayPower = int(GatewayPower, 16) / 100
    json_object = {"GatewayId": packet[24:40],
                   "GatewayBattary": GatewayBattary,
                   "GatewayPower": GatewayPower,
                   "Date": date,
                   "Time": time,
                   "packet": packet}
    for packet in Sensorhexlist:
        sensor_data = {
            "Sensorid": packet[0:8],
            "SensorBattary": int(packet[10:14], 16) / 1000,
            "temperature": TempFun(packet[14:18]),
            "humidity": HumFun(packet[18:20])
        }
        readings.append(sensor_data)
        print("sensor %s : " % packet[0:8], json_object)

    json_object["data"] = readings
    if not SendJsonToServer(json_object):
        return False
    return True

def ConvertKSA(packet):
    hour = packet[46:48]
    print(int(hour, 16))
    h = int(hour, 16)
    newtime = str(hex(int(hour, 16) + 1)).replace("0x", "")
    if len(newtime) == 1:
        newtime = "0" + newtime
    newpacket = packet[:46] + newtime + packet[48:]
    return newpacket


def Checked_SavedHolding_Database():
    client = InfluxDBClient(DATABASE_IP, DATABASE_PORT, USERNAME_DATABASE, PASSWORD_DATABASE,
                            INTERNAL_BACKUP_DATABASE_NAME)
    result = client.query('SELECT *  FROM ' + str(INTERNAL_BACKUP_DATABASE_NAME) + '."autogen".' + str(measurement))
    length = len(list(result.get_points()))
    if length != 0:
        return True
    else:
        return False


def Send_Saved_Database():
    client = InfluxDBClient(DATABASE_IP, DATABASE_PORT, USERNAME_DATABASE, PASSWORD_DATABASE,
                            INTERNAL_BACKUP_DATABASE_NAME)
    result = client.query('SELECT *  FROM ' + str(INTERNAL_BACKUP_DATABASE_NAME) + '."autogen".' + str(measurement))
    data = list(result.get_points())
    for point in data:
        success = False  # Initialize the success flag

        while not success:
            # Attempt to convert sensor data and send it to the server
            success = ConvertSensorsToReadings_server(str(point["Packet"]))

            if success:
                # Only delete the packet if the conversion and sending were successful
                client.delete_series(database=INTERNAL_BACKUP_DATABASE_NAME, measurement=measurement,
                                     tags={"id": point["id"]})
                print(f"Packet with id {point['id']} sent and deleted successfully.")
            else:
                # Log failure and retry after a delay
                print(f"Failed to send packet with id {point['id']}. Retrying in 5 seconds...")
                time.sleep(5)  # Wait for 5 seconds before retrying


def Save_IndexNum(index):
    textfile = open("IndexNum.txt", "w")
    textfile.write(str(index))
    textfile.close()


def Load_IndexNum():
    text_file = open("IndexNum.txt", "r")
    lines = text_file.readlines()
    Nlist = [i.replace("\n", "").strip() for i in lines]
    return int(Nlist[0])


def Set_IndexNumber():
    Save_IndexNum(0)


def SendPacketHoldingDataBase(packet):
    from influxdb import InfluxDBClient
    client = InfluxDBClient(DATABASE_IP, DATABASE_PORT, USERNAME_DATABASE, PASSWORD_DATABASE,
                            INTERNAL_BACKUP_DATABASE_NAME)
    try:
        index = Load_IndexNum()
    except:
        Set_IndexNumber()
        index = Load_IndexNum()

    DataPoint = [
        {
            "measurement": measurement,
            "tags": {
                "id": index
            },
            "fields": {
                "Packet": packet
            }
        }
    ]
    index += 1
    Save_IndexNum(index)
    client.write_points(DataPoint)





def Update_ACK(Packetindex):
    global responsePacket, response2
    # str = '@CMD,*000000,@ACK,'+Packetindex+'#,#'
    str1 = '@ACK,' + Packetindex + '#'
    str1 = str1.encode('utf-8')
    responsePacket = str1.hex()
    response2 = "Server UTC time:" + str(datetime.now())[:19]
    response2 = response2.encode('utf-8')
    response2 = response2.hex()


def ConvertRTCtoTime(RTC):
    Year, Month, Day, Hours, Min, Sec = RTC[0:2], RTC[2:4], RTC[4:6], RTC[6:8], RTC[8:10], RTC[10:12]
    Year, Month, Day, Hours, Min, Sec = int(Year, 16), int(Month, 16), int(Day, 16), int(Hours, 16), int(Min, 16), int(
        Sec, 16)
    print("Date is ", Year, "/", Month, "/", Day)
    print("Time is ", Hours, "/", Min, "/", Sec)
    Date = str(Year) + "/" + str(Month) + "/" + str(Day)
    Time = str(Hours) + "/" + str(Min) + "/" + str(Sec)
    # return  Year, Month, Day, Hours, Min, Sec
    return Date, Time


def TempFun(temp):
    sign = ''
    hexadecimal = temp
    end_length = len(hexadecimal) * 4
    hex_as_int = int(hexadecimal, 16)
    hex_as_binary = bin(hex_as_int)
    padded_binary = hex_as_binary[2:].zfill(end_length)
    normalbit = padded_binary[0]
    postitive = padded_binary[1]
    value = padded_binary[2:]
    if str(normalbit) == '0':
        pass
    else:
        return "Sensor error"

    if str(postitive) == '0':
        sign = '+'
    else:
        sign = '-'

    if sign == '+':
        return str(int(value, 2) / 10)

    else:
        return "-" + str(int(value, 2) / 10)


def HumFun(hum):
    hexadecimal = hum
    end_length = len(hexadecimal) * 4
    hex_as_int = int(hexadecimal, 16)
    hex_as_binary = bin(hex_as_int)
    padded_binary = hex_as_binary[2:].zfill(end_length)
    normalbit = padded_binary[0]
    value = padded_binary[1:]
    if str(normalbit) == '0':
        pass
    else:
        return "Sensor error"
    return str(int(value, 2))


def logic(packet):
    try:
        # Attempt to login and get the token
        token = login()

        if token:
            # If login is successful, process the packet
            ConvertSensorsToReadings_server(packet)

            # Check if there's saved data in the holding database
            if Checked_SavedHolding_Database():
                # If so, send the saved data in a separate thread
                threading.Thread(target=Send_Saved_Database, args=[]).start()
        else:
            # If login fails, store the packet in the database
            SendPacketHoldingDataBase(packet)

    except Exception as ex:
        # Catch any exceptions that occur during the process
        print(f"An error occurred: {str(ex)}")
        # Optionally, handle specific cases or log the error
        SendPacketHoldingDataBase(packet)


def ConvertPacketIntoElemets(packet):
    threading.Thread(target=logic, args=[packet]).start()

    sensorfound = False
    NumberOfSensors = 0
    Sensorhexlist = []
    Packetindex = packet[-12:-8]
    print(Packetindex)
    Update_ACK(str(int(Packetindex, 16)))
    Packetsensorlength = packet[76:80]
    if Packetsensorlength == "0000":
        return 0
    if int(Packetsensorlength, 16) != 0:
        sensorfound = True
        NumberOfSensors = packet[82:84]
        NumberOfSensors = int(NumberOfSensors, 16)
        print("Number Of Sensors", NumberOfSensors, "Sensor")
        result = 0
        for i in range(NumberOfSensors):
            i = i + result
            Sensorhexlist.append(packet[86 + i:108 + i])
            result += 21
    GatwayId = packet[24:40]
    print(GatwayId)
    RTC = packet[40:52]
    date, time = ConvertRTCtoTime(RTC)
    GatewayBattary = packet[68:72]
    GatewayBattary = int(GatewayBattary, 16) / 100
    print("Battary of Gateway ", GatewayBattary, "Volt")
    GatewayPower = packet[72:76]
    GatewayPower = int(GatewayPower, 16) / 100
    print("Power of Gateway ", GatewayPower, "Volt")
    print(sensorfound, NumberOfSensors, Sensorhexlist)
    ConvertSensorsToReadings(GatwayId, date, time, GatewayBattary, GatewayPower, NumberOfSensors, Sensorhexlist)



def ConvertSensorsToReadings(GatwayId, date, time, GatewayBattary, GatewayPower, NumberOfSensors, Sensorhexlist):
    sensor_id_list = []
    sensor_temp_list = []
    sensor_hum_list = []
    sensor_battary_list = []
    jsonlist = []
    dectionarylist = []
    for packet in Sensorhexlist:
        sensor_id_list.append(packet[0:8])
        sensor_battary_list.append(int(packet[10:14], 16) / 1000)
        sensor_temp_list.append(TempFun(packet[14:18]))
        sensor_hum_list.append(HumFun(packet[18:20]))
    print(sensor_id_list)
    print(sensor_temp_list)
    print(sensor_hum_list)
    print(sensor_battary_list)
    for index in range(NumberOfSensors):
        jsonname = {"GatewayId": GatwayId, "GatewayBattary": GatewayBattary, "GatewayPower": GatewayPower, "Date": date,
                    "Time": time,
                    "Sensorid": sensor_id_list[index], "SensorBattary": sensor_battary_list[index],
                    "temperature": sensor_temp_list[index], "humidity": sensor_hum_list[index]
                    }
        dectionarylist.append(jsonname)
        print(json.dumps(jsonname))
        jsonlist.append(json.dumps(jsonname))
    # mqttsend(jsonlist,sensor_id_list)
    del jsonname, jsonlist, sensor_id_list, sensor_temp_list, sensor_hum_list, sensor_battary_list, GatwayId, date, time, GatewayBattary, GatewayPower, NumberOfSensors, Sensorhexlist
    SendToInternalDataBase(dectionarylist)


'''
def SendToInternalDataBaseToken (dectionarylist):
    bucket = "n"
    client = InfluxDBClient(url="http://localhost:8086",
                            token="n9cd2F9mYZcfhDE7892UzJv7xP38SSyQG9ybQRsYmGp6Bbv6OnbrGl5QGygzsZuzaCQTX-10w1EqY4axQNEzVg==",
                            org="skarpt")

    write_api = client.write_api(write_options=SYNCHRONOUS)
    query_api = client.query_api()
    for i in dectionarylist :
        p = Point("Tzone").tag("gateway",i["Sensorid"]).field("temperature", float(i["temperature"])).time(datetime(2021, 12, 20, 0, 0), WritePrecision.US)
        write_api.write(bucket=bucket, record=p)
        print("database saved read")
'''


def BuildJsonDataBase(Date, Time, Temp, Hum, Battery, GateWayID, SensorID):
    listofdate = Date.split("/")
    Year, Month, day = listofdate
    listoftime = Time.split("/")
    Hour, Mins, Sec = listoftime
    Year = "20" + Year
    ReadingTime = datetime(int(Year), int(Month), int(day), int(Hour), int(Mins), int(Sec)).isoformat() + "Z"
    JsonData = [
        {
            "measurement": measurement,
            "tags": {
                "SensorID": SensorID,
                "GatewayID": GateWayID
            },
            "time": ReadingTime,
            "fields": {
                "Temperature": float(Temp),
                "Humidity": float(Hum),
                "Battery": float(Battery)
            }
        }
    ]
    return JsonData


def SendToInternalDataBase(dectionarylist):
    from influxdb import InfluxDBClient
    client = InfluxDBClient(DATABASE_IP, DATABASE_PORT, USERNAME_DATABASE, PASSWORD_DATABASE, INTERNAL_DATABASE_NAME)
    for i in dectionarylist:
        DataPoint = BuildJsonDataBase(i["Date"], i["Time"], i["temperature"], i["humidity"], i["SensorBattary"],
                                      i["GatewayId"], i["Sensorid"])
        client.write_points(DataPoint)
    del dectionarylist


def check_packet(data):
    return True
    check_code = data[-8:- 4]
    # The range is from Protocol type to Packet index(include Protocol type and Packet index)
    hex_data = data[8:-8]
    our_model = PyCRC.CRC_16_MODBUS
    crc = CRC.CRC(hex_data, our_model)

    if check_code.lower() == crc.lower():
        return True
    else:
        return False


def preprocess_packet(data):
    global full_packet_list

    data = str(binascii.hexlify(data).decode())
    print(data)
    data = data.strip()
    if data.startswith("545a") and data.endswith("0d0a"):
        full_packet_list = []
        if check_packet(data):
            ConvertPacketIntoElemets(data)
        return [binascii.unhexlify(responsePacket.strip()), binascii.unhexlify(response2.strip())]
    elif data.endswith("0d0a") and not data.startswith("545a") and full_packet_list:
        collecting_packet = ''
        for packet_part in full_packet_list:
            collecting_packet += packet_part
        collecting_packet += data
        if check_packet(collecting_packet):
            ConvertPacketIntoElemets(collecting_packet)
        full_packet_list = []
        return [binascii.unhexlify(responsePacket.strip()), binascii.unhexlify(response2.strip())]
    else:
        full_packet_list.append(data)

    return 0


class EchoHandler(asyncore.dispatcher_with_send):

    def handle_read(self):
        data = self.recv(8192)
        if data:
            try:
                send_list = preprocess_packet(data)
                if send_list != 0:
                    for i in send_list:
                        self.send(i)
            except:
                pass


class EchoServer(asyncore.dispatcher):

    def __init__(self, host, port):
        asyncore.dispatcher.__init__(self)
        self.create_socket()
        self.set_reuse_addr()
        self.bind((host, port))
        self.listen(5)

    def handle_accepted(self, sock, addr):
        print('Incoming connection from %s' % repr(addr))
        handler = EchoHandler(sock)


server = EchoServer('', 2000)
asyncore.loop()
