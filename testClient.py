#! python3
import socket, random, sys, json, time, select
#import multiprocessing as mp
#import asyncio
import multiprocessing as mp

import numpy

import matplotlib.pyplot as plt

""" ====================================================================================================
GLOBALS
==================================================================================================== """
HOST = '127.0.0.1'
json_port = 1234
data_port = 1235
live_port = 1236

NUM_FRAMES = 0
RUNNING = True
GET_IMAGES = False
SHOW_IMAGES = False

live_frame_rate = 0
live_index = 0

def GET_ALL():
    return '{{"cmd":"GET","id":{},"values":"*"}}'.format(random.randrange(100))
def START(): 
    return '{{"cmd":"ACTION","id":{},"values":"START"}}'.format(random.randrange(100))
def STOP(): 
    return '{{"cmd":"ACTION","id":{},"values":"STOP"}}'.format(random.randrange(100))
def DATA(index=0):
    return '{{"cmd":"DATA","id":{},"index":{}}}'.format(random.randrange(100), index)
def SET(par, val):
    return '{{"cmd":"SET","id":{},"values":{{"{}":"{}"}}}}'.format(random.randrange(100),par,val)
def GET(par):
    return '{{"cmd":"GET","id":{},"values":"{}"}}'.format(random.randrange(100), par)
def DET_OFF():
    return '{{"cmd":"ACTION","id":{},"values":"DET_OFF"}}'.format(random.randrange(100))
def MON_ON():
    return '{{"cmd":"ACTION","id":{},"values":"MONITOR_ON"}}'.format(random.randrange(100))
def MON_OFF():
    return '{{"cmd":"ACTION","id":{},"values":"MONITOR_OFF"}}'.format(random.randrange(100))
def REPLACE_ADD(on=True):
    return f'{{"cmd":"SET","id":{random.randrange(100)},"values":{{"replaceAdd":"{on}"}}}}'
def GET_IMAGE():
    return '{{"cmd":"ACTION","id:{},"values":"GET_IMAGE"}}'.format(random.randrange(100))
def GET_ACQ_STATS():
    return '{{"cmd":"ACTION","id:{},"values":"GET_ACQ_STATS"}}'.format(random.randrange(100))






'''
Connect to the server over the JSON/command and data sockets
'''
def connect_to_server():
    print("Making cmd socket...")
    json_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print(f"Connecting to port {json_port}...")
    json_socket.connect((HOST, json_port))
    print("Making data socket...")
    data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print(f"Connecting to port {data_port}...")
    data_socket.connect((HOST, data_port))
    data_socket.settimeout(1)
    print("Making live socket...")
    live_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    live_socket.settimeout(1)
    print(f"Connecting to port {live_port}...")
    live_socket.connect((HOST, live_port))
    
    assert json_socket != None and data_socket != None and live_socket != None
    
    print(f"json_socket={json_socket.getsockname()}")
    print(f"data_socket={data_socket.getsockname()}")
    print(f"live_socket={live_socket.getsockname()}")
    
    return (json_socket, data_socket, live_socket)
    
def disconnect_from_server(json_socket, data_socket, live_socket):
    json_socket.close()
    data_socket.close()
    live_socket.close()
    
'''
Send a message to the server and if the reply has 'index' object,
store it in the global data_index 
'''
def send_msg(cmd, json_socket, lock, receive=True, debug=True):
    start_time = time.time()
    
    if debug:
        print("Sending message: {}".format(cmd))
    time_start = time.time()
    msg = cmd + '\r\n'
    msg = msg.encode()
    with lock:
        json_socket.send(msg)
        string = ""
        if receive:
            char_ret = False
            while True:
                data = json_socket.recv(1)
                if data == b'\r': 
                    char_ret = True
                elif data == b'\n' and char_ret:
                    break
                elif time.time() - start_time > 5:
                    break
                else:
                    char_ret = False
                    string += data.decode("utf-8")
                #print(data)
            
            try:
                #print("Parsing JSON...")
                string = string.replace('\\','/')
                json_out = json.loads(string)
                
                if 'values' in json_out:
                    for value in json_out['values']:
                        if debug:
                            print(value)
                elif debug:
                    print(f"Reply: '{string}'")
            except:
                print("Error parsing json: ",sys.exc_info()[0])
                print(f"{string}")
            if debug:
                print(">>> Reply:\n{}".format(string))
                print("Time taken: {}".format(time.time() - time_start))
            
            return string

def get_live_img(socket):
    try:
        header = socket.recv(20)
        # print(">>> Live header:")
        # print([hex(x) for x in header[0:36]])
        
        width = int.from_bytes(header[8:12], byteorder='big', signed=False)
        height = int.from_bytes(header[12:16], byteorder='big', signed=False)
        length = int.from_bytes(header[16:20], byteorder='big', signed=False)
        
        if length:
            # print(f"Got live img of length {length}")
            data = socket.recv(length)
            
            # Uncomment following lines to display the frame
            # data_int = []
            # for i in range(int(length/4)):
                # data_int.append(int.from_bytes(data[i*4:(i*4)+4], byteorder='big', signed=False))
            # plot = numpy.reshape(data_int, (height, width))
            # plt.imshow(plot, cmap='hot', interpolation='nearest')
            # plt.show()
            
            return True
            
    except (BlockingIOError, TimeoutError) as err:
        pass
    return False
        
        
def get_image(data_socket):
    try:
        header = data_socket.recv(40)
        #print(">>> Data header:")
        #print([hex(x) for x in header[0:40]])
        
        marker = int.from_bytes(header[0:4], byteorder='big', signed=False)
        
        if hex(marker) == '0xf0f0':
            #print("marker found")
            
            index = int.from_bytes(header[4:8], byteorder='big', signed=True)
            state = int.from_bytes(header[8:12], byteorder='big', signed=False)
            acq_scans = int.from_bytes(header[12:16], byteorder='big', signed=False)
            width = int.from_bytes(header[16:20], byteorder='big', signed=False)
            height = int.from_bytes(header[20:24], byteorder='big', signed=False)
            length = int.from_bytes(header[24:28], byteorder='big', signed=False)
            cur_width = int.from_bytes(header[28:32], byteorder='big', signed=False)
            cur_height = int.from_bytes(header[32:36], byteorder='big', signed=False)
            cur_length = int.from_bytes(header[36:40], byteorder='big', signed=False)
            print(f"    index:  {index}")
            print(f"    state:  {state}")
            print(f"    length: {length} num pixels: {height*width}")
            if length:
                try:
                    data_one = data_socket.recv(length)
                    while len(data_one) < length:
                        data_one += data_socket.recv(length - len(data_one))
                    data_int = []
                    sum_one = 0
                    for i in range(int(length/4)):
                        pixel = int.from_bytes(data_one[i*4:(i*4)+4], byteorder='little', signed=False)
                        data_int.append(pixel)
                        sum_one += pixel
                        #print(data_int)
                    print(f"sum of first data channel: {sum_one}")
                    if SHOW_IMAGES:
                        plot = numpy.reshape(data_int, (height, width))
                        plt.imshow(plot, cmap='hot', interpolation='none')
                        plt.show()

                    data_two = data_socket.recv(cur_length)
                    while len(data_two) < cur_length:
                        data_two += data_socket.recv(cur_length - len(data_two))
                    data_int = []
                    sum_two = 0
                    for i in range(int(cur_length/4)):
                        pixel = int.from_bytes(data_two[i*4:(i*4)+4], byteorder='little', signed=False)
                        data_int.append(pixel)
                        sum_two += pixel
                        #print(pixel)
                    print(f"sum of 2nd data channel: {sum_two}")
                    if SHOW_IMAGES:
                        plot = numpy.reshape(data_int, (cur_height, cur_width))
                        plt.imshow(plot, cmap='hot', interpolation='none')
                        plt.show()
                    return index
                except Exception as exc:
                    print(f"Caught exception {exc} reading the rest of the data")
                    raise
        else:
            print(f"ERROR: marker={marker}/{hex(marker)}")
            raise RuntimeError("Failed reading data header start word")
    except (BlockingIOError, TimeoutError) as err:
        #print("blocking err")
        return -1
    
""" ====================================================================================================
TASKS
==================================================================================================== """
    
    
def input_task(json_socket, data_socket, live_socket, data_queue, live_queue, lock):
    print(">>>>>>> Client started")
    acq_active = False
    print_message = True
    while True: 
        if print_message:
            sys.stdout.flush()
            print('Enter command:')
        else:
            print_message = True
        s = input()
        if s == '':
            print_message = False
            continue
        elif s == 'EXIT':
            data_queue.put("EXIT")
            live_queue.put("EXIT")
            return
        elif s == 'START':
            send_msg(START(), json_socket, lock)
            if not acq_active:
                acq_active = True
                data_queue.put("ACQ_ACTIVE")
                live_queue.put("ACQ_ACTIVE")
                
                time.sleep(0.1)
            
        elif s == 'LISTEN':
            if not acq_active:
                acq_active = True
                data_queue.put("ACQ_ACTIVE")
                live_queue.put("ACQ_ACTIVE")
                time.sleep(0.1)
        elif s == 'STOP':
            if acq_active:
                
                acq_active = False
                data_queue.put("ACQ_ACTIVE")
                live_queue.put("ACQ_ACTIVE")
                time.sleep(0.5)
            
            # send stop AFTER stopping acq thr or json_socket will be 
            # used by both threads at the same time
            send_msg(STOP(), json_socket, lock) 
            print("STOP msg sent")
            
        elif s == 'SET_DET_ZERO':
            send_msg(SET('startX',0), json_socket, lock)
            send_msg(SET('startY',0), json_socket, lock)
            send_msg(SET('endX',0), json_socket, lock)
            send_msg(SET('endY',0), json_socket, lock)
            send_msg(SET('numSlices',0), json_socket, lock)
        elif s == 'SET_DET_MAX':
            send_msg(SET('startX',0), json_socket, lock)
            send_msg(SET('endX',657), json_socket, lock)
            send_msg(SET('startY',0), json_socket, lock)
            send_msg(SET('endY',491), json_socket, lock)
        elif s == 'SET':
            print('<parameter>=<value>: ')
            val = input()
            val = val.split('=')
            par = val[0]
            val = val[1]
            send_msg(SET(par,val), json_socket, lock)
        elif s == 'GET_ALL':
            send_msg(GET_ALL(), json_socket, lock)
        elif s == 'GET':
            print('<parameter>:')
            par = input()
            send_msg(GET(par), json_socket, lock)
        elif s == 'help' or s == 'h':
            print("Test client for MBSEA project. Sends commands over socket to MBSEA server. " \
                  "Available commands: GET_ALL, SET, START, STOP, DATA; EXIT to stop")
        #elif s == 'RESET':
        #    disconnect_from_server(json_socket, data_socket, live_socket)
        #    connect_to_server(json_socket, data_socket, live_socket)
        elif s == 'DET_OFF':
            send_msg(DET_OFF(), json_socket, lock)
        elif s == 'MON_ON':
            send_msg(MON_ON(), json_socket, lock)
        elif s == 'MON_OFF':
            send_msg(MON_OFF(), json_socket, lock)
        elif s == 'REPLACE_ADD':
            send_msg(REPLACE_ADD(), json_socket, lock)
        elif s == 'GET_IMAGE':
            send_msg(GET_IMAGE(), json_socket, lock)
            get_image(data_socket)
        elif s == 'RUN_TEST':
            send_msg(START(), json_socket, lock)
            time.sleep(0.1)
            for index in range(50):
                send_msg(GET_IMAGE(), json_socket, lock)
                get_image(data_socket)
                time.sleep(0.1)
        elif s == 'ACQ_STATS':
            send_msg(GET_ACQ_STATS(), json_socket, lock)
            print(f"Frame index: {get_image(data_socket)}")
        else:
            print('ERR: invalid command \'{}\''.format(s))
            
    
    
#async def recv_data(socket):
def recv_data(json_socket, data_socket, queue, lock):
    '''
    Receive data over the data socket, test that the length is correct (hint: it's not)
    and print (what should be) the message header
    '''
    num_frames = 0
    acq_active = False
    last_index = -1
    start_time = 0
    while True:
        #time.sleep(1)
        if not queue.empty():
            msg = queue.get_nowait()
            print(f"Data recv thread got msg: {msg}")
            if "EXIT" in msg:
                return
            elif "ACQ_ACTIVE" in msg:
                if acq_active:
                    acq_time = time.time() - start_time
                    print("="*80)
                    print("   Data acquisition stats:")
                    print(f"   num_frames={num_frames}\n   time={acq_time}\n   =>{num_frames/acq_time} frames/s")
                    print("="*80)
                    print("\n")
                else:
                    print("Start acquisition...")
                    start_time = time.time()
                acq_active = not acq_active
                num_frames = 0
            
        if acq_active:
            send_msg(GET_IMAGE(), json_socket, lock, debug=False)
            try:
                index = get_image(data_socket)
            except Exception as err:
                print(f"Caught exception {err}. Last good index = {last_index}")
                raise
            if index != -1 and index != last_index:
                last_index = index
                num_frames = num_frames + 1
                
            
            #await asyncio.sleep(0)
            
    print(f"Finished listening for data. Num frames received = {num_frames}")
         

def live_data(json_socket, data_socket, queue, lock):
    '''
    Receive data over the data socket, test that the length is correct (hint: it's not)
    and print (what should be) the message header
    '''
    num_frames = 0
    acq_active = False
    last_index = -1
    start_time = 0
    while True:
        #time.sleep(1)
        if not queue.empty():
            msg = queue.get_nowait()
            if "EXIT" in msg:
                return
            elif "ACQ_ACTIVE" in msg:
                if acq_active:
                    acq_time = time.time() - start_time
                    print("="*80)
                    print("   LIVE acquisition stats:")
                    print(f"   num_frames={num_frames}\n   time={acq_time}\n   =>{num_frames/acq_time} frames/s")
                    print("="*80)
                    print("\n")
                else:
                    #print("Start acquisition...")
                    start_time = time.time()
                acq_active = not acq_active
                num_frames = 0
            
        if acq_active:
            try:
                if get_live_img(data_socket):
                    num_frames += 1
                
                
            except Exception as err:
                print(f"Caught exception {err}. Last good index = {last_index}")
                raise
                
            
            #await asyncio.sleep(0)
            
    print(f"Finished listening for data. Num frames received = {num_frames}")
      

def main():
    json_socket, data_socket, live_socket = connect_to_server()
    
    lock = mp.Lock()
    
    data_thr_q = mp.Queue()
    data_recv_thr = mp.Process(target=recv_data, args=(json_socket, data_socket, data_thr_q, lock))
    data_recv_thr.start()
    
    live_thr_q = mp.Queue()
    live_data_thr = mp.Process(target=live_data, args=(json_socket, live_socket, live_thr_q, lock))
    live_data_thr.start()
    
    input_task(json_socket, data_socket, live_socket, data_thr_q, live_thr_q, lock)
    
    
    data_recv_thr.join()
    live_data_thr.join()
#    request_data_thr.join()                
 
#async def main():
#    json_socket, data_socket, live_socket = connect_to_server()
#    await asyncio.gather(input_task(json_socket),
#                         recv_data(data_socket),
#                         request_data(json_socket)
#    
       
    
if __name__ == "__main__":
	main()
