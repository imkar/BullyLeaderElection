import sys,os,zmq,time
import random
import threading
# from threading import Lock
from multiprocessing import Process, Array, Value

def responder(nodeId,go_arr,terminate_arr,response_arr,alivelist,numProc):
    print("RESPONDER STARTS",nodeId)

    context = zmq.Context()
    sub_socket = context.socket(zmq.SUB)
    sub_socket.subscribe("LEADER")
    sub_socket.subscribe("TERMINATE")
    poller = zmq.Poller()
    poller.register(sub_socket, zmq.POLLIN)

    ##====================================================##
    ##      Connects all ports of the Processes.          ##
    ##====================================================##
    breaker=0
    while True:
        
        for i in alivelist:
            PORT = 5550 + i
            sub_socket.connect("tcp://127.0.0.1:" + str(PORT))
            events = dict(poller.poll(timeout=100))
            # print(evts)
            if sub_socket in events:
                topic = sub_socket.recv_string()
                message = sub_socket.recv_json()
                for key, value in message.items():
                    if key == "TERMINATE":
                        with threading.Lock():
                            terminate_arr[nodeId] += 1
                        breaker=1
                    if key == "LEADER":
                        with threading.Lock():
                            go_arr[nodeId] += 1
                    ##====================================================##
                    ##      Responds to Node with lower Node ID's         ##
                    ##====================================================##
                        if value < nodeId:
                            print("RESPONDER RESPONDS",nodeId,value)

                            pub_socket = context.socket(zmq.PUB)
                            time.sleep(1)
                            PORT_ACK = 5550 + value + int(numProc)
                            pub_socket.connect("tcp://127.0.0.1:" + str(PORT_ACK))
                            mes = {"RESP":nodeId}
                            time.sleep(1)
                            pub_socket.send_string("RESP", flags=zmq.SNDMORE)
                            pub_socket.send_json(mes)
                            
                            with threading.Lock():
                                response_arr[int(nodeId)] += 1
        if breaker==1:
            break

def leader(nodeId,starterOrNot,alivelist,numProc):
    print("PROCESS STARTS",os.getpid(),nodeId,starterOrNot)

    go_arr = Array('i',[0]*int(numProc))
    terminate_arr = Array('i',[0]*int(numProc))
    response_arr = Array('i',[0]*int(numProc))
    #Creating threads
    listener_thread = threading.Thread(target=responder,args=(nodeId,go_arr,terminate_arr,response_arr,alivelist,numProc))
    #Starting threads
    listener_thread.start()

    #Creating Publish Socket and Port.
    context = zmq.Context()
    pub_socket = context.socket(zmq.PUB)
    PORT = 5550 + nodeId

    #If it is a starter Node.
    if starterOrNot == True:
        ##======================================================##
        ##  Starts sending "LEADER" message to all nodes.       ##
        ##======================================================##
        print("PROCESS MULTICASTS LEADER MSG:",nodeId)

        pub_socket.bind("tcp://127.0.0.1:" + str(PORT))
        mes = {"LEADER":nodeId}
        time.sleep(1)
    
        pub_socket.send_string("LEADER", flags=zmq.SNDMORE)
        pub_socket.send_json(mes)
        ##======================================================##
        ## Starts to listen messages from 'RESP' queue.         ##
        ##======================================================##
        PORT_ACK = 5550 + nodeId + int(numProc)
        sub_socket = context.socket(zmq.SUB)
        sub_socket.bind("tcp://127.0.0.1:" + str(PORT_ACK))
        sub_socket.subscribe("RESP")
        poller = zmq.Poller()
        poller.register(sub_socket, zmq.POLLIN)

        responded = 0
        with threading.Lock():
            stop = terminate_arr[nodeId]
        while True and not stop:
            with threading.Lock():
                stop = terminate_arr[nodeId]
            events = dict(poller.poll(timeout=100))
            if sub_socket in events:
                topic = sub_socket.recv_string()
                message = sub_socket.recv_json()
            with threading.Lock():
                responded = response_arr[nodeId]
            
            if responded == len(alivelist)-1:
                mes = {"TERMINATE":nodeId}
                time.sleep(1)
                print("PROCESS BROADCASTS TERMINATE MSG:",nodeId)
                pub_socket.send_string("TERMINATE", flags=zmq.SNDMORE)
                pub_socket.send_json(mes)
                with threading.Lock():
                    terminate_arr[nodeId] = 1
                stop = 1
    else:
        ##======================================================##
        ## Waits until "LEADER" message comes from other node's ##
        ##======================================================##
        with threading.Lock():
            stop = terminate_arr[nodeId]
        while True and not stop:
            with threading.Lock():
                go = go_arr[nodeId]
                stop = terminate_arr[nodeId]
            if go > 0:
                break

        ##======================================================##
        ##  Starts sending "LEADER" message to all nodes with   ##
        ##                  higher Node ID's.                   ##
        ##======================================================##
        print("PROCESS MULTICASTS LEADER MSG:",nodeId)

        pub_socket.bind("tcp://127.0.0.1:" + str(PORT))
        mes = {"LEADER":nodeId}
        time.sleep(1)
    
        pub_socket.send_string("LEADER", flags=zmq.SNDMORE)
        pub_socket.send_json(mes)

        ##======================================================##
        ## Starts to listen messages from 'RESP' queue.         ##
        ##======================================================##
        PORT_ACK = 5550 + nodeId + int(numProc)
        sub_socket = context.socket(zmq.SUB)
        sub_socket.bind("tcp://127.0.0.1:" + str(PORT_ACK))
        sub_socket.subscribe("RESP")
        poller = zmq.Poller()
        poller.register(sub_socket, zmq.POLLIN)

        responded = 0
        with threading.Lock():
            stop = terminate_arr[nodeId]
        while True and not stop:
            with threading.Lock():
                stop = terminate_arr[nodeId]   
            events = dict(poller.poll(timeout=100))
            if sub_socket in events:
                topic = sub_socket.recv_string()
                message = sub_socket.recv_json()
            with threading.Lock():
                responded = response_arr[nodeId] 
            if responded == len(alivelist)-1:
                mes = {"TERMINATE":nodeId}
                time.sleep(1)
                print("PROCESS BROADCASTS TERMINATE MSG:",nodeId)
                pub_socket.send_string("TERMINATE", flags=zmq.SNDMORE)
                pub_socket.send_json(mes)
                with threading.Lock():
                    terminate_arr[nodeId] = 1
                    stop = 1
    # Joins the listener Thread.
    listener_thread.join()

if __name__ == "__main__":
    #Total number of nodes
    numProc = sys.argv[1]
    #Number of nodes that are alive(online)
    numAlive = sys.argv[2]
    #Number of nodes that initiate the protocol
    numStarters = sys.argv[3]

    #Mainlist of all Nodes.
    mainlist = [*range(int(numProc))]
    #Random.sample from main list with the amount of numAlive.
    Aliveidlist = random.sample(mainlist,int(numAlive))
    print("Alives:\n",Aliveidlist)
    #Random.sample from list of Alive Nodes with the amount of numStarters.
    StarterId_list = random.sample(Aliveidlist,int(numStarters))
    print("Starters:\n",StarterId_list)
    #Start new process for each alive nodes.
    processes = []
    for i in range(len(Aliveidlist)):
        #If node Id is also in the starter list, inputs to "leader" method with boolean value of "True".
        if Aliveidlist[i] in StarterId_list:
            p = Process(target=leader, args=(Aliveidlist[i],True,Aliveidlist,numProc,))
        #Else, inputs as "False".
        else:
            p = Process(target=leader, args=(Aliveidlist[i],False,Aliveidlist,numProc,))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()