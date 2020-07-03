import zmq
import msgpack

# PORT = int(input("Enter PORT:"))
PORT = 6000
addr = "tcp://127.0.0.1:" + str(PORT)
socket = zmq.Context().socket(zmq.DEALER)
zmq.IDENTITY = 1
socket.connect(addr)
socket.connect(addr)

while True:
	cmd_str = input("Command:")
	cmd_bin = msgpack.packb(cmd_str)
	socket.send_multipart([cmd_bin])

socket.disconnect()
socket.close()