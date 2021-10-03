import ray
import os
import sys
import socket

@ray.remote
def add(a, b):
    print("add")
    return a + b

if __name__ == '__main__':
    print("Enter sample.py")
    print(f"sys.argv: {sys.argv}")
    ip = socket.gethostbyname(socket.gethostname())
    print(f"IP: {ip}")
    print(f"CWD: {os.getcwd()}")

    print(ray.get(add.remote(1, 2)))
