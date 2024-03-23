from collections import OrderedDict
import threading
import grpc
import traceback
from concurrent import futures
import mathdb_pb2_grpc
import mathdb_pb2

class MathCache:
    def __init__(self):
        self.dict = {}
        self.cache = OrderedDict()
        self.capacity = 10
        self.lock = threading.Lock()
    def Set(self, key, value):
        with self.lock:
            self.dict[key] = value
            self.cache = OrderedDict()
    def Get(self, key):
        return self.dict[key]
    def Add(self, key_a, key_b):
        with self.lock:
            key = "add" + key_a + key_b
            value = self.Get(key_a) + self.Get(key_b)
        return value, self.runCache(key, value)
    def Sub(self, key_a, key_b):
        with self.lock:
            key = "sub" + key_a + key_b
            value = self.Get(key_a) - self.Get(key_b)
        return value, self.runCache(key, value)
    def Mult(self, key_a, key_b):
        with self.lock :
            key = "mult" + key_a + key_b
            value = self.Get(key_a) * self.Get(key_b)
        return value, self.runCache(key, value)
    def Div(self, key_a, key_b):
        with self.lock:
            key = "div" + key_a + key_b
            value = self.Get(key_a) / self.Get(key_b)
        return value, self.runCache(key, value)

    def runCache(self, key, value):
        hit = False
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                hit = True
            else:
                self.cache[key] = value
                self.cache.move_to_end(key)
                if(len(self.cache) > self.capacity):
                    self.cache.popitem(last = False)
        return hit 

class MathDb(mathdb_pb2_grpc.MathDbServicer):
    def __init__(self):
        self.cache = MathCache()


    def Set(self, request, context):
        try:
            err = ""
            self.cache.Set(request.key, request.value)
        except Exception:
            err = traceback.format_exc()
        return mathdb_pb2.SetResponse(error=err)

    def Get(self, request, context):
        try:
            err = ""
            result = self.cache.Get(request.key)
        except Exception:
            result = None
            err = traceback.format_exc()
        return mathdb_pb2.GetResponse(value = result, error=err)
    def Add(self, request, context):
        try:
            err = ""
            result = self.cache.Add(request.key_a, request.key_b)
        except Exception:
            result = (None, None)
            err = traceback.format_exc()
        return mathdb_pb2.BinaryOpResponse(value = result[0], cache_hit = result[1], error=err)
    def Sub(self, request, context):
        try:
            err = ""
            result = self.cache.Sub(request.key_a, request.key_b)
        except Exception:
            result = (None, None)
            err = traceback.format_exc()
        return mathdb_pb2.BinaryOpResponse(value = result[0], cache_hit = result[1], error=err)
    def Mult(self, request, context):
        try:
            err = ""
            result = self.cache.Mult(request.key_a, request.key_b)
        except Exception:
            result = (None, None)
            err = traceback.format_exc()
        return mathdb_pb2.BinaryOpResponse(value = result[0], cache_hit = result[1], error=err)
    def Div(self, request, context):
        
        try:
            err = ""
            result = self.cache.Div(request.key_a, request.key_b)
        except Exception:
            result = (None, None)
            err = traceback.format_exc()
        return mathdb_pb2.BinaryOpResponse(value = result[0], cache_hit = result[1], error=err)

if __name__ == "__main__":
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=(('grpc.so_reusedport', 0),))
    mathdb_pb2_grpc.add_MathDbServicer_to_server(MathDb(), server)
    server.add_insecure_port("[::]:5440", )
    server.start()
    server.wait_for_termination()


