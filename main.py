from bytewax import Dataflow, inputs, spawn_cluster, AdvanceTo, Emit
from websocket import create_connection
import json


def ws_input():
    ws = create_connection("wss://ftx.com/ws")
    ws.send(
        json.dumps(
        {
                "market": "ETH/USD",
                "op": "subscribe",
                "channel": "trades"
            }
        )
    )
    epoch = 0
    while True:
        yield Emit(ws.recv())
        epoch += 1
        yield AdvanceTo(epoch)

def input_builder(worker_index, worker_count):
    # prods_per_worker = int(len(PRODUCT_IDS)/worker_count)
    # product_ids = PRODUCT_IDS[int(worker_index*prods_per_worker):int(worker_index*prods_per_worker+prods_per_worker)]
    return ws_input()


def output_builder(worker_index, worker_count):
    return print

flow = Dataflow()
flow.capture()


if __name__ == "__main__":
    spawn_cluster(flow, input_builder, output_builder)