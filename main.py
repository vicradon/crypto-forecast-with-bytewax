from bytewax import Dataflow, inputs, parse, run_cluster, AdvanceTo, Emit, spawn_cluster
from websocket import create_connection
import json

def input_builder(worker_index, worker_count):
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


def output_builder(worker_index, worker_count):
    return print

flow = Dataflow()
flow.capture()


if __name__ == "__main__":
    spawn_cluster(flow, input_builder, output_builder)