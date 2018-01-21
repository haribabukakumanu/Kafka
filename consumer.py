from kafka import KafkaConsumer
import avro.io
import json
import io

def readData():
    consumer = KafkaConsumer(bootstrap_servers = "localhost:9092", auto_offset_reset="latest")
    consumer.subscribe(["floodinfo"])
    schema = avro.schema.parse(open("schema/flood.json").read())

    for msg in consumer:
        bytes_reader = io.BytesIO(msg.value)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(schema)
        item = reader.read(decoder)
        print item

if __name__ == '__main__':
    readData()