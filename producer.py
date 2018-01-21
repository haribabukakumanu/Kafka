from kafka import KafkaProducer,KafkaClient
import json
import avro.io
import io

def senddata():
    producer = KafkaProducer(bootstrap_servers="localhost:9092")
    schema = avro.schema.parse(open("schema/flood.json").read())
    data = json.load(open("data/sample.json", "r"))
    for item in data:
        a_item = {
            "construction": str(item.get("construction")),
            "county": str(item.get("county")),
            "eq_site_deductible": item.get("eq_site_deductible"),
            "eq_site_limit": item.get("eq_site_limit"),
            "fl_site_deductible": item.get("fl_site_deductible"),
            "fl_site_limit": item.get("fl_site_limit"),
            "fr_site_deductible": item.get("fr_site_deductible"),
            "fr_site_limit": item.get("fr_site_limit"),
            "hu_site_deductible": item.get("hu_site_deductible"),
            "hu_site_limit": item.get("hu_site_limit"),
            "line": str(item.get("line")),
            "point_granularity": item.get("point_granularity"),
            "point_latitude": item.get("point_latitude"),
            "point_longitude": item.get("point_longitude"),
            "policyID": item.get("policyID"),
            "statecode": str(item.get("statecode")),
            "tiv_2011": item.get("tiv_2011"),
            "tiv_2012": item.get("tiv_2012")
        }

        # defines encoding format
        writer = avro.io.DatumWriter(schema)
        bytes_writes = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writes)
        writer.write(a_item, encoder)
        raw_bytes = bytes_writes.getvalue()

        producer.send("floodinfo",raw_bytes)
    producer.flush()

if __name__ == '__main__':
    senddata()