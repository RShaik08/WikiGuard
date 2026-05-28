import json, time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"))

print("WikiGuard Producer starting...")
i = 0
while True:
    comments = ["", "test edit", "fixing typo", "lol vandal", "updated info", ""]
    titles = ["Python","JavaScript","Donald Trump","India","ChatGPT",
              "Elon Musk","World War II","Cricket","Football","NASA"]
    rec = {
        "timestamp": int(time.time()),
        "title": titles[i % len(titles)],
        "user": "user_" + str(i % 20),
        "bot": 0, "wiki": "enwiki", "type": "edit", "namespace": 0,
        "old_length": 5000,
        "new_length": 4800,
        "comment": comments[i % len(comments)],
        "minor": int(i % 3 == 0), "patrolled": 0,
        "length_delta": -200 - (i % 5) * 300,
    }
    producer.send("wiki-edits", value=rec)
    print("Sent: " + rec["title"] + " delta=" + str(rec["length_delta"]))
    i += 1
    time.sleep(1)