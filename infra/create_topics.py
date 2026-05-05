from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


BOOTSTRAP_SERVERS = "kafka:9092"

TOPICS = [
    {"name": "mmcows.cbt", "num_partitions": 1, "replication_factor": 1},
    {"name": "mmcows.ankle", "num_partitions": 1, "replication_factor": 1},
    {"name": "mmcows.immu", "num_partitions": 1, "replication_factor": 1},
    {"name": "mmcows.milk", "num_partitions": 1, "replication_factor": 1},
    {"name": "mmcows.thi", "num_partitions": 1, "replication_factor": 1},
    {"name": "mmcows.pressure", "num_partitions": 1, "replication_factor": 1},
    {"name": "mmcows.uwb", "num_partitions": 1, "replication_factor": 1},
]


def main():
    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="mmcows-admin"
    )

    new_topics = [
        NewTopic(
            name=t["name"],
            num_partitions=t["num_partitions"],
            replication_factor=t["replication_factor"],
        )
        for t in TOPICS
    ]

    try:
        admin.create_topics(new_topics=new_topics, validate_only=False)
        for t in TOPICS:
            print(f"Topic created: {t['name']}")
    except TopicAlreadyExistsError:
        print("Some topics already exist")
    finally:
        admin.close()


if __name__ == "__main__":
    main()