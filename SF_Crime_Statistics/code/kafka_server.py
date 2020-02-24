import producer_server

CRIME_REPORT_TOPIC = "org.pdc.crimeevents"
BOOTSTRAP_SERVER = "localhost:9092"
def run_kafka_server():
	# :: get the json file path
    input_file = "police-department-calls-for-service.json"

    # :: configure producer
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic= CRIME_REPORT_TOPIC,
        bootstrap_servers= BOOTSTRAP_SERVER,
        client_id="crimeevents.producer"
    )

    return producer


def feed():
    print("------------------Feed---------------------")
    producer = run_kafka_server()
    
    print("------------------Generate_data---------------------")
    producer.generate_data()


if __name__ == "__main__":
    feed()
