il y a des bonnes explications et demo des streams et ksqldb
https://ksqldb.io/slides/kafka-summit-europe-2021/slides.html


Project package structure description : 

    config:
        KafkaProducerConfig : producer configuration
        SwaggerConfig : Swagger configuration
        TopicConfig : topic configuration
    
    consumer:
    
    controller:
        KafkaAdminController : endpoints for KafkaAdminClient
        ***KafkaPOCController : MAIN REST CONTROLLER
    
    manager:
    
    model:
        Order : POJO
        Return: POJO
    
    producer:
        OrderProducer : KafkaTemplate produce Order to topics : order.topic.name AND order.window.topic.name
    

    KafkaEventAlarmApplication : Main 


    Docker Compose for Kafka :  src/test/resources/docker-compose.yml

    

To launch Kafka : go into "src/test/resources/docker-compose.yml"

docker-compose up -V --remove-orphans


Access Swagger-UI

TO CREATE ORDER : 

    http://localhost:8282/swagger-ui.html#/kafka-poc-controller/sendMessageToKafkaTopicUsingPOST

or 

    curl -X POST "http://localhost:8282/kafka/createOrder" -H "accept: */*" -H "Content-Type: application/json" -d "{ \"orderId\": \"string\", \"orderTimestamp\": \"string\", \"product\": \"XYZ\", \"status\": \"NEW\"}"



http://localhost:9021/clusters/mU5S8MjlQDS2VrqB12E-VA/management/topics
https://www.andreinc.net/2021/03/07/cars-and-police-a-spring-boot-application-streaming-using-kafka-and-ksqldb


Transforming a stream
-- pq1
CREATE STREAM clean AS
SELECT sensor,
reading,
UCASE(location) AS location
FROM readings
EMIT CHANGES;

Filtering rows out of a stream
-- pq1
CREATE STREAM clean AS
    SELECT
        sensor,
        reading,
        UCASE(location) AS location
    FROM readings
    EMIT CHANGES;

-- pq2
CREATE STREAM high_readings AS
    SELECT sensor, reading, location
    FROM clean
    WHERE reading > 41
    EMIT CHANGES;

Combining many operations into one
-- pq1
CREATE STREAM high_pri AS
    SELECT sensor,
        reading,
        UCASE(location) AS location
    FROM readings
    WHERE reading > 41
    EMIT CHANGES;


Processing with multiple consumers
-- pq1
CREATE STREAM high_pri AS
    SELECT sensor,
        reading,
        UCASE(location) AS location
    FROM readings
    WHERE reading > 41
    EMIT CHANGES;

-- pq2
CREATE STREAM by_location AS
    SELECT *
    FROM high_pri
    PARTITION BY location
    EMIT CHANGES;

-- pq3
CREATE STREAM s1_by_location AS
    SELECT sensor,
        reading,
        UCASE(location) AS location
    FROM s2
    EMIT CHANGES;


Materializing a view from a stream
-- pq1
CREATE TABLE avg_readings AS
    SELECT sensor,
        AVG(reading) AS avg
    FROM readings
    GROUP BY sensor
    EMIT CHANGES;


https://github.com/confluentinc/demo-scene/blob/3e30f222897154f8305843b93b7de9a2a109ca9d/introduction-to-ksqldb/demo_introduction_to_ksqldb_02.adoc