FROM confluentinc/cp-kafka-connect:7.2.2

RUN confluent-hub install --no-prompt C0urante/kafka-connect-reddit:0.1.3

ADD connect-standalone.properties config/connect-standalone.properties

ADD reddit-connector.properties config/reddit-connector.properties
