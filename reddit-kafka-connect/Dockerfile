FROM confluentinc/cp-kafka-connect:7.2.2

ADD reddit-connector.properties config/reddit-connector.properties

ADD connect-standalone.properties config/connect-standalone.properties

RUN confluent-hub install --no-prompt C0urante/kafka-connect-reddit:0.1.3
