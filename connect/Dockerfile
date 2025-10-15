FROM confluentinc/cp-kafka-connect:7.6.1

# Install JDBC connector
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.6

# Add Postgres JDBC driver
RUN curl -fsSL -o /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/postgresql-42.7.4.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.4.jar
