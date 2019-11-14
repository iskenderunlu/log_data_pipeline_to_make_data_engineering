FROM java:8
WORKDIR /
ADD target/producer-1.0-SNAPSHOT.jar log_generator.jar
EXPOSE 8080
CMD java -jar log_generator.jar