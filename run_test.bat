@echo off
REM Start the MiniKafka server
start "MiniKafka Server" cmd /k "java -jar target/minikafka-1.0-SNAPSHOT-jar-with-dependencies.jar 9982 "./data" 4"

REM Wait for the server to start
timeout /t 3 /nobreak > nul

REM Start the message producer
start "Message Producer" cmd /k "java -cp target/minikafka-1.0-SNAPSHOT-jar-with-dependencies.jar MessageProducer localhost 9982 test-topic"

REM Wait for the producer to start
timeout /t 2 /nobreak > nul

REM Start the message consumer
start "Message Consumer" cmd /k "java -cp target/minikafka-1.0-SNAPSHOT-jar-with-dependencies.jar MiniKafkaClient localhost 9982"

echo All processes started. Press any key to stop all processes.
pause > nul

REM Stop all processes
taskkill /FI "WINDOWTITLE eq MiniKafka Server*" /F
taskkill /FI "WINDOWTITLE eq Message Producer*" /F
taskkill /FI "WINDOWTITLE eq Message Consumer*" /F
