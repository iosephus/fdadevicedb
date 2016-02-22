#!/bin/sh

HOST=localhost
DATABASE=test
DBUSER=nobody
DBPASSWORD=secret
JDBCPATH=/usr/share/java/postgresql-jdbc.jar
ASSEMBLY=FDADevices-SNAPSHOT-assembly-0.1.jar

spark-submit --driver-class-path ${JDBCPATH} ${ASSEMBLY}  --driver-memory 8g --database "jdbc:postgresql://${HOST}/${DATABASE}?user=${DBUSER}&password=${DBPASSWORD}" -u ${DBUSER} -p ${DBPASSSWORD}
