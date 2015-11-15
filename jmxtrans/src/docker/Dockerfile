FROM java

EXPOSE 2101

VOLUME ["/var/lib/jmxtrans"]

COPY jmxtrans-*-all.jar /jmxtrans-all.jar
COPY logback.xml /logback.xml
COPY jmxtrans.json /var/lib/jmxtrans/

CMD [ \
    "java", \
    "-Dcom.sun.management.jmxremote", \
    "-Dcom.sun.management.jmxremote.ssl=false", \
    "-Dcom.sun.management.jmxremote.authenticate=false", \
    "-Dcom.sun.management.jmxremote.port=2101", \
    "-Dlogback.configurationFile=/logback.xml", \
    "-jar", "/jmxtrans-all.jar", \
    "-j", "/var/lib/jmxtrans", \
    "-s", "10" \
]
