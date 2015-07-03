Sample Apache Drill application. Also used to test Maven artifacts for Drill.

1) Change the URL in pom.xml
2) Change the CONNECT_URL in DrillQuerySubmitter
3) Build: mvn clean install
4) Run: java -Dlogback.configurationFile=logback.xml -jar target/drill-example-1.0.jar "select * from sys.drillbits"
