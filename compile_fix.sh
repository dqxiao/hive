mvn clean package -Pdist -DskipTests

# copy connector to lib 
cp /usr/local/Cellar/hive/1.2.1/libexec/lib/mysql-connector-java-5.1.38-bin.jar $HIVE_HOME/lib/
# copy right configuration to conf 
cp /usr/local/Cellar/hive/1.2.1/libexec/conf/hive-site.xml $HIVE_HOME/conf/