if [[ $HOME != $PWD ]]
then 
	echo "ERROR: To run the setup_storm_zoo.sh file it must be in the $HOME folder" 1>&2
	exit 64
fi


printf "Enter Zoo Keeper Namenode (example: nashville)\n"
read first_namenode

printf "Enter Nimbus Namenode: (example: madison)\n"
read second_namenode

printf "Enter Workers (example: jackson lansing albany)\n"
read -a workers

printf "Enter Port Range (example: 31601 31650)\n"
read -a port_range

#first_namenode=pierre
#second_namenode=olympia
#workers=("montgomery" "montpelier" "oklahoma-city" "phoenix" "providence" "raleigh" "madison" "nashville")
#port_range=(31476 31500)

machines=("${workers[@]}") 
machines+=($first_namenode)
machines+=($second_namenode)

for j in "${machines[@]}"
do
      ssh $j "pkill supervisord"
      ssh $j "mkdir -p /s/$j/a/tmp/storm_$USER"
      ssh $j "mkdir -p /tmp/storm_$USER"
done

first_port=${port_range[0]}
echo ""
echo ""
echo ""
echo "Zoo Keeper Namnode: $first_namenode"
echo "Nimbus Namenode: $second_namenode"
echo "Workers: ${workers[*]}"
echo "Port Range: ${port_range[0]}-${port_range[1]}"

ssh $first_namenode "rm -rf /tmp/zookeeper"
ssh $first_namenode "rm -rf /s/$first_namenode/tmp/zookeeper/a/tmp/zookeeper_$USER"
mkdir -p ~/stormConf
mkdir -p ~/zookeeperConf
mkdir -p ~/stormExamples
cp -r /usr/local/storm/latest/conf/* ~/stormConf/
cp -r /usr/local/zookeeper/latest/conf/* ~/zookeeperConf/
cp -r -u /usr/local/storm/latest/examples/* ~/stormExamples/

cp ~/zookeeperConf/zoo_sample.cfg ~/zookeeperConf/zoo.cfg

sed -i "12s|dataDir=/tmp/zookeeper|dataDir=/s/$HOSTNAME/a/tmp/zookeeper_$USER|" ~/zookeeperConf/zoo.cfg
sed -i "14s|clientPort=2181|clientPort=$first_port|" ~/zookeeperConf/zoo.cfg

sed -i "18s|# storm.zookeeper.servers:|storm.zookeeper.servers:|" ~/stormConf/storm.yaml
sed -i "19s|#     - \"server1\"|     - \"$first_namenode.cs.colostate.edu\"|" ~/stormConf/storm.yaml

sed -i "20s|#     - \"server2\"|storm.zookeeper.port: $first_port|" ~/stormConf/storm.yaml

sed -i "22s|# nimbus.seeds: \[\"host1\", \"host2\", \"host3\"\]|nimbus.seeds: [\"$second_namenode.cs.colostate.edu\"]|" ~/stormConf/storm.yaml

sed -i "25s|# ##### These may optionally be filled in:|storm.log.dir: \"/tmp/storm_$USER\"|" ~/stormConf/storm.yaml
sed -i "26s|#|storm.local.dir: \"/tmp/storm_$USER\"|" ~/stormConf/storm.yaml

sed -i "27s|## List of custom serializations|supervisor.slots.ports:|" ~/stormConf/storm.yaml
sed -i "28s|# topology.kryo.register:|    - $(($first_port+1))|" ~/stormConf/storm.yaml
sed -i "29s|#     - org.mycompany.MyType|    - $(($first_port+2))|" ~/stormConf/storm.yaml
sed -i "30s|#     - org.mycompany.MyType2: org.mycompany.MyType2Serializer|    - $(($first_port+3))|" ~/stormConf/storm.yaml
sed -i "31s|#|    - $(($first_port+4))|" ~/stormConf/storm.yaml
sed -i "32s|## List of custom kryo decorators|ui.port: $(($first_port+5))|" ~/stormConf/storm.yaml
sed -i "36s|## Locations of the drpc servers|java.library.path: \"/usr/lib/jvm/java-1.8.0-openjdk\"|" ~/stormConf/storm.yaml
cp /etc/supervisord.conf ~/stormConf/zk-supervisord.conf 
sed -i "37s|# drpc.servers:|java.home: \"/usr/lib/jvm/java-1.8.0-openjdk/jre\"|" ~/stormConf/storm.yaml
sed -i "41s|## Metrics Consumers|supervisor.thrift.port: $(($first_port+6))|" ~/stormConf/storm.yaml
sed -i "42s|## max.retain.metric.tuples|pacemaker.port: $(($first_port+7))|" ~/stormConf/storm.yaml
sed -i "43s|## - task queue will be unbounded when max.retain.metric.tuples is equal or less than 0.|storm.exhibitor.port: $(($first_port+8))|" ~/stormConf/storm.yaml
sed -i "44s|## whitelist / blacklist|nimbus.thrift.port: $(($first_port+9))|" ~/stormConf/storm.yaml
sed -i "46s|## - you need to specify either whitelist or blacklist, or none of them. You can't specify both of them.|logviewer.port: $(($first_port+10))|" ~/stormConf/storm.yaml

sed -i "4s|file=/run/supervisor/supervisor.sock|file=/s/%(ENV_HOSTNAME)s/a/tmp/storm_%(ENV_USER)s/supervisor.sock|" ~/stormConf/zk-supervisord.conf
sed -i "16s|logfile=/var/log/supervisor/supervisord.log|logfile=/s/%(ENV_HOSTNAME)s/a/tmp/storm_%(ENV_USER)s/supervisord.log|" ~/stormConf/zk-supervisord.conf
sed -i "22s|minfds=1024|;minfds=1024|" ~/stormConf/zk-supervisord.conf
sed -i "23s|minprocs=200|;minprocs=200|" ~/stormConf/zk-supervisord.conf

sed -i "40s|serverurl=unix:///run/supervisor/supervisor.sock|serverurl=unix:///s/%(ENV_HOSTNAME)s/a/tmp/storm_%(ENV_USER)s/supervisor.sock|" ~/stormConf/zk-supervisord.conf
sed -i "128s|[include]|;[include]|" ~/stormConf/zk-supervisord.conf
sed -i "129s|files = supervisord.d/*.ini|;files = supervisord.d/*.ini|" ~/stormConf/zk-supervisord.conf

cp ~/stormConf/zk-supervisord.conf ~/stormConf/nimbus-supervisord.conf
cp ~/stormConf/zk-supervisord.conf ~/stormConf/worker-supervisord.conf


sed -i "51s|theprogramname|zookeeper|" ~/stormConf/zk-supervisord.conf
sed -i "51s|;||" ~/stormConf/zk-supervisord.conf
sed -i "52s|;command=/bin/cat|command=zkServer.sh start-foreground|" ~/stormConf/zk-supervisord.conf

sed -i "51s|theprogramname|storm_ui|" ~/stormConf/nimbus-supervisord.conf
sed -i "51s|;||" ~/stormConf/nimbus-supervisord.conf
sed -i "52s|;command=/bin/cat|command=storm ui|" ~/stormConf/nimbus-supervisord.conf
sed -i "54s|;numprocs=1|[program:storm_nimbus]|" ~/stormConf/nimbus-supervisord.conf
sed -i "55s|;directory=/tmp|command=storm nimbus|" ~/stormConf/nimbus-supervisord.conf

sed -i "51s|theprogramname|storm_supervisor|" ~/stormConf/worker-supervisord.conf
sed -i "51s|;||" ~/stormConf/worker-supervisord.conf
sed -i "52s|;command=/bin/cat|command=storm supervisor|" ~/stormConf/worker-supervisord.conf

echo ""
echo 'Storm and Zoo Keeper Configurations are now complete. Please check and make sure that the courses/cs535/pa2 module is loaded with the "module list" command.'
echo ""
echo "Start Zoo Keeper by sshing into your Zoo Keeper Node ($first_namenode) and running the command \"supervisord -c ~/stormConf/zk-supervisord.conf\""
echo ""
echo "Start Nimbus by sshing into your Nimbus Node ($second_namenode) and running the command \"supervisord -c ~/stormConf/nimbus-supervisord.conf\""
echo ""
echo "Start your Woker nodes by sshing into each of them (${workers[*]}) and running the command \"supervisord -c ~/stormConf/worker-supervisord.conf\""
echo ""
echo "The Storm UI is visible from http://$second_namenode:$(($first_port+5))"
echo ""
echo "To stop your Zoo Keeper, Nimbus, and Worker nodes ssh into each of them and run the command \"pkill supervisord\". "
echo ""
echo "There are example storm projects in the stormExamples folder in your home directory that you can run, for example"
echo ""
echo "$ cd ~/stormExamples/storm-starter/"
echo "$ mvn package"
echo "$ storm jar target/storm-starter-*.jar org.apache.storm.starter.RollingTopWords production-topology"
echo ""
